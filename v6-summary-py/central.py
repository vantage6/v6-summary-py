"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

import json
from typing import Any
import pandas as pd

from vantage6.common import info
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.tools.exceptions import AlgorithmExecutionError, InputError
from vantage6.algorithm.client import AlgorithmClient


@central
@algorithm_client
def summary(
    client: AlgorithmClient,
    columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Send task to each node participating in the task to compute a local summary,
    aggregate them for all nodes, and return the result.

    Parameters
    ----------
    client : AlgorithmClient
        The client object used to communicate with the server.
    columns : list[str] | None
        The columns to include in the summary. If not given, all columns are included.
    numeric_columns : list[str] | None
        Which of the columns are numeric. If not given, it will be inferred from the
        data.
    organizations_to_include : list[int] | None
        The organizations to include in the task. If not given, all organizations
        in the collaboration are included.
    """
    if columns and numeric_columns and not set(numeric_columns).issubset(set(columns)):
        numeric_not_in_columns = set(numeric_columns) - set(columns)
        raise InputError(
            "The 'numeric_columns' should be a subset of 'columns'. The following "
            f"columns are not in 'columns': {numeric_not_in_columns}"
        )

    # get all organizations (ids) within the collaboration so you can send a
    # task to them.
    if not organizations_to_include:
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        method="summary_per_data_station",
        arguments={
            "columns": columns,
            "numeric_columns": numeric_columns,
        },
        organizations=organizations_to_include,
        name="Subtask summary",
        description="Compute summary per data station",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    # aggregate the partial summaries of all nodes
    results = _aggregate_partial_summaries(results)

    # compute the variance now that we have the mean
    numerical_columns = list(results["numeric"].keys())
    means = [float(results["numeric"][column]["mean"]) for column in numerical_columns]
    if numerical_columns:
        task = client.task.create(
            method="variance_per_data_station",
            arguments={
                "columns": numerical_columns,
                "means": means,
            },
            organizations=organizations_to_include,
            name="Subtask variance",
            description="Compute variance per data station",
        )
        variance_results = client.wait_for_results(task_id=task.get("id"))

        # add the standard deviation to the results
        results = _add_sd_to_results(results, variance_results, numerical_columns)

    # return the final results of the algorithm
    return {
        "numeric": results["numeric"].to_json(),
        "categorical": results["categorical"].to_json(),
        "counts_unique_values": json.dumps(results["counts_unique_values"]),
        "num_complete_rows_per_node": results["num_complete_rows_per_node"].to_json(),
    }


def _aggregate_partial_summaries(results: list[dict]) -> dict:
    """Aggregate the partial summaries of all nodes.

    Parameters
    ----------
    results : list[dict]
        The partial summaries of all nodes.
    """
    info("Aggregating partial summaries")
    aggregated_summary = {}
    is_first = True
    for result in results:
        if result is None:
            raise AlgorithmExecutionError(
                "At least one of the nodes returned invalid result. Please check the "
                "logs."
            )
        if is_first:
            # copy results. Only convert num complete rows per node to a list so that
            # we can add the other nodes to it later
            aggregated_summary["numeric"] = pd.DataFrame(result["numeric"])
            aggregated_summary["categorical"] = pd.DataFrame(result["categorical"])
            aggregated_summary["num_complete_rows_per_node"] = [
                result["num_complete_rows_per_node"]
            ]
            is_first = False
            continue

        # aggregate data for numeric colums
        numeric_result = pd.DataFrame(result["numeric"])
        if not numeric_result.empty:
            aggregated_summary["numeric"].loc["count"] += numeric_result.loc["count"]
            aggregated_summary["numeric"].loc["min"] = pd.concat(
                [
                    aggregated_summary["numeric"].loc["min"],
                    numeric_result.loc["min"],
                ],
                axis=1,
            ).min(axis=1)
            aggregated_summary["numeric"].loc["max"] = pd.concat(
                [
                    aggregated_summary["numeric"].loc["max"],
                    numeric_result.loc["max"],
                ],
                axis=1,
            ).max(axis=1)
            aggregated_summary["numeric"].loc["missing"] += numeric_result.loc[
                "missing"
            ]
            aggregated_summary["numeric"].loc["sum"] += numeric_result.loc["sum"]

        # aggregate data for categorical columns
        categorical_result = pd.DataFrame(result["categorical"])
        if not categorical_result.empty:
            aggregated_summary["categorical"].loc["count"] += categorical_result.loc[
                "count"
            ]
            aggregated_summary["categorical"].loc["missing"] += categorical_result.loc[
                "missing"
            ]

        # add the number of complete rows for this node
        aggregated_summary["num_complete_rows_per_node"].append(
            result["num_complete_rows_per_node"]
        )

    aggregated_summary["counts_unique_values"] = _aggregate_counts_unique_values(
        results, list(aggregated_summary["categorical"].columns)
    )

    # now that all data is aggregated, we can compute the mean
    if not aggregated_summary["numeric"].empty:
        aggregated_summary["numeric"].loc["mean"] = (
            aggregated_summary["numeric"].loc["sum"]
            / aggregated_summary["numeric"].loc["count"]
        )

    # convert the list of complete rows per node to a pandas series
    aggregated_summary["num_complete_rows_per_node"] = pd.Series(
        aggregated_summary["num_complete_rows_per_node"]
    )

    return aggregated_summary


def _aggregate_counts_unique_values(
    results: list[dict], categorical_columns: list[str]
) -> dict[str, dict[str, int]]:
    """Sum the counts of unique values of all nodes, per column.

    A column is returned as an empty dict if any node did not share its counts,
    because a value was below the privacy threshold or because the node's policies do
    not allow sharing counts at all. Summing only the other nodes would give a
    misleading total.

    Parameters
    ----------
    results : list[dict]
        The partial summaries of all nodes.
    categorical_columns : list[str]
        The categorical columns in the summary.

    Returns
    -------
    dict[str, dict[str, int]]
        Per column, the total count of each unique value.
    """
    totals = {column: {} for column in categorical_columns}
    hidden_columns = set()
    for result in results:
        node_counts = result["counts_unique_values"]
        if node_counts is None:
            hidden_columns.update(categorical_columns)
            continue
        for column in categorical_columns:
            column_counts = node_counts.get(column)
            if not column_counts:
                hidden_columns.add(column)
                continue
            for value, count in column_counts.items():
                totals[column][value] = totals[column].get(value, 0) + int(count)
    for column in hidden_columns:
        totals[column] = {}
    return totals


def _add_sd_to_results(
    results: dict, variance_results: list[dict], numerical_columns: list[str]
) -> dict:
    """Add the variance to the results.

    Parameters
    ----------
    results : dict
        The results of the summary task.
    variance_results : list[dict]
        The variance results of all nodes.
    numerical_columns : list[str]
        The numerical columns.

    Returns
    -------
    dict
        The results with the variance added.
    """
    if not numerical_columns:
        return results
    variance_df = pd.DataFrame(variance_results)
    variance_sum_all_nodes = variance_df.sum()
    results["numeric"].loc["std"] = (
        variance_sum_all_nodes / (results["numeric"].loc["count"] - 1)
    ) ** 0.5
    return results
