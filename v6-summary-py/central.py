"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""

from typing import Any

from vantage6.algorithm.tools.util import info
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import AlgorithmExecutionError, InputError
from vantage6.algorithm.client import AlgorithmClient


@algorithm_client
def summary(
    client: AlgorithmClient,
    columns: list[str],
    is_numeric: list[bool] | None = None,
    organizations_to_include: list[int] | None = None,
) -> Any:
    """
    Send task to each node participating in the task to compute a local summary,
    aggregate them for all nodes, and return the result.

    Parameters
    ----------
    client : AlgorithmClient
        The client object used to communicate with the server.
    columns : list[str]
        The columns to include in the summary.
    is_numeric : list[bool] | None
        Whether each of the columns is numeric or not. If not given, the algorithm will
        try to infer the type of the columns.
    organizations_to_include : list[int] | None
        The organizations to include in the task. If not given, all organizations
        in the collaboration are included.
    """
    if is_numeric and len(is_numeric) != len(columns):
        raise InputError(
            "Length of is_numeric list does not match the length of columns list"
        )

    # get all organizations (ids) within the collaboration so you can send a
    # task to them.
    if not organizations_to_include:
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    # Define input parameters for a subtask
    info("Defining input parameters")
    input_ = {
        "method": "summary_per_data_station",
        "kwargs": {
            "columns": columns,
            "is_numeric": is_numeric,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
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
    means = [results["numeric"][column]["mean"] for column in numerical_columns]
    task = client.task.create(
        input_={
            "method": "variance_per_data_station",
            "kwargs": {
                "columns": numerical_columns,
                "means": means,
            },
        },
        organizations=organizations_to_include,
        name="Subtask variance",
        description="Compute variance per data station",
    )
    variance_results = client.wait_for_results(task_id=task.get("id"))

    # add the standard deviation to the results
    results = _add_sd_to_results(results, variance_results, numerical_columns)

    # return the final results of the algorithm
    return results


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
            aggregated_summary = result
            aggregated_summary["num_complete_rows_per_node"] = [
                result["num_complete_rows_per_node"]
            ]
            is_first = False
            continue

        # aggregate data for numeric colums
        for column in result["numeric"]:
            aggregated_dict = aggregated_summary["numeric"][column]
            aggregated_dict["count"] += result["numeric"][column]["count"]
            aggregated_dict["min"] = min(
                aggregated_summary["numeric"][column]["min"],
                result["numeric"][column]["min"],
            )
            aggregated_dict["max"] = max(
                aggregated_summary["numeric"][column]["max"],
                result["numeric"][column]["max"],
            )
            aggregated_dict["missing"] += result["numeric"][column]["missing"]
            aggregated_dict["sum"] += result["numeric"][column]["sum"]

        # aggregate data for categorical columns
        for column in result["categorical"]:
            aggregated_dict = aggregated_summary["categorical"][column]
            aggregated_dict["count"] += result["categorical"][column]["count"]
            aggregated_dict["missing"] += result["categorical"][column]["missing"]

        # add the number of complete rows for this node
        aggregated_summary["num_complete_rows_per_node"].append(
            result["num_complete_rows_per_node"]
        )

        # add the unique values
        for column in result["counts_unique_values"]:
            if column not in aggregated_summary["counts_unique_values"]:
                aggregated_summary["counts_unique_values"][column] = {}
            for value, count in result["counts_unique_values"][column].items():
                if value not in aggregated_summary["counts_unique_values"][column]:
                    aggregated_summary["counts_unique_values"][column][value] = 0
                aggregated_summary["counts_unique_values"][column][value] += count

    # now that all data is aggregated, we can compute the mean
    for column in aggregated_summary["numeric"]:
        aggregated_dict = aggregated_summary["numeric"][column]
        aggregated_dict["mean"] = aggregated_dict["sum"] / aggregated_dict["count"]

    return aggregated_summary


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
    for column in numerical_columns:
        sum_variance = 0
        for node_results in variance_results:
            sum_variance += node_results[column]
        variance = sum_variance / (results["numeric"][column]["count"] - 1)
        results["numeric"][column]["std"] = variance**0.5
    return results
