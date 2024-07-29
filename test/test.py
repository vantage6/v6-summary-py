"""
Run this script to test your algorithm locally (without building a Docker
image) using the mock client.

Run as:

    python test.py

Make sure to do so in an environment where `vantage6-algorithm-tools` is
installed. This can be done by running:

    pip install vantage6-algorithm-tools
"""

import os
import pytest
import pandas as pd
import numpy as np

from vantage6.algorithm.tools.mock_client import MockAlgorithmClient
from vantage6.algorithm.tools.exceptions import (
    AlgorithmExecutionError,
    PrivacyThresholdViolation,
    InputError,
)

# Create fake data. Three columns with random numbers, two columns with factors
np.random.seed(123)
columns = ["A", "B", "C", "D", "E", "F"]
size = 1000

data = pd.DataFrame(
    {
        "A": np.random.choice(range(1, 11), size=size, replace=True),
        "B": np.random.choice([1, 2, 3, np.nan], size=size, replace=True),
        "C": np.random.choice(list(range(6, 20)) + [np.nan], size=size, replace=True),
        "D": np.random.choice(["1", "2", "3", "4"], size=size, replace=True),
        "E": np.random.choice(["female", "male", None], size=size, replace=True),
        "F": np.random.choice(["other"], size=size, replace=True),
    }
)

# Split the dataframe into two sets
n_rows = len(data)
size_per_df = int(size / 2)

df1 = data.iloc[:size_per_df, :]
df2 = data.iloc[size_per_df:, :]

## Mock client
client = MockAlgorithmClient(
    datasets=[
        # Data for first organization
        [
            {
                "database": df1,
            }
        ],
        # Data for second organization
        [
            {
                "database": df2,
            }
        ],
    ],
    module="v6-summary-py",
)

# list mock organizations
organizations = client.organization.list()
org_ids = [organization["id"] for organization in organizations]


central_task = client.task.create(
    input_={
        "method": "summary",
        "kwargs": {
            "columns": columns,
        },
    },
    organizations=[org_ids[0]],
)
results = client.wait_for_results(central_task.get("id"))


def test_central_all_columns():
    """test central method on all columns"""
    central_task = client.task.create(
        input_={
            "method": "summary",
            "kwargs": {
                "columns": columns,
            },
        },
        organizations=[org_ids[0]],
    )
    results = client.wait_for_results(central_task.get("id"))

    # check results from central task
    num_complete_rows_per_node = results[0]["num_complete_rows_per_node"]
    assert num_complete_rows_per_node[0] == df1.dropna().shape[0]
    assert num_complete_rows_per_node[1] == df2.dropna().shape[0]
    numeric_results = results[0]["numeric"]
    assert numeric_results["A"]["count"] == data["A"].count()
    assert numeric_results["B"]["count"] == data["B"].count()
    assert numeric_results["C"]["count"] == data["C"].count()
    assert numeric_results["A"]["min"] == data["A"].min()
    assert numeric_results["B"]["min"] == data["B"].min()
    assert numeric_results["C"]["min"] == data["C"].min()
    assert numeric_results["A"]["max"] == data["A"].max()
    assert numeric_results["B"]["max"] == data["B"].max()
    assert numeric_results["C"]["max"] == data["C"].max()
    assert numeric_results["A"]["missing"] == data["A"].isna().sum()
    assert numeric_results["B"]["missing"] == data["B"].isna().sum()
    assert numeric_results["C"]["missing"] == data["C"].isna().sum()
    assert numeric_results["A"]["sum"] == data["A"].sum()
    assert numeric_results["B"]["sum"] == data["B"].sum()
    assert numeric_results["C"]["sum"] == data["C"].sum()
    assert numeric_results["A"]["mean"] == data["A"].mean()
    assert numeric_results["B"]["mean"] == data["B"].mean()
    assert numeric_results["C"]["mean"] == data["C"].mean()
    assert numeric_results["A"]["std"] == data["A"].std()
    assert numeric_results["B"]["std"] == data["B"].std()
    assert numeric_results["C"]["std"] == data["C"].std()
    categorical_results = results[0]["categorical"]
    assert categorical_results["D"]["count"] == data["D"].count()
    assert categorical_results["E"]["count"] == data["E"].count()
    assert categorical_results["F"]["count"] == data["F"].count()
    assert categorical_results["D"]["missing"] == data["D"].isna().sum()
    assert categorical_results["E"]["missing"] == data["E"].isna().sum()
    assert categorical_results["F"]["missing"] == data["F"].isna().sum()
    counts_unique_values = results[0]["counts_unique_values"]
    assert counts_unique_values["D"]["1"] == data["D"].value_counts().get("1", 0)
    assert counts_unique_values["D"]["2"] == data["D"].value_counts().get("2", 0)
    assert counts_unique_values["D"]["3"] == data["D"].value_counts().get("3", 0)
    assert counts_unique_values["D"]["4"] == data["D"].value_counts().get("4", 0)
    assert counts_unique_values["E"]["female"] == data["E"].value_counts().get(
        "female", 0
    )
    assert counts_unique_values["E"]["male"] == data["E"].value_counts().get("male", 0)
    assert counts_unique_values["F"]["other"] == data["F"].value_counts().get(
        "other", 0
    )


def test_central_single_numeric_column():
    """ensure that we can run a task for a single numeric column"""
    task = client.task.create(
        input_={
            "method": "summary",
            "kwargs": {
                "columns": ["A"],
            },
        },
        organizations=[org_ids[0]],
    )
    results = client.wait_for_results(task.get("id"))
    assert results[0]["numeric"]["A"]["count"] == data["A"].count()
    assert results[0]["numeric"]["A"]["min"] == data["A"].min()
    assert results[0]["numeric"]["A"]["max"] == data["A"].max()
    assert results[0]["numeric"]["A"]["missing"] == data["A"].isna().sum()
    assert results[0]["numeric"]["A"]["sum"] == data["A"].sum()
    assert results[0]["categorical"] == {}
    assert results[0]["counts_unique_values"] == {}
    assert results[0]["num_complete_rows_per_node"][0] == df1["A"].dropna().shape[0]
    assert results[0]["num_complete_rows_per_node"][1] == df2["A"].dropna().shape[0]


def test_central_single_categorical_column():
    """ensure that we can run a task for a single categorical column"""
    task = client.task.create(
        input_={
            "method": "summary",
            "kwargs": {
                "columns": ["E"],
            },
        },
        organizations=[org_ids[0]],
    )
    results = client.wait_for_results(task.get("id"))
    assert results[0]["categorical"]["E"]["count"] == data["E"].count()
    assert results[0]["categorical"]["E"]["missing"] == data["E"].isna().sum()
    assert results[0]["numeric"] == {}
    assert results[0]["counts_unique_values"]["E"]["female"] == data[
        "E"
    ].value_counts().get("female", 0)
    assert results[0]["counts_unique_values"]["E"]["male"] == data[
        "E"
    ].value_counts().get("male", 0)
    assert results[0]["num_complete_rows_per_node"][0] == df1["E"].dropna().shape[0]
    assert results[0]["num_complete_rows_per_node"][1] == df2["E"].dropna().shape[0]


def test_central_non_existing_column():
    """check that non-existing columns give an error"""
    with pytest.raises(InputError):
        client.task.create(
            input_={
                "method": "summary",
                "kwargs": {
                    "columns": ["non-existing-column"],
                },
            },
            organizations=[org_ids[0]],
        )


def test_partial_non_existing_column():
    """Test that non-existing columns give an error"""
    with pytest.raises(InputError):
        client.task.create(
            input_={
                "method": "summary_per_data_station",
                "kwargs": {
                    "columns": ["non-existing-column"],
                },
            },
            organizations=[org_ids[0]],
        )


def test_privacy_threshold_categorical():
    """Test that counts of unique values in a categorical column are not returned if the
    privacy threshold is violated
    """
    os.environ["SUMMARY_PRIVACY_THRESHOLD"] = "1000"
    task = client.task.create(
        input_={
            "method": "summary",
            "kwargs": {
                "columns": ["D"],
            },
        },
        organizations=[org_ids[0]],
    )
    results = client.wait_for_results(task.get("id"))
    os.environ["SUMMARY_PRIVACY_THRESHOLD"] = "5"
    assert results[0]["counts_unique_values"]["D"] == {}


def test_convert_categorical_to_numeric():
    """Test that we can convert a categorical column to a numeric column"""
    # note that column D is a categorical column that consists of "1", "2", "3", "4"
    # so it should be convertible to a numeric column
    central_task = client.task.create(
        input_={
            "method": "summary",
            "kwargs": {
                "columns": ["D"],
                "is_numeric": [True],
            },
        },
        organizations=[org_ids[0]],
    )
    results = client.wait_for_results(central_task.get("id"))
    numeric_data_D = data["D"].astype(int)
    assert results[0]["numeric"]["D"]["count"] == numeric_data_D.count()
    assert results[0]["numeric"]["D"]["min"] == numeric_data_D.min()
    assert results[0]["numeric"]["D"]["max"] == numeric_data_D.max()
    assert results[0]["numeric"]["D"]["missing"] == numeric_data_D.isna().sum()
    assert results[0]["numeric"]["D"]["sum"] == numeric_data_D.sum()
    assert results[0]["categorical"] == {}
    assert results[0]["counts_unique_values"] == {}
    assert results[0]["num_complete_rows_per_node"][0] == df1["D"].dropna().shape[0]
    assert results[0]["num_complete_rows_per_node"][1] == df2["D"].dropna().shape[0]


def test_partial_all_columns():
    """Verify results from partial task"""
    task = client.task.create(
        input_={
            "method": "summary_per_data_station",
            "kwargs": {
                "columns": columns,
            },
        },
        organizations=org_ids,
    )

    # Get the results from the task
    results = client.wait_for_results(task.get("id"))

    # verify results from partial tasks
    for node_idx in [0, 1]:
        check_df = data.iloc[size_per_df * node_idx : size_per_df * (node_idx + 1)]

        num_complete_rows_per_node = results[node_idx]["num_complete_rows_per_node"]
        assert num_complete_rows_per_node == check_df.dropna().shape[0]

        numeric_results = results[node_idx]["numeric"]
        assert numeric_results["A"]["count"] == check_df["A"].count()
        assert numeric_results["B"]["count"] == check_df["B"].count()
        assert numeric_results["C"]["count"] == check_df["C"].count()
        assert numeric_results["A"]["min"] == check_df["A"].min()
        assert numeric_results["B"]["min"] == check_df["B"].min()
        assert numeric_results["C"]["min"] == check_df["C"].min()
        assert numeric_results["A"]["max"] == check_df["A"].max()
        assert numeric_results["B"]["max"] == check_df["B"].max()
        assert numeric_results["C"]["max"] == check_df["C"].max()
        assert numeric_results["A"]["missing"] == check_df["A"].isna().sum()
        assert numeric_results["B"]["missing"] == check_df["B"].isna().sum()
        assert numeric_results["C"]["missing"] == check_df["C"].isna().sum()
        assert numeric_results["A"]["sum"] == check_df["A"].sum()
        assert numeric_results["B"]["sum"] == check_df["B"].sum()
        assert numeric_results["C"]["sum"] == check_df["C"].sum()

        categorical_results = results[node_idx]["categorical"]
        assert categorical_results["D"]["count"] == check_df["D"].count()
        assert categorical_results["E"]["count"] == check_df["E"].count()
        assert categorical_results["F"]["count"] == check_df["F"].count()
        assert categorical_results["D"]["missing"] == check_df["D"].isna().sum()
        assert categorical_results["E"]["missing"] == check_df["E"].isna().sum()
        assert categorical_results["F"]["missing"] == check_df["F"].isna().sum()

        counts_unique_values = results[node_idx]["counts_unique_values"]
        assert counts_unique_values["D"]["1"] == check_df["D"].value_counts().get(
            "1", 0
        )
        assert counts_unique_values["D"]["2"] == check_df["D"].value_counts().get(
            "2", 0
        )
        assert counts_unique_values["D"]["3"] == check_df["D"].value_counts().get(
            "3", 0
        )
        assert counts_unique_values["D"]["4"] == check_df["D"].value_counts().get(
            "4", 0
        )
        assert counts_unique_values["E"]["female"] == check_df["E"].value_counts().get(
            "female", 0
        )
        assert counts_unique_values["E"]["male"] == check_df["E"].value_counts().get(
            "male", 0
        )
        assert counts_unique_values["F"]["other"] == check_df["F"].value_counts().get(
            "other", 0
        )
