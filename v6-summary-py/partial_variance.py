"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd

from vantage6.algorithm.tools.util import info, warn, error, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import PrivacyThresholdViolation, InputError
from .globals import (
    DEFAULT_MINIMUM_ROWS,
    DEFAULT_PRIVACY_THRESHOLD,
    ENVVAR_ALLOWED_COLUMNS,
    ENVVAR_DISALLOWED_COLUMNS,
    ENVVAR_MINIMUM_ROWS,
    ENVVAR_PRIVACY_THRESHOLD,
)


@data(1)
def variance_per_data_station(
    df: pd.DataFrame, columns: list[str], means: list[float]
) -> dict:
    """
    Compuate the variance for a single data station to share with the
    aggregator part of the algorithm

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    columns : list[str]
        The columns to compute the summary statistics for
    means: list[float]
        The means of the columns

    Returns
    -------
    dict
        Contains the variance of the numeric columns
    """
    # Check that columnn names exist in the dataframe - note that this check should
    # not be necessary if a user runs the central task as is has already been checked
    # in that case
    if not all([col in df.columns for col in columns]):
        non_existing_columns = [col for col in columns if col not in df.columns]
        raise InputError(
            f"Columns {non_existing_columns} do not exist in the dataframe"
        )
    if len(columns) != len(means):
        raise InputError(
            "Length of columns list does not match the length of means list"
        )

    # filter dataframe to only include the columns of interest
    df = df[columns]

    # TODO generalize this function as it is also used in the other task
    # Check privacy settings
    min_length_df = get_env_var(
        ENVVAR_MINIMUM_ROWS, default=DEFAULT_MINIMUM_ROWS, as_type="int"
    )
    info("Checking if data complies to privacy settings")
    _check_privacy(df, min_length_df, columns)

    info("Calculating variance")
    variances = {}
    print(columns)
    print(means)
    for idx in range(len(columns)):
        column = columns[idx]
        mean = means[idx]
        variances[column] = ((df[column] - mean) ** 2).sum()

    return variances


def _check_privacy(
    df: pd.DataFrame, min_rows: int, requested_columns: list[str]
) -> None:
    """
    Check if the data complies with the privacy settings

    Parameters
    ----------
    df : pd.DataFrame
        The data to check
    min_rows : int
        The minimum length of the data frame
    requested_columns : list[str]
        The columns that are requested in the computation
    """
    if len(df) < min_rows:
        raise PrivacyThresholdViolation(
            f"Data contains less than {min_rows} rows. Refusing to "
            "handle this computation, as it may lead to privacy issues."
        )
    # check that each column has at least min_rows non-null values
    for col in df.columns:
        if df[col].count() < min_rows:
            raise PrivacyThresholdViolation(
                f"Column {col} contains less than {min_rows} non-null values. "
                "Refusing to handle this computation, as it may lead to privacy issues."
            )

    # Check if requested columns are allowed
    allowed_columns = get_env_var(ENVVAR_ALLOWED_COLUMNS)
    if allowed_columns:
        allowed_columns = allowed_columns.split(",")
        for col in requested_columns:
            if col not in allowed_columns:
                raise ValueError(
                    f"The node administrator does not allow '{col}' to be requested in "
                    "this algorithm computation. Please contact the node administrator "
                    "for more information."
                )
    non_allowed_collumns = get_env_var(ENVVAR_DISALLOWED_COLUMNS)
    if non_allowed_collumns:
        non_allowed_collumns = non_allowed_collumns.split(",")
        for col in requested_columns:
            if col in non_allowed_collumns:
                raise ValueError(
                    f"The node administrator does not allow '{col}' to be requested in "
                    "this algorithm computation. Please contact the node administrator "
                    "for more information."
                )
