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
def summary_per_data_station(
    df: pd.DataFrame, columns: list[str], is_numeric: list[bool] | None = None
) -> dict:
    """
    Compuate the summary statistics for a single data station to share with the
    aggregator part of the algorithm

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    columns : list[str]
        The columns to compute the summary statistics for
    is_numeric : list[bool]
        Whether the columns are numeric or not. For non-numeric columns, other summary
        statistics are computed. If not provided, it will be inferred.

    Returns
    -------
    dict | None
        The summary statistics for the data station. If the summary statistics cannot
        be computed, None is returned
    """
    # Check that columnn names exist in the dataframe
    if not all([col in df.columns for col in columns]):
        non_existing_columns = [col for col in columns if col not in df.columns]
        raise InputError(
            f"Columns {non_existing_columns} do not exist in the dataframe"
        )

    # filter dataframe to only include the columns of interest
    df = df[columns]

    # Get privacy settings from environment variables
    min_length_df = get_env_var(
        ENVVAR_MINIMUM_ROWS, default=DEFAULT_MINIMUM_ROWS, as_type="int"
    )

    # Check privacy settings
    info("Checking if data complies to privacy settings")
    _check_privacy(df, min_length_df, columns)

    # Split the data in numeric and non-numeric columns
    inferred_is_numeric = [df[col].dtype in [int, float] for col in df.columns]
    if is_numeric is None:
        is_numeric = inferred_is_numeric
    else:
        df = _check_match_inferred_is_numeric(
            is_numeric, inferred_is_numeric, columns, df
        )

    # set numeric and non-numeric columns
    numeric_columns = [col for col, is_num in zip(columns, is_numeric) if is_num]
    non_numeric_columns = [
        col for col, is_num in zip(columns, is_numeric) if not is_num
    ]
    df_numeric = df[numeric_columns]
    df_non_numeric = df[non_numeric_columns]

    # compute data summary for numeric columns
    summary_numeric = pd.DataFrame()
    if not df_numeric.empty:
        summary_numeric = _get_numeric_summary(df_numeric)

    # compute data summary for non-numeric columns. Also compute the counts of the
    # unique values in the non-numeric columns (if they meet the privacy threshold)
    summary_categorical = pd.DataFrame()
    counts_unique_values = {}
    if not df_non_numeric.empty:
        summary_categorical = _get_categorical_summary(df_non_numeric)
        counts_unique_values = _get_counts_unique_values(df_non_numeric)

    # count complete rows without missing values
    num_complete_rows_per_node = len(df.dropna())
    return {
        "numeric": summary_numeric.to_dict(),
        "categorical": summary_categorical.to_dict(),
        "num_complete_rows_per_node": num_complete_rows_per_node,
        "counts_unique_values": counts_unique_values,
    }


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


def _check_match_inferred_is_numeric(
    is_numeric: list[bool],
    inferred_is_numeric: list[bool],
    columns: list[str],
    df: pd.DataFrame,
):
    """
    Check if the provided is_numeric list matches the inferred is_numeric list

    Parameters
    ----------
    is_numeric : list[bool]
        The provided is_numeric list
    inferred_is_numeric : list[bool]
        The inferred is_numeric list
    columns : list[str]
        The columns for which the is_numeric list is provided
    df: pd.DataFrame
        The original data. The type of the data may be modified if possible
    """
    if len(is_numeric) != len(columns):
        raise ValueError(
            "Length of is_numeric list does not match the length of columns list"
        )
    if not all(
        [is_numeric[i] == inferred_is_numeric[i] for i in range(len(is_numeric))]
    ):
        # check which columns do not match
        wrongly_numeric_columns = [
            columns[i]
            for i in range(len(columns))
            if is_numeric[i] and not inferred_is_numeric[i]
        ]
        wrongly_non_numeric_columns = [
            columns[i]
            for i in range(len(columns))
            if not is_numeric[i] and inferred_is_numeric[i]
        ]
        # TODO there still needs to be a try to cast the columns to numeric
        msg = ""
        if wrongly_numeric_columns:
            # try to cast the columns to numeric
            try:
                for col in wrongly_numeric_columns:
                    df[col] = pd.to_numeric(df[col])
            except ValueError:
                msg += (
                    f"Columns {wrongly_numeric_columns} are not numeric, but is_numeric"
                    " is set to True\n"
                )
        if wrongly_non_numeric_columns:
            msg += (
                f"Columns {wrongly_non_numeric_columns} are numeric, but is_numeric is "
                "set to False"
            )
        if msg:
            raise ValueError(msg)
    return df


def _get_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the summary statistics for the numeric columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to compute the summary statistics for
    """
    summary_numeric = df.describe(include=[int, float], percentiles=[])
    summary_numeric.loc["missing"] = df.isna().sum()
    summary_numeric.loc["sum"] = df.sum()
    summary_numeric.drop(["50%", "mean", "std"], inplace=True)
    return summary_numeric


def _get_categorical_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the summary statistics for the non-numeric columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to compute the summary statistics for
    """
    # summary for non-numeric columns. Include the NA count and remove the values
    # that we don't want to share
    summary_categorical = df.describe(exclude=[int, float])
    summary_categorical.loc["missing"] = df.isna().sum()
    summary_categorical.drop(["top", "freq", "unique"], inplace=True)
    return summary_categorical


def _get_counts_unique_values(df: pd.DataFrame) -> dict:
    """
    Get the counts of the unique values in categorical columns

    Parameters
    ----------
    df : pd.DataFrame
        The data to get the counts of the unique values for

    Returns
    -------
    dict
        The counts of the unique values
    """
    counts = {}
    privacy_threshold = get_env_var(
        ENVVAR_PRIVACY_THRESHOLD, default=DEFAULT_PRIVACY_THRESHOLD, as_type="int"
    )
    for col in df.columns:
        counts[col] = _mask_privacy(df[col].value_counts(), privacy_threshold, col)
    return counts


def _mask_privacy(counts: pd.Series, privacy_threshold: int, column: str) -> dict:
    """
    Mask the values of a pandas series if the frequency is too low

    Parameters
    ----------
    counts : pd.Series
        The counts of the unique values
    privacy_threshold : int
        The minimum frequency of a value to be shared
    column : str
        The name of the column whose values are counted

    Returns
    -------
    pd.Series
        The masked counts
    """
    num_low_counts = counts[counts < privacy_threshold].sum()
    if num_low_counts > 0:
        # It may be possible to share ranges of values instead of the actual values,
        # but we need to be vary careful. E.g. if the dataframe length is 20 and we
        # have frequencies 2 and 18, masking 2 as 0-5 while sharing 18 and 20 is not
        # effective. Similarly, if we have frequencies 17 and three times 1, masking 1
        # as 0-5 thrice and sharing 17 is also not helpful.
        # Because it is rather difficult to ensure that nothing can be inferred, we
        # choose not to share anything if one of the frequencies is too low.
        # TODO how do we make clear to the user that this happened in the central task?
        warn(
            f"Value counts for column {column} contain values with low frequency. "
            "All counts for this column will be masked."
        )
        return {}
    return counts.to_dict()
