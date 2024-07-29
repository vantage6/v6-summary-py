import pandas as pd

from vantage6.algorithm.tools.util import get_env_var
from vantage6.algorithm.tools.exceptions import PrivacyThresholdViolation
from .globals import (
    DEFAULT_MINIMUM_ROWS,
    DEFAULT_PRIVACY_THRESHOLD,
    ENVVAR_ALLOWED_COLUMNS,
    ENVVAR_DISALLOWED_COLUMNS,
    ENVVAR_MINIMUM_ROWS,
    ENVVAR_PRIVACY_THRESHOLD,
)


def check_privacy(df: pd.DataFrame, requested_columns: list[str]) -> None:
    """
    Check if the data complies with the privacy settings

    Parameters
    ----------
    df : pd.DataFrame
        The data to check
    requested_columns : list[str]
        The columns that are requested in the computation
    """
    min_rows = get_env_var(
        ENVVAR_MINIMUM_ROWS, default=DEFAULT_MINIMUM_ROWS, as_type="int"
    )
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


def check_match_inferred_is_numeric(
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
        msg = ""
        if wrongly_numeric_columns:
            # try to cast the columns to numeric
            try:
                df = cast_df_to_numeric(df, wrongly_numeric_columns)
            except ValueError as exc:
                msg += str(exc)
        if wrongly_non_numeric_columns:
            msg += (
                f"Columns {wrongly_non_numeric_columns} are numeric, but is_numeric is "
                "set to False"
            )
        if msg:
            raise ValueError(msg)
    return df


def cast_df_to_numeric(
    df: pd.DataFrame, columns: list[str] | None = None
) -> pd.DataFrame:
    """
    Cast the columns in the dataframe to numeric if possible

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to cast
    columns : list[str] | None
        The columns to cast. If None, all columns are cast

    Returns
    -------
    pd.DataFrame
        The dataframe with the columns cast to numeric
    """
    if columns is None:
        columns = df.columns
    for col in columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError as exc:
            raise ValueError(f"Column {col} could not be cast to numeric") from exc
    return df
