""" Global variables for the summary package. """

# names of environment variables
## minimum number of rows in the dataframe
ENVVAR_MINIMUM_ROWS = "SUMMARY_MINIMUM_ROWS"
## whitelist of columns allowed to be requested
ENVVAR_ALLOWED_COLUMNS = "SUMMARY_ALLOWED_COLUMNS"
## blacklist of columns not allowed to be requested
ENVVAR_DISALLOWED_COLUMNS = "SUMMARY_DISALLOWED_COLUMNS"
## privacy threshold for count of a unique value in a categorical column
ENVVAR_PRIVACY_THRESHOLD = "SUMMARY_PRIVACY_THRESHOLD"

# default values for environment variables
DEFAULT_MINIMUM_ROWS = 10
DEFAULT_PRIVACY_THRESHOLD = 5
