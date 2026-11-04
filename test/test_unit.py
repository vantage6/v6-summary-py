"""
Unit tests for functions that can be tested without the mock network.

Run as:

    uv run pytest test/test_unit.py
"""

import importlib

# The package name contains hyphens, so it cannot be imported with a regular import
central = importlib.import_module("v6-summary-py.central")


def test_aggregate_counts_hidden_column_gets_no_other_labels():
    """A hidden column must not get the values of other columns as keys"""
    node = {"counts_unique_values": {"sex": {"FEMALE": 503, "MALE": 435}, "D": {}}}
    assert central._aggregate_counts_unique_values([node, node], ["sex", "D"]) == {
        "sex": {"FEMALE": 1006, "MALE": 870},
        "D": {},
    }


def test_aggregate_counts_hidden_on_one_node():
    """A column hidden on one node must not show the totals of the other nodes"""
    shown = {"counts_unique_values": {"E": {"female": 10, "male": 12}}}
    hidden = {"counts_unique_values": {"E": {}}}
    not_allowed = {"counts_unique_values": None}
    aggregate = central._aggregate_counts_unique_values
    assert aggregate([shown, hidden], ["E"]) == {"E": {}}
    assert aggregate([shown, not_allowed], ["E"]) == {"E": {}}
