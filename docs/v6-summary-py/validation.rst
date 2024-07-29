Validation
==========

A `test script <https://github.com/vantage6/v6-summary-py/blob/main/test/test.py>`_ is
available in the `test` directory. It contains `pytest` unit tests and can be run with
the following command:

.. code-block:: bash

    pytest test/test.py

Be sure to install ``pytest`` before running this command. The script will run the
summary algorithm via the vantage6 ``MockAlgorithmClient``.
