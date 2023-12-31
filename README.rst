===============
CODAC assesment
===============

Programming Exercise using PySpark.

Running
=======

Script:
-------

``python .\codac\codac.py <path1> <path2> <filter [filter ...]>``

where:
* ``path1`` - path to the firs CSV file,
* ``path2`` - path to the second CSV file,
* ``filter`` - list of phrases to filter data by (phrase with spaces should be quoted).

Assesment example:
------------------

``python .\codac\codac.py .\data\dataset_one.csv .\data\dataset_two.csv "United Kingdom" Netherlands``

where:
* ``.\data\dataset_one.csv`` - path to CSV file with clients information,
* ``.\data\dataset_two.csv`` - path to CSV file with clients financial data,
* ``"United Kingdom" Netherlands`` - list of countries to filter data by.

Results
-------

Results are saved in: ``client_data/dataset.csv``

Testing
=======

To run all tests:
* go to main codac directory
* ``cd codac``
* ``python -m unittest``

To run end to end test only:
* go to main codac directory
* ``cd codac``
* ``python -m unittest tests.test_end_to_end ``
