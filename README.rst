========
db_drift
========


.. image:: https://img.shields.io/pypi/v/db_drift.svg
        :target: https://pypi.python.org/pypi/db_drift

.. image:: https://img.shields.io/travis/gndctrl2mjrtm/db_drift.svg
        :target: https://travis-ci.com/gndctrl2mjrtm/db_drift

.. image:: https://readthedocs.org/projects/db-drift/badge/?version=latest
        :target: https://db-drift.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Databricks Data Drift Library

Data Drift is a common problem with data quality where underlying changes to the data can upset downstream processes.
A situation where this commonly happens is in ML training where a model that was once trained on the data cannot accurately
work with new data because of a drift in the data itself. Examples of this might be changes in upstream processes, customer
behavior, or malfunctions in product signals. Detection of changes of data is critical for data engineers in order to improve
data quality and to alert data science teams.

This library has tools for gathering metrics for data drift and anomaly detectors.


* Free software: Apache Software License 2.0
* Documentation: https://db-drift.readthedocs.io.


Features
--------

* Add support for additional anomaly detection models
* Collect more metrics with the metadata collector
* Add the detectors to the Data Drift object

