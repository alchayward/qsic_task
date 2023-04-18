# Qsic Coding Test


## running
To run the pipeline and generate the sales profiles for sales_data.tsv:

Create a python environment and install the requirements.

run: `python sales_data_processor.py`

The resulting sales profile is in [sales_profile.json](sales_profiles.json)
## coverage report

[Coverage report is found here.](coverage_report/index.html)

To generate (and run tests):
`python -m pytest --cov-report=html:coverage_re --cov sales_data_processor`

## notes

There is only some data validation here. More is always possible, but depends on the providence of the data etc. 

The question of what to do in various edge cases (e.g. no data for a specified store), depend really on the business case at hand, and cant be decided without wider consultation.

