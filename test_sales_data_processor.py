import pytest
from sales_data_processor import normalize_product_name, process_sales_data
from pyspark.sql import SparkSession
import json
import os
import csv
from py4j.protocol import Py4JJavaError

tsv_headers = [
        "product_id",
        "store_id",
        "product_name",
        "units",
        "transaction_id",
        "price",
        "timestamp"]


def write_tsv(data: list, output_file: str) -> None:
    with open(output_file, "w", newline='\n') as f:
        writer = csv.DictWriter(f, fieldnames=tsv_headers, delimiter="\t")
        writer.writeheader()
        for row in data:
            writer.writerow(row)


class PipelineRunner:
    def __init__(self, data):
        self.input_file = 'test_sales_data.tsv'
        self.output_file = 'test_sales_profiles.json'
        self.data = data

    def __enter__(self):
        write_tsv(self.data, self.input_file)
        return self

    def read_sales_report(self):
        with open(self.output_file) as f:
            return json.load(f)

    def run_pipeline(self, spark, store_ids):
        process_sales_data(spark, store_ids, self.input_file, self.output_file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            os.remove(self.input_file)
        except FileNotFoundError:
            pass
        try:
            os.remove(self.output_file)
        except FileNotFoundError:
            pass


@pytest.fixture()
def spark():
    return SparkSession.builder \
        .appName("SalesDataProcessorTest") \
        .getOrCreate()


def test_normalize_product_name():
    assert normalize_product_name("coffee-large") == "coffee large"
    assert normalize_product_name("coffee_large") == "coffee large"
    assert normalize_product_name("coffee large") == "coffee large"


def test_filter_negative_and_0_units(spark):
    """units must be positive or 0, negative values of unit must be filtered out."""
    store_ids = {1}
    test_data = [
        {"product_id": 0,
         "store_id": 1,
         "product_name": "coffee_large",
         "units": -3,
         "transaction_id": 1,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"},
        {"product_id": 1,
         "store_id": 1,
         "product_name": "coffee_small",
         "units": 0,
         "transaction_id": 2,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"},
        {"product_id": 2,
         "store_id": 1,
         "product_name": "coffee_medium",
         "units": 1,
         "transaction_id": 3,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"}
    ]
    with PipelineRunner(test_data) as runner:
        runner.run_pipeline(spark, store_ids)
        sales_profiles = runner.read_sales_report()

    assert "coffee large" not in sales_profiles["1"]
    assert "coffee small" not in sales_profiles["1"]
    assert "coffee medium" in sales_profiles["1"]


def test_empty_sales_data(spark):
    """If the sales data is empty, the sales profile should be empty as well."""
    store_ids = {1}
    test_data = []

    with PipelineRunner(test_data) as runner:
        runner.run_pipeline(spark, store_ids)
        sales_profiles = runner.read_sales_report()

    assert sales_profiles == {}


def test_malformed_data_error(spark):
    store_ids = {1}
    test_data = [
        {"product_id": 0,
         "store_id": "what",
         "product_name": "coffee_large",
         "units": 3,
         "transaction_id": 1,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"}
    ]
    with PipelineRunner(test_data) as runner:
        with pytest.raises(Py4JJavaError):
            runner.run_pipeline(spark, store_ids)


def test_process_sales_data(spark):
    store_ids = {1}
    test_data = [
        {"product_id": 0,
         "store_id": 1,
         "product_name": "coffee_large",
         "units": 3,
         "transaction_id": 1,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"},
        {"product_id": 1,
         "store_id": 1,
         "product_name": "coffee_small",
         "units": 1,
         "transaction_id": 2,
         "price": 1.0,
         "timestamp": "2021-12-01 17:48:41.569057"}
    ]

    with PipelineRunner(test_data) as runner:
        runner.run_pipeline(spark, store_ids)
        sales_profiles = runner.read_sales_report()

    assert "1" in sales_profiles  # keys should be strings, not ints
    assert sales_profiles["1"]["coffee large"] == 0.75
    assert sales_profiles["1"]["coffee small"] == 0.25

