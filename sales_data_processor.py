from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType, TimestampType
import json


def normalize_product_name(product_name):
    return product_name.replace("-", " ").replace("_", " ").lower()

normalize_product_name_udf = udf(normalize_product_name, StringType())


schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("store_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("units", IntegerType(), nullable=False),
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("price", FloatType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False)
])


def process_sales_data(spark, store_ids, input_file, output_file):
    """

    :param spark: spark session
    :param store_ids: which store ids to include in the sales profile
    :param input_file: file path to the TSV file
    :param output_file: file path to the JSON file
    :return: None

    In the case of bad values in the CSV, pipeline will fail, rather than dropping rows.
    In the case of a store with no valid transactions, pipeline will return no data for this store.
    """

    # Read the TSV file
    df = spark.read.csv(input_file, sep="\t", header=True, schema=schema, mode="FAILFAST")

    # Normalize product names and filter out invalid transactions
    df = df.withColumn("product_name", normalize_product_name_udf(col("product_name"))) \
        .filter(col("units") > 0)


    # Filter for the selected store ids
    df = df.filter(col("store_id").isin(store_ids))

    # Aggregate the data
    df = df.groupBy("store_id", "product_name") \
        .sum("units") \
        .withColumnRenamed("sum(units)", "units_sum")

    # Convert to Pandas DataFrame
    pdf = df.toPandas()

    # Compute the sales profiles
    pdf["units_sum_normalized"] = pdf.groupby("store_id")["units_sum"].transform(lambda x: x / x.sum())

    # Pivot and convert to JSON
    sales_profiles = pdf.pivot_table(index="store_id", columns="product_name", values="units_sum_normalized") \
        .rename_axis(index=None, columns=None) \
        .to_dict(orient="index")

    # Convert the keys (store ids) of sales_profiles to strings, as specified in the example schema
    sales_profiles = {str(k): v for k, v in sales_profiles.items()}

    # Write the result to a JSON file
    with open(output_file, "w") as f:
        json.dump(sales_profiles, f, indent=2)


if __name__ == "__main__":
    store_ids = {1, 3}
    input_file = "big_sales_data.tsv"
    output_file = "sales_profiles.json"

    spark = SparkSession.builder \
        .appName("SalesDataProcessor") \
        .getOrCreate()

    process_sales_data(spark, store_ids, input_file, output_file)
