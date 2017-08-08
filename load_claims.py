import csv
import sys
from pyspark import SparkContext
from google.cloud import storage
from pyspark.sql import HiveContext, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

if __name__ == "__main__":
    """
    Usage: load_claims.py [file_path]
    Example: load_claims.py gs://claims-data-spark-test-ghen/Episodes_of_care_1.txt
    """
    file_path = sys.argv[1]


    sc = SparkContext("local", "Claims Data Loader")
    hc = HiveContext(sc)

    # Step 1: Open up TSV data file from Google Cloud Storage and parse it
    csv_data = sc.textFile(file_path)
    csv_data = csv_data.map(lambda p: p.split("\t"))

    # Step 2: Define the schema for the to be constructed DataFrame
    header = csv_data.first()
    fields = [StructField(field_name, StringType(), True) for field_name in header]

    fields[0].dataType = IntegerType() # year
    fields[4].dataType = IntegerType() # race
    fields[5].dataType = IntegerType() # ethnicity
    fields[6].dataType = IntegerType() # language
    fields[7].dataType = IntegerType() # metro
    fields[8].dataType = FloatType()   # paid
    fields[9].dataType = FloatType()   # patpaid

    schema = StructType(fields[0:10]) # Only keep up patient demographics and payment info

    # Step 3: Convert the CSV to a DataFrame
    csv_data = csv_data.filter(lambda p:p != header)

    def safe_parse(function, value):
        # Handles '' value when parsing int() or float()
        return function(value) if value != '' else None

    csv_df = csv_data.map(lambda p: (int(p[0]), p[1], p[2], p[3], safe_parse(int, p[4]), safe_parse(int, p[5]), safe_parse(int, p[6]), safe_parse(int, p[7]), safe_parse(float, p[8]), safe_parse(float, p[9]))).toDF(schema)

    # Step 4: Persist the DataFrame to HIVE
    # NOTE: Using mode("append") like this is brittle since this let's
    #       us load duplicated data. This is just an example, in production
    #       you would want to do something to maintain this table's integrity
    hc = HiveContext(sc)
    csv_df.write.format("orc").mode("append").saveAsTable("all_claims")

    sc.stop()
