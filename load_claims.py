from google.cloud import storage
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import Row


if __name__ == "__main__":
    """
    Usage: load_claims.py [file_path] [year] [quarter]
    Example: load_claims.py gs://claims-data-spark-test-ghen/Episodes_of_care_1.txt 2011 1
    Note:
    """
    def parse_claim(claim):
        return Row(
            year: int(year),
            quarter: int(quarter),
            ...
        )

    claims_file_path = int(sys.argv[1])
    year = int(sys.argv[2])
    quarter = int(sys.argv[3])

    sc = SparkContext("local", "Claims Data Loader")
    hc = HiveContext(sc)


    csv_data = sc.textFile(claims_file_path)
    csv_data = csv_data.map(lambda p: p.split(",")) # Assumes pretty clean data files
    df_csv = csv_data.map(parse_claim).toDF()

    # NOTE: This is a really simple load prone to errors from duplication
    df_csv.write.format("orc").mode("append").saveAsTable("claims")

    sc.stop()
