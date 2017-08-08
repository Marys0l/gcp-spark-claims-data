from google.cloud import storage
from pyspark.sql import HiveContext
from pyspark.sql import Row

# Step 1: Open up data from Google Cloud Storage and convert it to a DataFrame
csv_data = sc.textFile('gs://claims-data-spark-test-ghen/VHA_quality.csv')
csv_data = csv_data.map(lambda p: p.split(","))
header = csv_data.first()
csv_data = csv_data.filter(lambda p:p != header)
csv_data.first()

# NOTE: We have hard-coded the schema here and parse datatypes
df_csv = csv_data.toDF()

# Step 2: Append the data frame to a HIVE table
hc = HiveContext(sc)
df_csv.write.format("orc").mode("append").saveAsTable("claims")
