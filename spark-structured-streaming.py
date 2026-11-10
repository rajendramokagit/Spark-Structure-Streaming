# Converted from notebook: spark-structured-sreaming
# Spark Structured Streaming: read customer CSVs from a UC volume and stream them
# into a Delta sink with an Available Now trigger.

# Cell 1: Define the schema for the customer CSV files
my_schema = """Index int,Customer_Id string,First_Name string,Last_Name string,Company string,City string,Country string"""

# Cell 2: Show the schema string
print(my_schema)

# Cell 3: Define the streaming read from the source volume
df_rs = (spark.readStream.format("csv")
    .option("inferSchema", "True")
    .option("header", "True")
    .schema(my_schema)
    .load("/Volumes/pyspark1/source/customers")
)

# Cell 4: Write the stream to a Delta sink and wait for it to finish
query = (df_rs.writeStream.format("delta")
    .option("checkPointLocation", "/Volumes/pyspark1/target/checkpoint/")
    .option("mergeSchema", "True")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/Volumes/pyspark1/target/cust_target/data/")
)

# Wait for the Available Now trigger to process all available data and stop
query.awaitTermination()

# Cell 5: Verify the written Delta data
spark.sql("select * from delta.`/Volumes/pyspark1/target/cust_target/data/`").show(truncate=False)
