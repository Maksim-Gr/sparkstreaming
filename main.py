from pyspark.sql import SparkSession
from config.schema import schema
from pyspark.sql import functions as f


def main():
    spark = (SparkSession
             .builder
             .appName("aip.adf.streaming")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    source = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap", "localhost:9092")
              .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
              .option("kafka.security.protocol", "SASL_PLAINTEXT")
              .option("subscribe", "testTopic")
              .load())

    df = (source.selectExpr("CAST(value AS STRING)"))
    df = (df
          .select(f.from_json("value", schema).alias("data")))

    console = (df.writeStream.format("console").queryName("console output"))
    console.start().awaitTermination()


if __name__ == "__main__":
    main()
