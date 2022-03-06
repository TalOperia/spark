import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, SparkSession}

import scala.concurrent.duration.DurationInt


object Program {

  def main(aggs: Array[String]): Unit = {

    val schema = new StructType()
      .add("url", StringType, true)
      .add("deviceType", StringType, true)
      .add("type", StringType, true)

    val spark: SparkSession = SparkSession.builder()
      .appName("spark")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "7pfy6izbIIrK3IIMNBXJlor5gvE=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "XyyLqrhiABeTO2dAO/gDGAoa7qRuYhKcJriJZVKA/c4=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://10.0.7.92:9000")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.7.84:9092,10.0.7.86:9092,10.0.7.88:9092")
      .option("subscribe", "metadata")
      .option("kafka.group.id", "spark")
      .option("startingOffsets", "latest")
      .load()

    val parquetQuery = df
      .coalesce(1)
      .withColumn("key_str", df.col("key").cast("String").alias("key_str"))
      .drop("key").withColumn("value_str", df.col("value").cast("String").alias("value_str"))
      .drop("value").withColumnRenamed("key_str", "key")
      .withColumnRenamed("value_str", "value")
      .writeStream
      .format("parquet")
      .option("path", "s3a://test/parquet/2022/02/11")
      .trigger(Trigger.ProcessingTime(1.minutes))
      .option("checkpointLocation", "s3a://test/parquet/2022/02/11/checkpoint")
      .outputMode(OutputMode.Append())
      .start()

    parquetQuery.awaitTermination()
  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }
}

/*val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.0.6.4:9092,10.0.6.5:9092,10.0.6.6:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )*/

/*val parquetFileDF = spark.read.parquet("s3a://test/parquet/2022/02/10")
    val df = parquetFileDF.withColumn("key_str", parquetFileDF.col("key").cast("String").alias("key_str"))
      .drop("key").withColumn("value_str", parquetFileDF.col("value").cast("String").alias("value_str"))
      .drop("value").withColumnRenamed("key_str", "key").withColumnRenamed("value_str", "value")
    df.show()*/

/*val df = spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "10.0.7.84:9092,10.0.7.86:9092,10.0.7.88:9092")
  .option("subscribe", "metadata")
  .option("kafka.group.id", "spark")
  .load()

val dfJson = df.withColumn("key_str", df.col("key").cast("String").alias("key_str"))
  .drop("key").withColumn("value_str", df.col("value").cast("String").alias("value_str"))
  .drop("value").withColumnRenamed("key_str", "key")
  .withColumnRenamed("value_str", "value")
  .withColumn("jsonData",from_json(col("value"),schema))
val DF = dfJson.select(flattenSchema(dfJson.schema):_*)
DF.printSchema()
DF.show()

DF.write
    .format("parquet")
    .parquet("s3a://test/parquet/2022/02/11")*/

