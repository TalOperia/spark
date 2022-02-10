import org.apache.spark.sql.SparkSession


object Program {

  def main(aggs: Array[String]): Unit = {

    /*val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.0.6.4:9092,10.0.6.5:9092,10.0.6.6:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )*/

    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("spark-ggggg")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("park.ui.port", "9000")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "7pfy6izbIIrK3IIMNBXJlor5gvE=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "XyyLqrhiABeTO2dAO/gDGAoa7qRuYhKcJriJZVKA/c4=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://10.0.6.160:9000")

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.6.4:9092,10.0.6.5:9092,10.0.6.6:9092")
      .option("subscribe", "test1")
      .load()

    df.write
      .format("parquet")
      .parquet("s3a://test/parquet/2022/02/10")

    /*val parquetFileDF = spark.read.parquet("s3a://test/parquet/2022/02/10")
    val df = parquetFileDF.withColumn("key_str", parquetFileDF.col("key").cast("String").alias("key_str"))
      .drop("key").withColumn("value_str", parquetFileDF.col("value").cast("String").alias("value_str"))
      .drop("value").withColumnRenamed("key_str", "key").withColumnRenamed("value_str", "value")
    df.show()*/

  }
}
