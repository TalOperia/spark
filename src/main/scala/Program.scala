import org.apache.spark.sql.SparkSession


object Program {

  def main(aggs: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("spark clickhous")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("park.ui.port", "9000")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "7pfy6izbIIrK3IIMNBXJlor5gvE=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "XyyLqrhiABeTO2dAO/gDGAoa7qRuYhKcJriJZVKA/c4=")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://10.0.6.160:9000")

    val data = Seq(("James", "39192", "Smith", "36636", "M", 3000),
      ("Michael", "Rose", "39192", "40288", "M", 4000),
      ("Robert", "39192", "Williams", "42114", "M", 4000),
      ("Maria", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "39192", "F", -1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)
    df.show(3)

    df.write.parquet("s3a://test/parquet/people.parquet")
  }
}
