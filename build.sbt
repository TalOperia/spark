
name := "spark"
version := "0.1"
scalaVersion := "2.12.15"

exportJars:= false

libraryDependencies += "org.apache.spark" %% "spark-tags" % "3.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client-api" % "3.3.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.12.150"

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}