import org.apache.{spark => spark}

@main def main() =
  val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
  println("Hello World!")
  sparkSession.stop()
