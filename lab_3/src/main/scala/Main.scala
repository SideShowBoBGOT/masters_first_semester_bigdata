import org.apache.{spark => spark}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    println("Hello World!")
    sparkSession.stop()
  }
}
