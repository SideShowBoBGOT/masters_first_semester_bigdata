import org.apache.{spark => spark}

@main def main() =
  val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
  import sparkSession.implicits._
  sparkSession.read.options(Map(
      "header" -> "true"
    ))
    .csv("traindata/RecGym.csv")
    .select(
      $"Subject" as "subj",
      $"Position" as "pos",
      $"Session" as "ses",
      $"A_x" as "ax",
      $"A_y" as "ay",
      $"A_z" as "az",
      $"G_x" as "gx",
      $"G_y" as "gy",
      $"G_z" as "gz",
      $"C_1" as "cap",
      $"workout" as "work"
    )
  println("Hello World!")
  sparkSession.stop()
