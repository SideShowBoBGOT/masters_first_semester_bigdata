import org.apache.{spark => spark}
import org.knowm.{xchart => xchart}

enum DataColumns(val value: String):
  case Subject extends DataColumns("Subject")
  case Position extends DataColumns("Position")
  case Session extends DataColumns("Session")
  case Ax extends DataColumns("A_x")
  case Ay extends DataColumns("A_y")
  case Az extends DataColumns("A_z")
  case Gx extends DataColumns("G_x")
  case Gy extends DataColumns("G_y")
  case Gz extends DataColumns("G_z")
  case BodyCapacitance extends DataColumns("C_1")
  case Workout extends DataColumns("Workout")

  def col = spark.sql.functions.col(this.value)

@main def main() =
  val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
  import sparkSession.implicits._
  val seed = 12321L
  val data = sparkSession.read.options(Map(
      "header" -> "true"
    ))
    .csv("traindata/RecGym.csv")
    .select(
      DataColumns.Subject.col.cast(spark.sql.types.IntegerType)
      , DataColumns.Position.col.cast(spark.sql.types.StringType)
      , DataColumns.Session.col.cast(spark.sql.types.IntegerType)
      , DataColumns.Ax.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Ay.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Az.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Gx.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Gy.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Gz.col.cast(spark.sql.types.DoubleType)
      , DataColumns.BodyCapacitance.col.cast(spark.sql.types.DoubleType)
      , DataColumns.Workout.col.cast(spark.sql.types.StringType),
    ).cache()
  val targetN = 500000
  val total = data.count()
  val fraction = math.min(1.0, targetN.toDouble / total.toDouble)
  val sample = data.sample(false, fraction, seed).limit(targetN).cache()

  sparkSession.stop()

