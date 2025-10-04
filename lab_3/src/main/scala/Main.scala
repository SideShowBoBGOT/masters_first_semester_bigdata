import org.apache.{spark => spark}

// def buildPreprocessingPipeline(
//   useScalingForLR: Boolean = true,
//   labelCol: String = "work",  // target: workout
//   posCol: String = "pos",     // categorical: wrist/pocket/calf
//   subjCol: String = "subj"    // optional categorical: subject id
// ): PrepArtifacts = {
//   // Categorical columns to encode (position is definitely helpful; subject optional)
//   val catCols = Array(posCol) // you may add subjCol if you want model to use subject context
//
//   // StringIndexers for categorical + label
//   val posIndexer = new StringIndexer()
//     .setInputCol(posCol)
//     .setOutputCol(s"${posCol}_idx")
//     .setHandleInvalid("keep")
//
//   val labelIndexer = new StringIndexer()
//     .setInputCol(labelCol)
//     .setOutputCol(s"${labelCol}_idx")
//     .setHandleInvalid("error")
//
//   // OneHotEncoder
//   val posEncoder = new OneHotEncoder()
//     .setInputCols(Array(s"${posCol}_idx"))
//     .setOutputCols(Array(s"${posCol}_ohe"))
//
//   val assembledFeaturesCol = "features_raw"
//   val scaledFeaturesCol    = "features" // the column models will use
//   val indexedLabelCol      = s"${labelCol}_idx"
//
//   // Assemble numeric + OHE categorical into features
//   val assemblerInputs = numericCols ++ Array(s"${posCol}_ohe")
//   val assembler = new VectorAssembler()
//     .setInputCols(assemblerInputs)
//     .setOutputCol(assembledFeaturesCol)
//     .setHandleInvalid("keep")
//
//   // Scaling is typically useful for LR; RF is scale-invariant
//   val scaler = new StandardScaler()
//     .setInputCol(assembledFeaturesCol)
//     .setOutputCol(scaledFeaturesCol)
//     .setWithMean(true)
//     .setWithStd(true)
//
//   val stages = Array(posIndexer, labelIndexer, posEncoder, assembler, scaler)
//
//   PrepArtifacts(
//     pipeline = new Pipeline().setStages(stages),
//     featureCols = assemblerInputs,
//     labelCol = labelCol,
//     posIndexerCol = s"${posCol}_idx",
//     posOheCol = s"${posCol}_ohe",
//     featuresCol = if (useScalingForLR) scaledFeaturesCol else assembledFeaturesCol,
//     scaledFeaturesCol = scaledFeaturesCol,
//     indexedLabelCol = indexedLabelCol
//   )
// }
//
// // -----------------------------
//   // 2) Logistic Regression + K-fold CV
//   // -----------------------------
//   final case class CVResult(
//     bestModel: PipelineModel,
//     accuracy: Double,
//     f1: Double
//   )
//
//   def trainEvalLogRegKFold(
//     preparedDf: DataFrame,
//     prep: PrepArtifacts,
//     kFolds: Int,
//     seed: Long
//   ): CVResult = {
//     val classifier = spark.ml.classification.LogisticRegression()
//       .setFeaturesCol("fdsf")
//       .setLabelCol("fdsfsf")
//       .setMaxIter(100)
//       .setFamily("multinomial")
//       .setStandardization(false)
//     val lr = new LogisticRegression()
//       .setFeaturesCol(prep.featuresCol)
//       .setLabelCol(prep.indexedLabelCol)
//       .setMaxIter(100)
//       .setFamily("multinomial") // important for multi-class
//       .setStandardization(false) // we already scaled via StandardScaler
//
//     val lrPipe = new Pipeline().setStages(Array(lr))
//
//     // Hyperparameters for LR (small grid; full tuning task can be done for RF below)
//     val paramGrid = new ParamGridBuilder()
//       .addGrid(lr.regParam, Array(0.0, 1e-2, 1e-1))
//       .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
//       .build()
//
//     val evaluator = new MulticlassClassificationEvaluator()
//       .setLabelCol(prep.indexedLabelCol)
//       .setPredictionCol("prediction")
//       .setMetricName("f1")
//
//     val cv = new CrossValidator()
//       .setEstimator(lrPipe)
//       .setEvaluator(evaluator)
//       .setEstimatorParamMaps(paramGrid)
//       .setNumFolds(kFolds)
//       .setSeed(seed)
//
//     val cvModel = cv.fit(preparedDf)
//
//     val predictions = cvModel.transform(preparedDf)
//
//     val acc = new MulticlassClassificationEvaluator()
//       .setLabelCol(prep.indexedLabelCol)
//       .setPredictionCol("prediction")
//       .setMetricName("accuracy")
//       .evaluate(predictions)
//
//     val f1  = new MulticlassClassificationEvaluator()
//       .setLabelCol(prep.indexedLabelCol)
//       .setPredictionCol("prediction")
//       .setMetricName("f1")
//       .evaluate(predictions)
//
//     CVResult(cvModel.bestModel.asInstanceOf[PipelineModel], acc, f1)
//   }

// @main def main() =
  // val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
  // import sparkSession.implicits._
  // sparkSession.read.options(Map(
  //     "header" -> "true"
  //   ))
  //   .csv("traindata/RecGym.csv")
  //   .select(
  //     $"Subject" as "subj" cast spark.sql.types.IntegerType,
  //     $"Position" as "pos" cast spark.sql.types.StringType,
  //     $"Session" as "ses" cast spark.sql.types.IntegerType,
  //     $"A_x" as "ax" cast spark.sql.types.DoubleType,
  //     $"A_y" as "ay" cast spark.sql.types.DoubleType,
  //     $"A_z" as "az" cast spark.sql.types.DoubleType,
  //     $"G_x" as "gx" cast spark.sql.types.DoubleType,
  //     $"G_y" as "gy" cast spark.sql.types.DoubleType,
  //     $"G_z" as "gz" cast spark.sql.types.DoubleType,
  //     $"C_1" as "cap" cast spark.sql.types.DoubleType,
  //     $"workout" as "work" cast spark.sql.types.StringType
  //   )
  // println("Hello World!")
  // sparkSession.stop()

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
  sparkSession.read.options(Map(
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
    )

  val positionIndexerColumn = DataColumns.Position.value + "indexer"
  val rawFeaturesColumn = "rawFeatures"

  val positionStringIndexer = spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Position.value) 
    .setOutputCol(positionIndexerColumn)
    .setHandleInvalid("keep")
  
  val workoutStringIndexer = spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Workout.value) 
    .setOutputCol(DataColumns.Workout.value)
    .setHandleInvalid("keep")

  val numbericCols = Array(
    DataColumns.Ax.value
    , DataColumns.Ay.value
    , DataColumns.Az.value
    , DataColumns.Gx.value
    , DataColumns.Gy.value
    , DataColumns.Gz.value
    , DataColumns.BodyCapacitance.value
  )

  enum ClassificationType:
    case Logistic, RandomForest

  inline val classificationType = ClassificationType.Logistic

  spark.ml.Pipeline()
    .setStages(
      classificationType match
        case ClassificationType.Logistic =>
          val positionHotEncoderColumn = DataColumns.Position.value + "hotEncoder"
          val scaledFeaturesRawColumn = "scaledFeatures"
          Array(
            positionStringIndexer
            , spark.ml.feature.OneHotEncoder()
              .setInputCol(positionIndexerColumn)
              .setOutputCol(positionHotEncoderColumn)
            , workoutStringIndexer
            , spark.ml.feature.VectorAssembler()
              .setInputCols(Array(positionHotEncoderColumn) ++ numbericCols)
              .setOutputCol(rawFeaturesColumn)
            , spark.ml.feature.StandardScaler()
              .setInputCol(rawFeaturesColumn)
              .setOutputCol(scaledFeaturesRawColumn)
          )
        case ClassificationType.RandomForest =>
          Array(
            positionStringIndexer
            , workoutStringIndexer
            , spark.ml.feature.VectorAssembler()
              .setInputCols(Array(positionIndexerColumn) ++ numbericCols)
              .setOutputCol(rawFeaturesColumn)
          )
    )
    println("Hello World!")
  sparkSession.stop()

