import org.apache.{spark => spark}
import org.knowm.{xchart => xchart}

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

def exportPerClassROC(
  predictions: spark.sql.DataFrame,
  labelColumnName: String, 
  sparkSession: spark.sql.SparkSession
) = 
  val outDir = "build/rocs"
  val labelColumn = spark.sql.functions.col(labelColumnName)
  val numClasses = predictions
    .select(labelColumn)
    .agg(spark.sql.functions.max(labelColumn))
    .head()
    .getDouble(0)
    .toInt + 1

  (0 until numClasses).foreach { c =>
    val scoreAndLabel =
      predictions
        .select(spark.sql.functions.col("probability"), labelColumn.cast(spark.sql.types.DoubleType))
        .rdd
        .map { row =>
          val prob = row.getAs[org.apache.spark.ml.linalg.Vector]("probability")
          val lab  = row.getDouble(1)
          val scoreForC = prob(c)
          val binLabel  = if (lab == c.toDouble) 1.0 else 0.0
          (scoreForC, binLabel)
        }
    val metrics = spark.mllib.evaluation.BinaryClassificationMetrics(scoreAndLabel)
    val rocPoints = metrics.roc().collect()
    val xs = rocPoints.map(_._1).toArray
    val ys = rocPoints.map(_._2).toArray
    val auc = metrics.areaUnderROC()
    val chart = xchart.XYChartBuilder()
      .width(1920)
      .height(1080)
      .title(s"ROC (class $c) AUC=${"%.4f".format(auc)}")
      .xAxisTitle("False Positive Rate")
      .yAxisTitle("True Positive Rate")
      .build()
    val series = chart.addSeries("ROC", xs, ys)
    series.setMarker(xchart.style.markers.None())
    chart
      .addSeries("Random", Array(0.0, 1.0), Array(0.0, 1.0))
      .setMarker(xchart.style.markers.None())
    java.io.File(outDir).mkdirs()
    xchart.BitmapEncoder.saveBitmap(chart, s"$outDir/roc_class_$c", xchart.BitmapEncoder.BitmapFormat.PNG)
  }

def testLogisticRegression(sparkSession: spark.sql.SparkSession, data: spark.sql.DataFrame, seed: Long) =

  val positionIndexerColumn = DataColumns.Position.value + "indexer"
  val positionHotEncoderColumn = DataColumns.Position.value + "hotEncoder"
  val workoutIndexerColumn = DataColumns.Workout.value + "indexer"
  val positionStringIndexer = spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Position.value) 
    .setOutputCol(positionIndexerColumn)
    .setHandleInvalid("keep")
  val workoutStringIndexer = spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Workout.value) 
    .setOutputCol(workoutIndexerColumn)
    .setHandleInvalid("keep")
  val numericCols = Array(
    DataColumns.Ax.value
    , DataColumns.Ay.value
    , DataColumns.Az.value
    , DataColumns.Gx.value
    , DataColumns.Gy.value
    , DataColumns.Gz.value
    , DataColumns.BodyCapacitance.value
  )
  val assembeledNumericColumn = "assembeledNumericColumns"
  val scaledNumericColumn = "scaledNumericColumn"
  val assembeledFeatures = "assembeledFeatures"
  val kFolds = 5

  val classifier = spark.ml.classification.LogisticRegression()
    .setFeaturesCol(assembeledFeatures)
    .setLabelCol(workoutIndexerColumn)
    .setMaxIter(100)
    .setFamily("multinomial")
    .setStandardization(false)

  val grid = spark.ml.tuning.ParamGridBuilder()
    .addGrid(classifier.regParam, Array(0.0, 1e-1))
    .addGrid(classifier.elasticNetParam, Array(0.0, 1.0))
    // .addGrid(classifier.regParam, Array(0.0, 1e-2, 1e-1))
    // .addGrid(classifier.elasticNetParam, Array(0.0, 0.5, 1.0))
    // .addGrid(classifier.elasticNetParam, Array(0.0))
    .build()
  
  val estimator = spark.ml.Pipeline()
    .setStages(
      Array(
        positionStringIndexer
        , spark.ml.feature.OneHotEncoder()
          .setInputCol(positionIndexerColumn)
          .setOutputCol(positionHotEncoderColumn)
          .setHandleInvalid("keep")
        , workoutStringIndexer
        , spark.ml.feature.VectorAssembler()
          .setInputCols(numericCols)
          .setOutputCol(assembeledNumericColumn)
        , spark.ml.feature.StandardScaler()
          .setInputCol(assembeledNumericColumn)
          .setOutputCol(scaledNumericColumn)
        , spark.ml.feature.VectorAssembler()
          .setInputCols(Array(scaledNumericColumn, positionHotEncoderColumn))
          .setOutputCol(assembeledFeatures)
        , classifier
      )
    )

  val f1Eval = spark.ml.evaluation.MulticlassClassificationEvaluator()
      .setLabelCol(workoutIndexerColumn)
      .setPredictionCol("prediction")
      .setMetricName("f1")

  val cvF1 = spark.ml.tuning.CrossValidator()
      .setEstimator(estimator)
      .setEvaluator(f1Eval)
      .setEstimatorParamMaps(grid)
      .setNumFolds(kFolds)
      .setSeed(seed)

  val cached = data.cache()
  val cvModelF1 = cvF1.fit(cached)
  val bestF1 = cvModelF1.avgMetrics.max
  
  val predsForROC = cvModelF1.bestModel.transform(cached)
  exportPerClassROC(predsForROC, workoutIndexerColumn, sparkSession) 

  val accEval = spark.ml.evaluation.MulticlassClassificationEvaluator()
    .setLabelCol(workoutIndexerColumn)
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val cvACC = spark.ml.tuning.CrossValidator()
    .setEstimator(estimator)
    .setEvaluator(accEval)
    .setEstimatorParamMaps(grid)
    .setNumFolds(kFolds)
    .setSeed(seed)

  val cvModelACC = cvACC.fit(cached)
  val bestACC = cvModelACC.avgMetrics.max

  println(s"columns ${predsForROC.columns}")
  println(f"[LR][CV] mean-F1 (best-by-F1): $bestF1%.4f")
  println(f"[LR][CV] mean-ACC (best-by-ACC): $bestACC%.4f")

  
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
    )
  val targetN = 100000
  val total = data.count()
  val fraction = math.min(1.0, targetN.toDouble / total.toDouble)
  val sample = data.sample(false, fraction, seed).limit(targetN).cache()
  // sample.write
  //   .mode("overwrite")
  //   .option("header", "true")
  //   .option("quoteAll", "true")
  //   .csv("traindata/sample.csv")
  testLogisticRegression(sparkSession, sample, seed)
  sparkSession.stop()

