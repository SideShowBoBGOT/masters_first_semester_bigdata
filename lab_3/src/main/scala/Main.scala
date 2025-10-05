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

def exportPerClassROC(
  sparkSession: spark.sql.SparkSession
  , predictions: spark.sql.DataFrame
  , labelColumnName: String
  , outDir: String
) = 
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

def testClassificationLogisticRegression(
  sparkSession: spark.sql.SparkSession
  , data: spark.sql.DataFrame
  , seed: Long
) =
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
  exportPerClassROC(sparkSession, predsForROC, workoutIndexerColumn, "build/logisticRegressionRocs") 

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

  val file = java.io.File("build/classificationLogisticRegression.txt")
  file.getParentFile.mkdirs()
  val writer = java.io.PrintWriter(java.io.FileWriter(file, true))
  writer.println(f"[LR][CV] mean-F1 (best-by-F1): $bestF1%.4f")
  writer.println(f"[LR][CV] mean-ACC (best-by-ACC): $bestACC%.4f")
  writer.close()

def testClassificationRandomForest(
  sparkSession: spark.sql.SparkSession
  , data: spark.sql.DataFrame
  , seed: Long
) =
  val positionIndexerColumn = DataColumns.Position.value + "indexer"
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
  val assembeledFeatures = "assembeledFeatures"
  val kFolds = 5

  val classifier = spark.ml.classification.RandomForestClassifier()
    .setFeaturesCol(assembeledFeatures)
    .setLabelCol(workoutIndexerColumn)
    .setNumTrees(100)
    .setSeed(seed)

  val grid = spark.ml.tuning.ParamGridBuilder()
    // .addGrid(classifier.numTrees, Array(100, 200))
    // .addGrid(classifier.maxDepth, Array(10, 15, 20))
    // .addGrid(classifier.featureSubsetStrategy, Array("sqrt", "log2"))
    .addGrid(classifier.numTrees, Array(100, 200))
    .addGrid(classifier.maxDepth, Array(10, 15))
    // .addGrid(classifier.featureSubsetStrategy, Array("sqrt", "log2"))
    .build()
  
  val estimator = spark.ml.Pipeline()
    .setStages(
      Array(
        positionStringIndexer
        , workoutStringIndexer
        , spark.ml.feature.VectorAssembler()
          .setInputCols(numericCols ++ Array(workoutIndexerColumn))
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
  exportPerClassROC(sparkSession, predsForROC, workoutIndexerColumn, "build/randomForestRocs") 

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

  val file = java.io.File("build/classificationRandomForest.txt")
  file.getParentFile.mkdirs()
  val writer = java.io.PrintWriter(java.io.FileWriter(file, true))
  writer.println(f"[RF][CV] mean-F1 (best-by-F1): $bestF1%.4f")
  writer.println(f"[RF][CV] mean-ACC (best-by-ACC): $bestACC%.4f")
  writer.close()
 
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
  testClassificationLogisticRegression(sparkSession, sample, seed)
  testClassificationRandomForest(sparkSession, sample, seed)
  sparkSession.stop()

