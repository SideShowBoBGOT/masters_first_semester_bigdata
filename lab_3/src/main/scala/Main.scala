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

  val positionStringIndexer = new spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Position.value)
    .setOutputCol(positionIndexerColumn)
    .setHandleInvalid("keep")

  val workoutStringIndexer = new spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Workout.value)
    .setOutputCol(workoutIndexerColumn)
    .setHandleInvalid("keep")

  val numericCols = Array(
    DataColumns.Ax.value,
    DataColumns.Ay.value,
    DataColumns.Az.value,
    DataColumns.Gx.value,
    DataColumns.Gy.value,
    DataColumns.Gz.value,
    DataColumns.BodyCapacitance.value
  )

  val assembledNumericColumn = "assembledNumericColumns"
  val scaledNumericColumn = "scaledNumericColumn"
  val assembledFeatures = "assembledFeatures"
  val kFolds = 5

  val classifier = new spark.ml.classification.LogisticRegression()
    .setFeaturesCol(assembledFeatures)
    .setLabelCol(workoutIndexerColumn)
    .setMaxIter(100)
    .setFamily("multinomial")
    .setStandardization(false)

  val grid = new spark.ml.tuning.ParamGridBuilder()
    .addGrid(classifier.regParam, Array(0.0, 1e-1))
    .addGrid(classifier.elasticNetParam, Array(0.0, 1.0))
    .build()

  val preprocessPipeline = new spark.ml.Pipeline()
    .setStages(
      Array(
        positionStringIndexer,
        new spark.ml.feature.OneHotEncoder()
          .setInputCol(positionIndexerColumn)
          .setOutputCol(positionHotEncoderColumn)
          .setHandleInvalid("keep"),
        workoutStringIndexer,
        new spark.ml.feature.VectorAssembler()
          .setInputCols(numericCols)
          .setOutputCol(assembledNumericColumn),
        new spark.ml.feature.StandardScaler()
          .setInputCol(assembledNumericColumn)
          .setOutputCol(scaledNumericColumn),
        new spark.ml.feature.VectorAssembler()
          .setInputCols(Array(scaledNumericColumn, positionHotEncoderColumn))
          .setOutputCol(assembledFeatures)
      )
    )

  val preprocessModel = preprocessPipeline.fit(data)
  val preprocessedData = preprocessModel.transform(data)

  val f1Eval = new spark.ml.evaluation.MulticlassClassificationEvaluator()
    .setLabelCol(workoutIndexerColumn)
    .setPredictionCol("prediction")
    .setMetricName("f1")

  val cvF1 = new spark.ml.tuning.CrossValidator()
    .setEstimator(classifier)
    .setEvaluator(f1Eval)
    .setEstimatorParamMaps(grid)
    .setNumFolds(kFolds)
    .setSeed(seed)

  val Array(trainData, testData) = preprocessedData.randomSplit(Array(0.7, 0.3), seed = seed)
  val cvModelF1 = cvF1.fit(trainData)
  val bestF1 = cvModelF1.avgMetrics.max
  val predsForROC = cvModelF1.bestModel.transform(testData)

  exportPerClassROC(sparkSession, predsForROC, workoutIndexerColumn, "build/logisticRegressionRocs") 

  val accEval = new spark.ml.evaluation.MulticlassClassificationEvaluator()
    .setLabelCol(workoutIndexerColumn)
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val cvACC = new spark.ml.tuning.CrossValidator()
    .setEstimator(classifier)
    .setEvaluator(accEval)
    .setEstimatorParamMaps(grid)
    .setNumFolds(kFolds)
    .setSeed(seed)

  val cvModelACC = cvACC.fit(trainData)

  val bestACC = cvModelACC.avgMetrics.max

  val file = new java.io.File("build/classificationLogisticRegression.txt")
  file.getParentFile.mkdirs()
  val writer = new java.io.PrintWriter(new java.io.FileWriter(file, true))
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
  val positionStringIndexer = new spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Position.value)
    .setOutputCol(positionIndexerColumn)
    .setHandleInvalid("keep")
  val workoutStringIndexer = new spark.ml.feature.StringIndexer()
    .setInputCol(DataColumns.Workout.value)
    .setOutputCol(workoutIndexerColumn)
    .setHandleInvalid("keep")
  val numericCols = Array(
    DataColumns.Ax.value,
    DataColumns.Ay.value,
    DataColumns.Az.value,
    DataColumns.Gx.value,
    DataColumns.Gy.value,
    DataColumns.Gz.value,
    DataColumns.BodyCapacitance.value
  )
  val assembledFeatures = "assembledFeatures"
  val kFolds = 5

  val classifier = new spark.ml.classification.RandomForestClassifier()
    .setFeaturesCol(assembledFeatures)
    .setLabelCol(workoutIndexerColumn)
    .setNumTrees(100)
    .setSeed(seed)

  val grid = new spark.ml.tuning.ParamGridBuilder()
    .addGrid(classifier.numTrees, Array(40, 70, 100, 130))
    .build()

  val preprocessPipeline = new spark.ml.Pipeline()
    .setStages(
      Array(
        positionStringIndexer,
        workoutStringIndexer,
        new spark.ml.feature.VectorAssembler()
          .setInputCols(numericCols ++ Array(workoutIndexerColumn))
          .setOutputCol(assembledFeatures)
      )
    )

  val preprocessModel = preprocessPipeline.fit(data)
  val preprocessedData = preprocessModel.transform(data)

  val f1Eval = new spark.ml.evaluation.MulticlassClassificationEvaluator()
    .setLabelCol(workoutIndexerColumn)
    .setPredictionCol("prediction")
    .setMetricName("f1")

  val cvF1 = new spark.ml.tuning.CrossValidator()
    .setEstimator(classifier)
    .setEvaluator(f1Eval)
    .setEstimatorParamMaps(grid)
    .setNumFolds(kFolds)
    .setSeed(seed)

  val Array(trainData, testData) = preprocessedData.randomSplit(Array(0.7, 0.3), seed = seed)
  val cvModelF1 = cvF1.fit(trainData)
  val bestF1 = cvModelF1.avgMetrics.max
  val predsForROC = cvModelF1.bestModel.transform(testData)

  exportPerClassROC(sparkSession, predsForROC, workoutIndexerColumn, "build/randomForestRocs") 

  val accEval = new spark.ml.evaluation.MulticlassClassificationEvaluator()
    .setLabelCol(workoutIndexerColumn)
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val cvACC = new spark.ml.tuning.CrossValidator()
    .setEstimator(classifier)
    .setEvaluator(accEval)
    .setEstimatorParamMaps(grid)
    .setNumFolds(kFolds)
    .setSeed(seed)

  val cvModelACC = cvACC.fit(trainData)

  val bestACC = cvModelACC.avgMetrics.max

  val file = new java.io.File("build/classificationRandomForest.txt")
  file.getParentFile.mkdirs()
  val writer = new java.io.PrintWriter(new java.io.FileWriter(file, true))
  writer.println(f"[RF][CV] mean-F1 (best-by-F1): $bestF1%.4f")
  writer.println(f"[RF][CV] mean-ACC (best-by-ACC): $bestACC%.4f")
  writer.close()

def testClusterizationKmeans(
  sparkSession: spark.sql.SparkSession,
  data: spark.sql.DataFrame,
  seed: Long
) = {
  val assembler = new spark.ml.feature.VectorAssembler()
    .setInputCols(
      Array(
        DataColumns.Gx.value,
        DataColumns.Gy.value,
        DataColumns.Gz.value
      )
    )
    .setOutputCol("features")

  val assembledData = assembler.transform(data)
  val Array(trainData, testData) = assembledData.randomSplit(Array(0.7, 0.3), seed = seed)

  (2 until 12).foreach { k =>
    val kMeans = new spark.ml.clustering.KMeans().setK(k).setSeed(seed)
    val kMeansModel = kMeans.fit(trainData)
    val predictions = kMeansModel.transform(testData)
    val evaluator = new spark.ml.evaluation.ClusteringEvaluator()
    val score = evaluator.evaluate(predictions)
    val file = new java.io.File(s"build/clusterizationKmeans/$k.txt")
    file.getParentFile.mkdirs()
    val writer = new java.io.PrintWriter(new java.io.FileWriter(file, true))
    writer.println(f"score=$score")
    writer.close()
  }
}

def testClusterizationGaussianMixture(
  sparkSession: spark.sql.SparkSession,
  data: spark.sql.DataFrame,
  seed: Long
) = {
  val assembler = new spark.ml.feature.VectorAssembler()
    .setInputCols(
      Array(
        DataColumns.Gx.value,
        DataColumns.Gy.value,
        DataColumns.Gz.value
      )
    )
    .setOutputCol("features")

  val assembledData = assembler.transform(data)
  val Array(trainData, testData) = assembledData.randomSplit(Array(0.7, 0.3), seed = seed)

  (2 until 12).foreach { k =>
    val clusterizationAlg = new spark.ml.clustering.GaussianMixture().setK(k).setSeed(seed)
    val model = clusterizationAlg.fit(trainData)
    val predictions = model.transform(testData)
    val evaluator = new spark.ml.evaluation.ClusteringEvaluator()
    val score = evaluator.evaluate(predictions)
    val file = new java.io.File(s"build/clusterizationGaussianMixture/$k.txt")
    file.getParentFile.mkdirs()
    val writer = new java.io.PrintWriter(new java.io.FileWriter(file, true))
    writer.println(f"score=$score")
    writer.close()
  }
}


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
  // testClassificationLogisticRegression(sparkSession, sample, seed)
  testClassificationRandomForest(sparkSession, sample, seed)
  // testClusterizationKmeans(sparkSession, sample, seed)
  // testClusterizationGaussianMixture(sparkSession, sample, seed)
  sparkSession.stop()

