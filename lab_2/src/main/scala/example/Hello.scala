import org.apache.{spark => spark}

object Main {
  case class EdgeAttr(airlineId: Long, distanceKm: Double)

  def main(args: Array[String]): Unit = {
    val sparkSession = spark.sql.SparkSession.builder()
      .appName("AirlinesDistanceGraphX")
      .getOrCreate()

    val airportsPath = "sampledata/OpenFlights.org/airports-extended.dat"
    val airlinesPath = "sampledata/OpenFlights.org/airlines.dat"
    val routesPath   = "sampledata/OpenFlights.org/routes.dat"
    
    val csvOpts = Map(
      "header" -> "false",
      "inferSchema" -> "false",
      "quote" -> "\"",
      "escape" -> "\"",
      "mode" -> "PERMISSIVE",
      "multiLine" -> "false",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace" -> "true"
    )
    val airportsDF = sparkSession.read.options(csvOpts).csv(airportsPath)
      .select(
        spark.sql.functions.col("_c0").as("airport_id"),
        spark.sql.functions.col("_c1").as("airport_name"),
        spark.sql.functions.col("_c6").as("lat"),
        spark.sql.functions.col("_c7").as("lon")
      )
      .filter(spark.sql.functions.col("airport_id").isNotNull && spark.sql.functions.col("lat").isNotNull && spark.sql.functions.col("lon").isNotNull)
      .filter(!spark.sql.functions.col("airport_id").equalTo("\\N") && !spark.sql.functions.col("lat").equalTo("\\N") && !spark.sql.functions.col("lon").equalTo("\\N"))
      .withColumn("airport_id_l", spark.sql.functions.col("airport_id").cast(spark.sql.types.LongType))
      .withColumn("lat_d", spark.sql.functions.col("lat").cast(spark.sql.types.DoubleType))
      .withColumn("lon_d", spark.sql.functions.col("lon").cast(spark.sql.types.DoubleType))
      .select("airport_id_l", "airport_name", "lat_d", "lon_d")
      .cache()
    
    val vertices: spark.rdd.RDD[(spark.graphx.VertexId, (String, Double, Double))] =
      airportsDF.rdd.map { r =>
        val id  = r.getAs[Long]("airport_id_l")
        val nm  = r.getAs[String]("airport_name")
        val lat = r.getAs[Double]("lat_d")
        val lon = r.getAs[Double]("lon_d")
        (id, (nm, lat, lon))
      }
    
    val airportCoordMap = airportsDF
      .select("airport_id_l", "lat_d", "lon_d")
      .rdd
      .map(r => (r.getLong(0), (r.getDouble(1), r.getDouble(2))))
      .collectAsMap()
    val bAirportCoords = sparkSession.sparkContext.broadcast(airportCoordMap)
   
    val airlinesMap: Map[Long, String] = sparkSession.read.options(csvOpts).csv(airlinesPath)
      .select(spark.sql.functions.col("_c0").as("airline_id"), spark.sql.functions.col("_c1").as("airline_name"))
      .filter(spark.sql.functions.col("airline_id").isNotNull && !spark.sql.functions.col("airline_id").equalTo("\\N"))
      .withColumn("airline_id_l", spark.sql.functions.col("airline_id").cast(spark.sql.types.LongType))
      .select("airline_id_l", "airline_name")
      .rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .collect()
      .toMap
    val bAirlines = sparkSession.sparkContext.broadcast(airlinesMap)

    val routesDF = sparkSession.read.options(csvOpts).csv(routesPath)
      .select(spark.sql.functions.col("_c1").as("airline_id"),
              spark.sql.functions.col("_c3").as("src_id"),
              spark.sql.functions.col("_c5").as("dst_id"))
      .filter(spark.sql.functions.col("airline_id").isNotNull && spark.sql.functions.col("src_id").isNotNull && spark.sql.functions.col("dst_id").isNotNull)
      .filter(!spark.sql.functions.col("airline_id").equalTo("\\N") && !spark.sql.functions.col("src_id").equalTo("\\N") && !spark.sql.functions.col("dst_id").equalTo("\\N"))
      .withColumn("airline_id_l", spark.sql.functions.col("airline_id").cast(spark.sql.types.LongType))
      .withColumn("src_id_l", spark.sql.functions.col("src_id").cast(spark.sql.types.LongType))
      .withColumn("dst_id_l", spark.sql.functions.col("dst_id").cast(spark.sql.types.LongType))
      .select("airline_id_l", "src_id_l", "dst_id_l")
    
    def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val R = 6371.0088 
      val dLat = Math.toRadians(lat2 - lat1)
      val dLon = Math.toRadians(lon2 - lon1)
      val a = Math.pow(Math.sin(dLat / 2), 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
          Math.pow(Math.sin(dLon / 2), 2)
      val c = 2 * Math.asin(Math.min(1.0, Math.sqrt(a)))
      R * c
    }
    val edges = routesDF.rdd.flatMap { r =>
      val airlineId = r.getLong(0)
      val srcId     = r.getLong(1)
      val dstId     = r.getLong(2)
      val coords    = bAirportCoords.value
      (coords.get(srcId), coords.get(dstId)) match {
        case (Some((slat, slon)), Some((dlat, dlon))) =>
          val dist = haversineKm(slat, slon, dlat, dlon)
          
          if (dist.isNaN || dist <= 0.0) None
          else Some(spark.graphx.Edge(srcId, dstId, EdgeAttr(airlineId, dist)))
        case _ => None
      }
    }
    
    val graph = spark.graphx.Graph(vertices, edges)
    val totalsByAirline: spark.rdd.RDD[(Long, Double)] =
      graph.edges.map(e => (e.attr.airlineId, e.attr.distanceKm))
        .reduceByKey(_ + _)
        .cache()
    
    val topMax = totalsByAirline.takeOrdered(1)(Ordering.by[(Long, Double), Double](-_._2)).headOption
    val topMin = totalsByAirline.takeOrdered(1)(Ordering.by[(Long, Double), Double](_._2)).headOption

    def nameOf(id: Long): String = bAirlines.value.getOrElse(id, s"Airline#$id")

    topMax.foreach { case (id, sumKm) =>
      println(f"MAX total distance: ${nameOf(id)} (airline_id=$id) -> ${sumKm}%.2f km")
    }
    topMin.foreach { case (id, sumKm) =>
      println(f"MIN total distance: ${nameOf(id)} (airline_id=$id) -> ${sumKm}%.2f km")
    }

    sparkSession.stop()
  }
}

