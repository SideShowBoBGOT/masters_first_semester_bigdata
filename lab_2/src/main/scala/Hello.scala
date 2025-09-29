import org.apache.{spark => spark}

object Main {
  case class EdgeAttr(airlineId: Long, distanceKm: Double)
  val sampledataOpenFlightsOrg = "sampledata/OpenFlights.org/";
  val csvOpts = Map(
    "header" -> "false",
    "inferSchema" -> "false",
    "quote" -> "\"",
    "escape" -> "\"",
    "mode" -> "PERMISSIVE",
    "multiLine" -> "false",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true"
  );
  sealed trait SqlField {
    def sqlType: spark.sql.types.DataType
    def name: String
  }
  sealed trait AirportField extends SqlField
  object AirportField {
    case object Id extends AirportField {
      val sqlType = spark.sql.types.LongType
      val name = "id"
    }
    case object Name extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "name"
    }
    case object City extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "city"
    }
    case object Country extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "city"
    }
    case object IATA extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "iata"
    }
    case object ICAO extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "icao"
    }
    case object Latitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "latitude"
    }
    case object Longitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "longitude"
    }
    case object Altitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "altitude"
    }
    case object TimezoneOffset extends AirportField {
      val sqlType = spark.sql.types.IntegerType
      val name = "timezone_offset"
    }
    case object DaylightSavingTime extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "daylight_saving_time"
    }
  }
  def readAirports(sparkSession: spark.sql.SparkSession) = {
    
    sparkSession.read.options(csvOpts).csv(sampledataOpenFlightsOrg + "airports-extended.dat")
      spark.sql.functions.col
      .select(
        spark.sql.functions.col("_c0").as("id"),
        spark.sql.functions.col("_c1").as("name"),
        spark.sql.functions.col("_c2").as("city"),
        spark.sql.functions.col("_c3").as("country"),
        spark.sql.functions.col("_c6").as("lat"),
        spark.sql.functions.col("_c7").as("lon"),
        // spark.sql.functions.col("_c11").as("type"),
      )
      .filter(
        spark.sql.functions.col("id").isNotNull
        && spark.sql.functions.col("name").isNotNull
        && spark.sql.functions.col("lat").isNotNull
        && spark.sql.functions.col("lon").isNotNull
      )
      .filter(
        !spark.sql.functions.col("id").equalTo("\\N")
        && !spark.sql.functions.col("name").equalTo("\\N")
        && !spark.sql.functions.col("lat").equalTo("\\N")
        && !spark.sql.functions.col("lon").equalTo("\\N")
      )
      .withColumn("id_l", spark.sql.functions.col("id").cast(spark.sql.types.LongType))
      .withColumn("lat_d", spark.sql.functions.col("lat").cast(spark.sql.types.DoubleType))
      .withColumn("lon_d", spark.sql.functions.col("lon").cast(spark.sql.types.DoubleType))
      .select("id_l", "name", "lat_d", "lon_d")
  }
  def readAirlines(sparkSession: spark.sql.SparkSession) = {
    sparkSession.read.options(csvOpts).csv(sampledataOpenFlightsOrg + "airlines.dat")
      .select(
        spark.sql.functions.col("_c0").as("id"),
        spark.sql.functions.col("_c1").as("name")
      )
      .filter(
        spark.sql.functions.col("id").isNotNull
        && spark.sql.functions.col("name").isNotNull
      )
      .filter(
        !spark.sql.functions.col("id").equalTo("\\N")
        && !spark.sql.functions.col("name").equalTo("\\N")
      )
      .withColumn("id_l", spark.sql.functions.col("id").cast(spark.sql.types.LongType))
      .select("id_l", "name")
  }
  def readRoutes(sparkSession: spark.sql.SparkSession) = {
    sparkSession.read.options(csvOpts).csv(sampledataOpenFlightsOrg + "routes.dat")
      .select(
        spark.sql.functions.col("_c1").as("airline_id"),
        spark.sql.functions.col("_c3").as("src_id"),
        spark.sql.functions.col("_c5").as("dst_id"),
        spark.sql.functions.col("_c7").as("stops")
      )
      .filter(
        spark.sql.functions.col("airline_id").isNotNull
        && spark.sql.functions.col("src_id").isNotNull
        && spark.sql.functions.col("dst_id").isNotNull
        && spark.sql.functions.col("stops").isNotNull
      )
      .filter(
        !spark.sql.functions.col("airline_id").equalTo("\\N")
        && !spark.sql.functions.col("src_id").equalTo("\\N")
        && !spark.sql.functions.col("dst_id").equalTo("\\N")
        && !spark.sql.functions.col("stops").equalTo("\\N")
      )
      .withColumn("airline_id_l", spark.sql.functions.col("airline_id").cast(spark.sql.types.LongType))
      .withColumn("src_id_l", spark.sql.functions.col("src_id").cast(spark.sql.types.LongType))
      .withColumn("dst_id_l", spark.sql.functions.col("dst_id").cast(spark.sql.types.LongType))
      .withColumn("stops_l", spark.sql.functions.col("stops").cast(spark.sql.types.LongType))
      .select("airline_id_l", "src_id_l", "dst_id_l", "stops_l")
  }
  def taskOne(): Unit = {
    val sparkSession = spark.sql.SparkSession.builder()
      .appName("local").getOrCreate()

    val airportsDf = readAirports(sparkSession);
    val vertices = airportsDf.rdd.map { r =>
        val id  = r.getAs[Long]("id_l")
        val nm  = r.getAs[String]("name")
        val lat = r.getAs[Double]("lat_d")
        val lon = r.getAs[Double]("lon_d")
        (id, (nm, lat, lon))
      }
    
    val airportCoordMap = airportsDf
      .select("id_l", "lat_d", "lon_d")
      .rdd
      .map(r => (r.getLong(0), (r.getDouble(1), r.getDouble(2))))
      .collectAsMap();
        
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

    val edges = readRoutes(sparkSession).rdd.flatMap { r =>
      val airlineId = r.getLong(0)
      val srcId = r.getLong(1)
      val dstId = r.getLong(2)
      (airportCoordMap.get(srcId), airportCoordMap.get(dstId)) match {
        case (Some((slat, slon)), Some((dlat, dlon))) =>
          val dist = haversineKm(slat, slon, dlat, dlon)
          if (dist.isNaN || dist <= 0.0) None
          else Some(spark.graphx.Edge(srcId, dstId, EdgeAttr(airlineId, dist)))
        case _ => None
      }
    }
    
    val graph = spark.graphx.Graph(vertices, edges)
    val totalsByAirline = graph.edges.map(e => (e.attr.airlineId, e.attr.distanceKm))
        .reduceByKey(_ + _).cache()
    
    val topMax = totalsByAirline.takeOrdered(1)(Ordering.by[(Long, Double), Double](-_._2)).headOption
    val topMin = totalsByAirline.takeOrdered(1)(Ordering.by[(Long, Double), Double](_._2)).headOption

    val airlines = readAirlines(sparkSession).rdd.map { r =>
      (r.getLong(0), r.getString(1))
    }.collect().toMap

    def nameOf(id: Long): String = airlines.getOrElse(id, s"Airline#$id")

    topMax.foreach { case (id, sumKm) =>
      println(f"MAX total distance: ${nameOf(id)} (airline_id=$id) -> ${sumKm}%.2f km")
    }
    topMin.foreach { case (id, sumKm) =>
      println(f"MIN total distance: ${nameOf(id)} (airline_id=$id) -> ${sumKm}%.2f km")
    }

    sparkSession.stop()
  }

  def taskTwoGraphX() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    readRoutes(sparkSession).rdd.map { r =>
      r.getAs[Long]("src_id_l") 
    } 

    sparkSession.stop()
  }

  def main(args: Array[String]): Unit = {
    taskOne();
  }
}

