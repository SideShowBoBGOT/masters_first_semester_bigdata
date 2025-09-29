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
    def index: Int
  }
  sealed trait AirportField extends SqlField
  object AirportField {
    case object Id extends AirportField {
      val sqlType = spark.sql.types.LongType
      val name = "id"
      val index = 0
    }
    case object Name extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "name"
      val index = 1
    }
    case object City extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "city"
      val index = 2
    }
    case object Country extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "city"
      val index = 3
    }
    case object IATA extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "iata"
      val index = 4
    }
    case object ICAO extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "icao"
      val index = 5
    }
    case object Latitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "latitude"
      val index = 6
    }
    case object Longitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "longitude"
      val index = 7
    }
    case object Altitude extends AirportField {
      val sqlType = spark.sql.types.DoubleType
      val name = "altitude"
      val index = 8
    }
    case object TimezoneOffset extends AirportField {
      val sqlType = spark.sql.types.IntegerType
      val name = "timezone_offset"
      val index = 9
    }
    case object DaylightSavingTime extends AirportField {
      val sqlType = spark.sql.types.StringType
      val name = "daylight_saving_time"
      val index = 10
    }
  }
  sealed trait AirlineField extends SqlField
  object AirlineField {
    case object Id extends AirlineField {
      val sqlType = spark.sql.types.LongType
      val name = "id"
      val index = 0
    }
    case object Name extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "name"
      val index = 1
    }
    case object Alias extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "alias"
      val index = 2
    }
    case object IATA extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "iata"
      val index = 3
    }
    case object ICAO extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "icao"
      val index = 4
    }
    case object Callsign extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "callsign"
      val index = 5
    }
    case object Country extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "country"
      val index = 6
    }
    case object Active extends AirlineField {
      val sqlType = spark.sql.types.StringType
      val name = "active"
      val index = 7
    }
  }
  sealed trait RouteField extends SqlField
  object RouteField {
    case object Airline extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "airline"
      val index = 0
    }
    case object AirlineId extends RouteField {
      val sqlType = spark.sql.types.LongType
      val name = "airline_id"
      val index = 1
    }
    case object SourceAirport extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "src_airport"
      val index = 2
    }
    case object SourceAirportId extends RouteField {
      val sqlType = spark.sql.types.LongType
      val name = "src_airport_id"
      val index = 3
    }
    case object DestAirport extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "dst_airport"
      val index = 4
    }
    case object DestAirportId extends RouteField {
      val sqlType = spark.sql.types.LongType
      val name = "dst_airport_id"
      val index = 5
    }
    case object Codeshare extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "codeshare"
      val index = 6
    }
    case object Stops extends RouteField {
      val sqlType = spark.sql.types.IntegerType
      val name = "stops"
      val index = 7
    }
    case object Equipment extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "equipment"
      val index = 8
    }
  }
  def readData(sparkSession: spark.sql.SparkSession, name: String, fields: Vector[SqlField]) = {
    val columns = fields.map { field =>
      spark.sql.functions.col(s"_c${field.index}").as(field.name)
    }
    val baseDf = sparkSession.read.options(csvOpts).csv(sampledataOpenFlightsOrg + name)
      .select(columns: _*)
      .filter(columns.map { col => col.isNotNull }.reduce(_ && _))
      .filter(columns.map { col => !col.equalTo("\\N") }.reduce(_ && _))

    fields.foldLeft(baseDf) { (df, field) =>
      df.withColumn(field.name, spark.sql.functions.col(field.name).cast(field.sqlType)) 
    }
    .select(fields.map(f => spark.sql.functions.col(f.name)): _*)
  }
  def readAirports(sparkSession: spark.sql.SparkSession, fields: Vector[AirportField]) = {
    readData(sparkSession, "airports-extended.dat", fields)
  }
  def readAirlines(sparkSession: spark.sql.SparkSession, fields: Vector[AirlineField]) = {
    readData(sparkSession, "airlines.dat", fields)
  }
  def readRoutes(sparkSession: spark.sql.SparkSession, fields: Vector[RouteField]) = {
    readData(sparkSession, "routes.dat", fields)
  }
  def taskOne(): Unit = {
    val sparkSession = spark.sql.SparkSession.builder()
      .appName("local").getOrCreate()

    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Name,
        AirportField.Latitude,
        AirportField.Longitude
      )
    );
    val vertices = airportsDf.rdd.map { r =>
        (
          r.getAs[Long](AirportField.Id.name),
          (
            r.getAs[String](AirportField.Name.name),
            r.getAs[Double](AirportField.Latitude.name),
            r.getAs[Double](AirportField.Longitude.name)
          )
        )
      }
    
    val airportCoordMap = airportsDf.rdd.map{ r =>
      (
        r.getAs[Long](AirportField.Id.name),
        (
          r.getAs[Double](AirportField.Latitude.name),
          r.getAs[Double](AirportField.Longitude.name)
        )
      )
    }
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

    val edges = readRoutes(
      sparkSession,
      Vector(
        RouteField.AirlineId,
        RouteField.SourceAirportId,
        RouteField.DestAirportId)
      )
      .rdd.flatMap { r =>
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

    val airlines = readAirlines(sparkSession, Vector(AirlineField.Id, AirlineField.Name)).rdd.map { r =>
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

    sparkSession.stop()
  }

  def main(args: Array[String]): Unit = {
    taskOne();
  }
}
