import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Main {
  case class EdgeAttr(airlineId: Long, distanceKm: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AirlinesDistanceGraphX")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
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

    val airportsDF = spark.read.options(csvOpts).csv(airportsPath)
      .select(
        F.col("_c0").as("airport_id"),
        F.col("_c1").as("airport_name"),
        F.col("_c6").as("lat"),
        F.col("_c7").as("lon")
      )
      .filter(F.col("airport_id").isNotNull && F.col("lat").isNotNull && F.col("lon").isNotNull)
      .filter(!F.col("airport_id").equalTo("\\N") && !F.col("lat").equalTo("\\N") && !F.col("lon").equalTo("\\N"))
      .withColumn("airport_id_l", F.col("airport_id").cast(LongType))
      .withColumn("lat_d", F.col("lat").cast(DoubleType))
      .withColumn("lon_d", F.col("lon").cast(DoubleType))
      .select("airport_id_l", "airport_name", "lat_d", "lon_d")
      .cache()

    val vertices: RDD[(VertexId, (String, Double, Double))] =
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
    val bAirportCoords = spark.sparkContext.broadcast(airportCoordMap)
    val airlinesMap: Map[Long, String] = spark.read.options(csvOpts).csv(airlinesPath)
      .select(F.col("_c0").as("airline_id"), F.col("_c1").as("airline_name"))
      .filter(F.col("airline_id").isNotNull && !F.col("airline_id").equalTo("\\N"))
      .withColumn("airline_id_l", F.col("airline_id").cast(LongType))
      .select("airline_id_l", "airline_name")
      .rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .collect()
      .toMap
    val bAirlines = spark.sparkContext.broadcast(airlinesMap)

    val routesDF = spark.read.options(csvOpts).csv(routesPath)
      .select(F.col("_c1").as("airline_id"),
              F.col("_c3").as("src_id"),
              F.col("_c5").as("dst_id"))
      .filter(F.col("airline_id").isNotNull && F.col("src_id").isNotNull && F.col("dst_id").isNotNull)
      .filter(!F.col("airline_id").equalTo("\\N") && !F.col("src_id").equalTo("\\N") && !F.col("dst_id").equalTo("\\N"))
      .withColumn("airline_id_l", F.col("airline_id").cast(LongType))
      .withColumn("src_id_l", F.col("src_id").cast(LongType))
      .withColumn("dst_id_l", F.col("dst_id").cast(LongType))
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

    val edges: RDD[Edge[EdgeAttr]] = routesDF.rdd.flatMap { r =>
      val airlineId = r.getLong(0)
      val srcId     = r.getLong(1)
      val dstId     = r.getLong(2)
      val coords    = bAirportCoords.value
      (coords.get(srcId), coords.get(dstId)) match {
        case (Some((slat, slon)), Some((dlat, dlon))) =>
          val dist = haversineKm(slat, slon, dlat, dlon)
                    if (dist.isNaN || dist <= 0.0) None
          else Some(Edge(srcId, dstId, EdgeAttr(airlineId, dist)))
        case _ => None
      }
    }

    val graph: Graph[(String, Double, Double), EdgeAttr] = Graph(vertices, edges)

    val totalsByAirline: RDD[(Long, Double)] =
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

    spark.stop()
  }
}

