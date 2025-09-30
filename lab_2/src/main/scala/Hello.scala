import org.apache.{spark => spark}

object Main {
  private def timeIt[A](sparkSession: spark.sql.SparkSession, label: String)(f: => A): A = {
    val t0 = System.nanoTime()
    val res = f
    sparkSession.sparkContext.runJob(sparkSession.sparkContext.range(0, 1), (_: Iterator[Long]) => ())
    val t1 = System.nanoTime()
    println(f"[$label] took ${(t1 - t0)/1e3/1e3}%.1f ms")
    res
  }
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
      val name = "country"
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
      val name = "src"
      val index = 2
    }
    case object SourceAirportId extends RouteField {
      val sqlType = spark.sql.types.LongType
      val name = "src_id"
      val index = 3
    }
    case object DestAirport extends RouteField {
      val sqlType = spark.sql.types.StringType
      val name = "dest"
      val index = 4
    }
    case object DestAirportId extends RouteField {
      val sqlType = spark.sql.types.LongType
      val name = "dest_id"
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

  def taskTwoGraphFrames() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    timeIt(sparkSession, "taskTwoGraphFrames") {

      import sparkSession.implicits._

      val airportsV = readAirports(
        sparkSession,
        Vector(AirportField.Id, AirportField.Country)
      )
      .distinct()

      val routesE = readRoutes(
        sparkSession,
        Vector(RouteField.SourceAirportId, RouteField.DestAirportId)
      )
        .select(
          $"src_id".as("src"),
          $"dest_id".as("dst")
        )
        .distinct()

      import org.graphframes.GraphFrame

      val g = GraphFrame(airportsV, routesE)

      def onewayAnalyze(srcCountry: String, destCountry: String) = {
        val p0 = g.find(" (a)-[e1]->(b) ")
          .filter(
            s"""
              a.country = '$srcCountry' AND b.country = '$destCountry'
              AND a.id <> b.id
            """
          )
          .select(spark.sql.functions.array($"a.id", $"b.id").as("path"))

        val p1 = g.find(" (a)-[e1]->(m1); (m1)-[e2]->(b) ")
          .filter(
            s"""
              a.country = '$srcCountry' AND b.country = '$destCountry'
              AND a.id <> m1.id AND m1.id <> b.id AND a.id <> b.id
            """
          )
          .select(spark.sql.functions.array($"a.id", $"m1.id", $"b.id").as("path"))

        val p2 = g.find(" (a)-[e1]->(m1); (m1)-[e2]->(m2); (m2)-[e3]->(b) ")
          .filter(
            s"""
              a.country = '$srcCountry' AND b.country = '$destCountry'
              AND a.id <> m1.id AND a.id <> m2.id AND a.id <> b.id
              AND m1.id <> m2.id AND m1.id <> b.id
              AND m2.id <> b.id
            """)
          .select(spark.sql.functions.array($"a.id", $"m1.id", $"m2.id", $"b.id").as("path"))

        p0.withColumn("stops", spark.sql.functions.lit(0))
          .unionByName(p1.withColumn("stops", spark.sql.functions.lit(1)))
          .unionByName(p2.withColumn("stops", spark.sql.functions.lit(2)))
      }
      val srcCountry = "Poland"
      val destCountry = "France"

      val all = onewayAnalyze(srcCountry, destCountry)
        .unionByName(onewayAnalyze(destCountry, srcCountry))
        .distinct()

      all.limit(1000).collect().foreach { r =>
          val path = r.getAs[Seq[Long]]("path")
          val stops = r.getAs[Int]("stops")
          println(s"[motif][$stops stops] " + path.mkString(" -> "))
      }
      all
    }
    sparkSession.stop()
  }

  def taskTwoSql() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    timeIt(sparkSession, "taskTwoSql") {
      val airports = readAirports(
        sparkSession,
        Vector(AirportField.Id, AirportField.Country)
      )
      .distinct()

      val routes = readRoutes(
        sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
      )
      .distinct()

      import sparkSession.implicits._
      
      def onewayAnalyze(srcCountry: String, destCountry: String) = {
        val airportsSrc = airports.filter($"country" === srcCountry)
          .as("asrc")
          .cache()

        val airportsDest = airports.filter($"country" === destCountry)
          .as("adest")
          .cache()
        
        val r0 = routes.as("r0")
        val q0 = r0
          .join(airportsSrc, $"asrc.id" === $"r0.src_id")
          .join(airportsDest, $"adest.id" === $"r0.dest_id")
          .select(spark.sql.functions.array($"asrc.id", $"adest.id").as("path"))
        
        val r1 = routes.as("r1")
        val q1 = r0
          .join(r1, $"r0.dest_id" === $"r1.src_id")
          .join(airportsSrc, $"asrc.id" === $"r0.src_id")
          .join(airportsDest, $"adest.id" === $"r1.dest_id")
          .where(
            $"r0.dest_id" =!= $"asrc.id"
            && $"r0.dest_id" =!= $"adest.id" 
            && $"r1.src_id" =!= $"asrc.id"
            && $"r1.src_id" =!= $"adest.id" 
          )
          .select(spark.sql.functions.array($"r0.src_id", $"r0.dest_id", $"r1.dest_id").as("path"))

        val r2 = routes.as("r2")
        val q2 = r0
          .join(r1, $"r0.dest_id" === $"r1.src_id")
          .join(r2, $"r1.dest_id" === $"r2.src_id")
          .join(airportsSrc, $"asrc.id" === $"r0.src_id")
          .join(airportsDest, $"adest.id" === $"r2.dest_id")
          .where(
            $"r0.dest_id" =!= $"asrc.id"
            && $"r0.dest_id" =!= $"adest.id" 
            && $"r1.src_id" =!= $"asrc.id"
            && $"r1.src_id" =!= $"adest.id" 
            && $"r1.dest_id" =!= $"asrc.id"
            && $"r1.dest_id" =!= $"adest.id"
            && $"r2.src_id" =!= $"asrc.id"
            && $"r2.src_id" =!= $"adest.id" 
          )
          .select(spark.sql.functions.array($"r0.src_id", $"r0.dest_id", $"r1.dest_id", $"r2.dest_id").as("path"))

          val q0l = q0.withColumn("stops", spark.sql.functions.lit(0))
          val q1l = q1.withColumn("stops", spark.sql.functions.lit(1))
          val q2l = q2.withColumn("stops", spark.sql.functions.lit(2))
          val all = q0l.unionByName(q1l).unionByName(q2l)
          all
      }
      val srcCountry = "Poland"
      val destCountry = "France"
      val all = onewayAnalyze(srcCountry, destCountry)
        .unionByName(onewayAnalyze(destCountry, srcCountry))
        .distinct()
      all
        .limit(1000) 
        .collect()
        .foreach { r =>
          val path = r.getAs[Seq[Long]]("path"); val s = r.getAs[Int]("stops")
          println(s"[sql][$s stops] " + path.mkString(" -> "))
        }
      all
    } 
    sparkSession.stop()
  }
  def taskThree() = {
    val sparkSession = spark.sql.SparkSession.builder()
      .appName("local").getOrCreate()

    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Latitude,
        AirportField.Longitude
      )
    );
    val vertices = airportsDf.rdd.map { r =>
        (
          r.getAs[Long](AirportField.Id.name),
          (
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
        
    val edges = readRoutes(
      sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
      )
      .rdd.flatMap { r =>
        val srcId = r.getLong(1)
        val dstId = r.getLong(2)
        (airportCoordMap.get(srcId), airportCoordMap.get(dstId)) match {
          case (Some((slat, slon)), Some((dlat, dlon))) =>
            val dist = haversineKm(slat, slon, dlat, dlon)
            if (dist.isNaN || dist <= 0.0) None
            else Some(spark.graphx.Edge(srcId, dstId, dist))
          case _ => None
        }
      }
    
    val graph = spark.graphx.Graph(vertices, edges)
  }
  def taskFive() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Name,
        AirportField.Country
      )
    )
    val vertices = airportsDf.rdd.map { r =>
      (r.getLong(0), (r.getString(1), r.getString(2)))
    }

    val edges = readRoutes(
      sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
    ).rdd.map { r =>
      spark.graphx.Edge(r.getLong(0), r.getLong(1), ())
    }
    val graph = spark.graphx.Graph(vertices, edges)
    val cc = graph.connectedComponents().vertices

    val labeled = cc.join(vertices).map {
      case (id, (compId, (name, country))) =>
        (compId, (id, name, country))
    }

    val clusters = labeled.groupByKey()
      .mapValues(_.toSeq)
      .filter { case (_, members) => members.size >= 5 }

    val sortedClusters = clusters.sortByKey()

    sortedClusters.take(10).foreach { case (compId, members) =>
      println(s"Cluster $compId (size=${members.size}):")
      members.take(10).foreach { case (id, name, country) =>
        println(s"  $id $name ($country)")
      }
    }

    sparkSession.stop()
  }
  def taskSix() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    import sparkSession.implicits._

    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Name,
        AirportField.Country
      )
    )
    val vertices = airportsDf.rdd.map { r =>
      (r.getLong(0), (r.getString(1), r.getString(2)))
    }

    val edges = readRoutes(
      sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
    ).rdd.map { r =>
      spark.graphx.Edge(r.getLong(0), r.getLong(1), ())
    }
    val graph = spark.graphx.Graph(vertices, edges)
    val tol = 0.00001
    val ranks = graph.pageRank(tol).vertices

    val labeled = ranks.join(vertices)
      .map { case (id, (rank, (name, country))) => (id, name, country, rank) }

    val top10 = labeled.takeOrdered(10)(Ordering.by[(Long,String,String,Double), Double](-_._4))

    println("\n=== PageRank: Top 10 airports ===")
    top10.zipWithIndex.foreach { case ((id, name, country, rank), i) =>
      println(f"${i+1}%2d) id=$id%6d  PR=${rank}%.6f  $name ($country)")
    }

    sparkSession.stop()
  }

  def showClusters(
    airportIdWithComponentIdRdd: spark.graphx.VertexRDD[Long],
    vertices: spark.rdd.RDD[(Long, (String, String))],
    title: String,
    minSize: Int = 2,
    topN: Int = 10
  ): Unit = {

    val componentIdSizeRdd = airportIdWithComponentIdRdd.map { case (_, cid) => (cid, 1) }.reduceByKey(_ + _)
    val largestComponentId = componentIdSizeRdd.max()(Ordering.by(_._2))._1

    val filteredComponentIds = componentIdSizeRdd
      .filter { case (cid, sz) => sz >= minSize && cid != largestComponentId }
      .map(_._1)

    airportIdWithComponentIdRdd.map { case (airportId, componentId) => (componentId, airportId) }
      .join(filteredComponentIds.map((_, ())))
      .map { case (componentId, (airportId, _)) => (airportId, componentId) }
      .join(vertices)
      .map { case (airportId, (componentId, (name, country))) => (componentId, (airportId, name, country)) }
      .groupByKey()
      .mapValues(_.toArray)
      .foreach { case (componentId, info) =>
        println(s"Component $componentId (size=${info.size})")
        info
          .take(topN)
          .foreach { case (airportId, name, country) =>
          println(s"  $airportId $name ($country)")
        }
      }
  }
  def taskSevenConnectedComponents() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    import sparkSession.implicits._
    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Name,
        AirportField.Country
      )
    )
    val vertices = airportsDf.rdd.map { r =>
      (r.getLong(0), (r.getString(1), r.getString(2)))
    }

    val edges = readRoutes(
      sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
    ).rdd.map { r =>
      spark.graphx.Edge(r.getLong(0), r.getLong(1), ())
    }
    val graph = spark.graphx.Graph(vertices, edges)
    val airportIdWithComponentIdRdd = graph.connectedComponents().vertices
    showClusters(airportIdWithComponentIdRdd, vertices, "connectedComponents")
    sparkSession.stop()
  }
  def taskSevenLabelPropagation() = {
    val sparkSession = spark.sql.SparkSession.builder().appName("local").getOrCreate()
    import sparkSession.implicits._
    val airportsDf = readAirports(
      sparkSession,
      Vector(
        AirportField.Id,
        AirportField.Name,
        AirportField.Country
      )
    )
    val vertices = airportsDf.rdd.map { r =>
      (r.getLong(0), (r.getString(1), r.getString(2)))
    }
    val edges = readRoutes(
      sparkSession,
        Vector(
          RouteField.SourceAirportId,
          RouteField.DestAirportId
        )
    ).rdd.map { r =>
      spark.graphx.Edge(r.getLong(0), r.getLong(1), ())
    }
    val graph = spark.graphx.Graph(vertices, edges)
    val airportIdWithComponentIdRdd = spark.graphx.lib.LabelPropagation.run(graph, maxSteps = 100).vertices

    showClusters(airportIdWithComponentIdRdd, vertices, "LabelPropagation")
    sparkSession.stop()
  }
  def main(args: Array[String]): Unit = {
    taskSevenLabelPropagation()
    // taskSevenConnectedComponents()
  }
}
