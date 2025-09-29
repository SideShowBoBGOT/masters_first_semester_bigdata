name := "lab_2"
organization := "org.panchenko"
version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"
lazy val sparkV = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"   % sparkV,
      "org.apache.spark" %% "spark-sql"    % sparkV,
      "org.apache.spark" %% "spark-graphx" % sparkV,
      "io.graphframes"   % "graphframes-spark3_2.12" % "0.9.3"
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Dspark.master=local[*]",
      "-Xms1g","-Xmx1g","-XX:+UseG1GC","-XX:+UseStringDeduplication"
    ),
    Compile / mainClass := Some("Main")
  )

