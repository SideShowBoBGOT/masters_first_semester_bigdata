name := "lab_4"
organization := "org.panchenko"
version := "0.1.0"
ThisBuild / scalaVersion := "3.3.4"
val sparkV = "4.0.1"
lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % sparkV % "provided").cross(CrossVersion.for3Use2_13)
      , ("org.apache.spark" %% "spark-sql" % sparkV % "provided").cross(CrossVersion.for3Use2_13)
      , ("org.apache.spark" %% "spark-mllib" % sparkV % "provided").cross(CrossVersion.for3Use2_13)
      , ("org.knowm.xchart" % "xchart" % "3.8.8")
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Dspark.master=local[*]",
      "-Xms1g",
      "-Xmx1g",
      "-XX:+UseG1GC",
      "-XX:+UseStringDeduplication"
    ),
    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated 
  )
