ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "lab_2",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.3" % "provided",
      "org.apache.spark" %% "spark-graphx" % "3.5.3" % "provided"
    )
  )

