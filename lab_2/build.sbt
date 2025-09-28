ThisBuild / scalaVersion := "2.12.18"
lazy val sparkV = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "lab_2",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"   % sparkV,      // no "provided"
      "org.apache.spark" %% "spark-sql"    % sparkV,
      "org.apache.spark" %% "spark-graphx" % sparkV
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Dspark.master=local[*]",
      "-Xms1g","-Xmx1g","-XX:+UseG1GC","-XX:+UseStringDeduplication"
    ),
    Compile / mainClass := Some("Main")  // if your object is `Main`
  )

