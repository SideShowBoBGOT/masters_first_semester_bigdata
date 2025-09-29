name := "lab_2"
organization := "org.panchenko"
version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"
lazy val sparkV = "3.5.3"

lazy val macroParadise = compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

lazy val macros = (project in file("macros")).settings(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    macroParadise
  )
)

lazy val root = (project in file("."))
  .aggregate(macros)
  .dependsOn(macros)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"   % sparkV,      
      "org.apache.spark" %% "spark-sql"    % sparkV,
      "org.apache.spark" %% "spark-graphx" % sparkV,
      macroParadise
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Dspark.master=local[*]",
      "-Xms1g","-Xmx1g","-XX:+UseG1GC","-XX:+UseStringDeduplication"
    ),
    Compile / mainClass := Some("Main")  
  )
