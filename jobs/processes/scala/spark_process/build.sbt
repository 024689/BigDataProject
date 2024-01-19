ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark_job"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0"
)

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-30" % "7.17.16" % "provided"
libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.19"

