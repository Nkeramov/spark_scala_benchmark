version := "1.0.0"

javacOptions ++= Seq("-source", "11")
javacOptions ++= Seq("-target", "11")

scalaVersion := "2.12.17"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

lazy val root = (project in file("."))
  .settings(
    name := "spark_scala_benchmark"
  )