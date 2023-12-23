ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-anonymization-toolkit"
  )

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.16.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
