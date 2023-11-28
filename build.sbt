ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "VinhLearningDE",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.databricks" %% "dbutils-api" % "0.0.6" % Provided,
    )
  )
