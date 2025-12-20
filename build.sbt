ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkConsumer",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "com.typesafe.play" %% "play-json" % "2.10.0-RC7",
      "com.clearspring.analytics" % "stream" % "2.9.6",
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
      "com.twitter" %% "algebird-core" % "0.13.10"
    )
  )
