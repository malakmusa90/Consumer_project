ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkConsumer",

    resolvers ++= Seq(
      "John Snow Labs Repository" at "https://repo.johnsnowlabs.com/releases"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.3",
      "com.typesafe.play" %% "play-json" % "2.10.0-RC7",
      "com.clearspring.analytics" % "stream" % "2.9.6",
      "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
    )
  )
