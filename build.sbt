name := "stomac"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "joda-time" % "joda-time" % "2.8.1",
  "org.mockito" % "mockito-all" % "1.9.5",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.2.2",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)
