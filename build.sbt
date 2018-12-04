name := "8INF803_TP2"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-graphx_2.11" % "2.2.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.2"
)