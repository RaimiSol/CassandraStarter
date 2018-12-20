name := "CassandraScalaSparkStarter"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "edu.ucar" % "netcdf" % "4.2.20",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.0"
)