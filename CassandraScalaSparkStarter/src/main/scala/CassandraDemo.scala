import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, CassandraTableScanRDD}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object CassandraDemo {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("CassandraDemo")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for CassandraDemo")
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val data = getRealWorldData()
    val listRDD = data._1
    val listDF = data._2
    //println(listDF.show())
    //writeToCassandra(listRDD)
    val readData = readFromCassandra()
    val readRDD = readData._1
    val readDF = readData._2


    //val readJoinRDD = readJoinFromCassandra

    //println(readJoinRDD.columnNames)
    //readJoinRDD.where("institution='IN' AND date>'2010120000000000' AND date < '20110000000000'")
    //  .limit(1).foreach(println)
    //readJoinRDD.on(SomeColumns("institution")).select("institution,date").collect.foreach(println)
  }

  def writeToCassandra(listRDD: RDD[Row]):Unit={
    listRDD.saveToCassandra("test", "cassandrademo", SomeColumns("file", "date","latitude","longitude","ocean","profiler_type", "institution","date_update"))
  }

  def readFromCassandra():(CassandraTableScanRDD[CassandraRow], DataFrame)={
    val readRDD = sc.cassandraTable("test", "cassandrademo")
    val readDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "cassandrademo", "keyspace" -> "test"))
      .load()
    (readRDD,readDF)
  }
  def readJoinFromCassandra():CassandraJoinRDD[CassandraRow, CassandraRow]={
    val readRDD = sc.cassandraTable("test", "cassandrademo")
      .joinWithCassandraTable("test","institutioninfo")
      .on(SomeColumns("institution"))
    readRDD
  }

  def getRealWorldData(): (RDD[Row],DataFrame ) ={
    val fetchedTXT = sc.
      wholeTextFiles("ftp://anonymous:test@ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt")
    val fullString = fetchedTXT.take(1)(0)._2
    val rowArray = fullString.split("\n")

    // EXTRACT SCHEMA
    val schemaString = rowArray.zipWithIndex.view.filter(row => row._1.charAt(0) != '#').head
    val schemaArray = schemaString._1.split(",")
    val schema = StructType( schemaArray.map(fieldName => StructField(fieldName, StringType, true)))

    val fullArray = rowArray.drop(schemaString._2+1).map(row=>row.split(","))

    val listRDD = sc.parallelize(fullArray).map(row => Row.fromSeq(row))
    val listDF = spark.sqlContext.createDataFrame(listRDD, schema)

    (listRDD, listDF)
  }
}
