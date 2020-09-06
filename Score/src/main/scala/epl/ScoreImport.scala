//package epl

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.ConstantInputDStream

object epl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Score")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val epldataallcols = sc.textFile("/home/hduser/workspace/Score/src/main/resources/Data/*.csv")
    val eplheader = epldataallcols.first()
    val epldata = epldataallcols.filter(x=>x!=eplheader)
    case class epltablecols(Division:String,Date:String,HomeTeam:String,AwayTeam:String,FullTimeHomeGoal:BigInt,FullTimeAwayGoal:BigInt,FullTimeResult:String,HalfTimeHomeGoal:BigInt,HalfTimeAwayGoal:BigInt,HalfTimeResult:String)
    val epldatafinal = epldata.map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6)
      ,x.split(",")(7),x.split(",")(8),x.split(",")(9))).toDF("division","date","hometeam","awayteam","fulltimehomegoal","fulltimeawaygoal","fulltimeresult","halftimehomegoal","halftimeawaygoal","halftimeresult")
    //epldatafinal.registerTempTable("epltable")
    //sqlContext.sql("select * from epltable limit 10")
    /* initial Import of the data into Cassandra  */
    epldatafinal.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> "epl", "table" -> "epl_match_summary"))
      .save()
    val ssc = new StreamingContext(sc, Seconds(10))
    val cassandraRDD = ssc.cassandraTable("epl", "epl_match_summary")
    val dstream = new ConstantInputDStream(ssc, cassandraRDD)
    dstream.foreachRDD{ rdd =>
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(rdd.collect.mkString("\n"))
      //dstream.saveToCassandra("epl_out", "epl_match_summary_out")
    }
    dstream.saveToCassandra("epl_out", "epl_match_summary_out")
    ssc.start()
    ssc.awaitTermination()
    
  }
}
