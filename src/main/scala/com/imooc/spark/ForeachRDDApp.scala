package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //result.print()  //此处仅仅是将统计结果输出到控制台

    //TODO... 将结果写入到MySQL

    /**
      * 此种写法会报序列化异常，官网已提示，不能这样编写
      * This is incorrect as this requires the connection object to be serialized and sent from the driver to the worker.
      * Such connection objects are rarely transferable across machines.
      * This error may manifest as serialization errors (connection object not serializable),
      * initialization errors (connection object needs to be initialized at the workers), etc.
      * The correct solution is to create the connection object at the worker.
      */
//    result.foreachRDD(rdd =>{
//      val connection = createConnection()  // executed at the driver
//      rdd.foreach { record =>
//        val sql = "insert into wordcount(word, wordcount) values('"+record._1 + "'," + record._2 +")"
//        connection.createStatement().execute(sql)
//      }
//    })

    result.print()

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 获取MySQL的连接
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "password")
  }

}
