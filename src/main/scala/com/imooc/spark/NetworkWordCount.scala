package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming处理Socket数据
  *
  * standalone模式运行命令：bin/spark-submit --class com.imooc.spark.NetworkWordCount --master spark://localhost:7077 ~/software/files/sparktrain-1.0.jar
  * yarn client模式运行命令：bin/spark-submit --class com.imooc.spark.NetworkWordCount --master yarn --deploy-mode client --executor-cores 2 ~/software/files/sparktrain-1.0.jar
  * yarn cluster模式运行命令:bin/spark-submit --class com.imooc.spark.NetworkWordCount --master yarn --deploy-mode cluster --executor-cores 2 ~/software/files/sparktrain-1.0.jar
  *
  * 测试： nc -lk 6789
  */
object NetworkWordCount {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("NetworkWordCount")

    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
