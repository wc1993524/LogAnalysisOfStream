package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowWordCount {

  def main(args: Array[String]): Unit = {

     val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowWordCount")
     val ssc = new StreamingContext(sparkConf,Seconds(5))

     ssc.checkpoint("hdfs://localhost:8020/imooc/checkpoint")

     val lines = ssc.socketTextStream("localhost",6789)
     val words = lines.flatMap(_.split(" ")).map((_,1))

//     val results = words.reduceByKeyAndWindow((x : Int,y : Int) => x + y,Seconds(15),Seconds(10))
     val results = words.reduceByKeyAndWindow(_+_,_-_,Seconds(20),Seconds(30))

     results.print()

     ssc.start()
     ssc.awaitTermination()
  }
}
