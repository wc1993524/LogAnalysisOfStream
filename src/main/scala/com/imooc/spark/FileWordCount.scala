package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理文件系统(local/hdfs)的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {
    /**
      * sparkStreaming访问文件系统不需要Receiver接受数据，可以只启动一个线程（local）
      */
    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

//    val lines = ssc.textFileStream("file:///home/wangc/software/files/imooc/")
    val lines = ssc.textFileStream("hdfs://localhost:8020/imooc/sparkstreaming/")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
