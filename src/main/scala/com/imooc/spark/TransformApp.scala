package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    /**
      * 构建黑名单
      *
      * 输入：
      * 20180808,zs
      * 20180808,ls
      * 20180808,ww
      *
      * 黑名单：
      * (zs: true)(ls: true)
      *
      * leftjoin：
      * (zs: [<20180808,zs>, <true>])
      * (ls: [<20180808,ls>, <true>])
      * (ww: [<20180808,ww>, <false>])
      */
    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6789)
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x=> x._2._2.getOrElse(false) != true)
        .map(x=>x._2._1)
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
