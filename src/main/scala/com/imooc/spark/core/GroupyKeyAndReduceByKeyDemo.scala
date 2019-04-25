package com.imooc.spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey和groupByKey区别与用法
  */
object GroupyKeyAndReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val config = new SparkConf().setAppName("GroupyKeyAndReduceByKeyDemo").setMaster("local[2]")
    val sc = new SparkContext(config)
    val arr = Array("val config", "val arr")
    val socketDS = sc.parallelize(arr).flatMap(_.split(" ")).map((_, 1))
    //groupByKey 和reduceByKey 的区别：
    //他们都是要经过shuffle的，groupByKey在方法shuffle之间不会合并原样进行shuffle，
    //reduceByKey进行shuffle之前会先做合并,这样就减少了shuffle的io传送，所以效率高一点
    socketDS.groupByKey().map(tuple => (tuple._1, tuple._2.sum)).foreach(x => {
      println(x._1 + " " + x._2)
    })
    println("----------------------")
    socketDS.reduceByKey(_ + _).foreach(x => {
      println(x._1 + " " + x._2)
    })
    sc.stop()
  }
}
