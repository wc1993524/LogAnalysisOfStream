package com.imooc.spark.core.study

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCountApp")
    val sc = new SparkContext(conf)

    val text = sc.textFile("file:///home/wangc/software/files/wordcount.txt")
    val wordcount = text.flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_)
      .map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    wordcount.foreach(x => println(x))

    sc.stop()
  }
}
