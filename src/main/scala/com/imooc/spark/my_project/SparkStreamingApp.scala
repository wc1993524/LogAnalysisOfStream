package com.imooc.spark.my_project

import com.imooc.spark.project.dao.CourseClickCountDAO
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount}
import com.imooc.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {

    if(args.length != 4){
      println("Usage SparkStreamingApp <zkQuorum> <groupId> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum,groupId,topics,numThreads) = args

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingApp")
    val ssc = new StreamingContext(conf,Seconds(60))

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val message = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)

    val logs = message.map(_._2)
    val cleanData = logs.map(log => {
      val infos = log.split("\t")
      val url = infos(2).split(" ")(1)

      var courseId = 0
      if(url.startsWith("class/")){
        val courseStr = url.split("/")(1)
        courseId = courseStr.substring(0,courseStr.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(4).toInt,infos(3))
    }).filter(_.courseId != 0)

    cleanData.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    cleanData.map(data =>{
      (data.time.substring(0,8)+"_"+data.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd =>{
      rdd.foreachPartition(partiton => {
        var list = new ListBuffer[CourseClickCount]
        partiton.foreach(x=>{
          list.append(CourseClickCount(x._1,x._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
