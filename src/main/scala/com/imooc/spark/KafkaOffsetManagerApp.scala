package com.imooc.spark

import _root_.kafka.message.MessageAndMetadata
import _root_.kafka.serializer.StringDecoder
import _root_.kafka.common.TopicAndPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc._

/**
  * Spark Streaming整合Kafka offset管理
  * https://www.jianshu.com/p/08b89df5965a
  */
object KafkaOffsetManagerApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaOffsetManagerApp")
    val ssc = new StreamingContext(conf,Seconds(10));

    //sparkStreaming对接kafka
    val topicsSet = "kafka_offset_manager_topic".split(",").toSet
    val groupId = "ruoze.group.id"
    val kafkaParams = Map[String,String](
      "metadata.broker.list"-> "localhost:9092",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    //读取mysql,获取kafka偏移量offset
    //val fromOffsets = MysqlUtil.select()
    //加载数据库配置信息,默认加载db.default.*
    DBs.setup()
    val fromOffsets = DB.readOnly(implicit session => {
      SQL(s"select * from `kafka_offset` where groupid = '${groupId}'").map(rs => {
        (TopicAndPartition(rs.string("topic"),rs.int("partition")),rs.long("offset"))
      }).list.apply()
    }).toMap
    println("********************"+fromOffsets)

    val messages = if(fromOffsets.size == 0){
      //第一次开始运行，从smallest开始消费，即从头开始消费
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkaParams,topicsSet
      )
    }else{
      //从zk中读取到偏移量，从上次的偏移量开始消费数，避免出现kafka重复消费
      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
        ssc,kafkaParams,fromOffsets,messageHandler
      )
    }

    //打印结果到控制台,注意重复消费情况是否消失
    messages.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        println("~~~~~~~~~~~~~~~~~~~~~~"+rdd.count())

        //获取当前rdd所更新到的offset值（OffsetRange(topic: 'kafka_test4', partition: 0, range: [1 -> 4])）
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //更新每个批次的偏移量到Mysql中，注意这段代码是在driver上执行的
        DB.localTx { implicit session =>
          for (or <- offsetRanges) {
            SQL("replace into `kafka_offset`(topic,groupid,partition,offset) values(?,?,?,?)")
              .bind(or.topic, groupId, or.partition, or.untilOffset).update().apply()
          }
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
