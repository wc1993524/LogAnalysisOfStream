package com.imooc.spark.my_project

import com.imooc.spark.influxdb.InfluxDBConnection
import com.imooc.spark.my_project.domain.OnlineLog
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.influxdb.InfluxDB.ConsistencyLevel

/**
  * 在线日志分析实战
  * bin/spark-submit --class com.imooc.spark.my_project.OnLineLogAnalysis --master yarn --deploy-mode cluster --conf "spark.executor.extraJavaOptions= -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0,org.influxdb:influxdb-java:2.10 ~/software/files/sparktrain-1.0.jar
  */
object OnLineLogAnalysis {

  val shutdownMarker = "hdfs://localhost:8020/tmp/shutdownmarker"
  var stopFlag:Boolean = false

  def main(args: Array[String]): Unit = {
    val topics = "OnLineLogAnalysis"
    val brokers = "localhost:9092"

    val conf = new SparkConf().setAppName("OnLineLogAanlysis").setMaster("local[2]")
    //启用反压机制
    conf.set("spark.streaming.backpressure.enabled","true")
    //这是在启用反压机制时每个接收器将接收第一批数据的初始最大接收速率（每秒记录数）
    conf.set("spark.streaming.backpressure.initialRate","100")
    //每个Kafka分区读取数据的最大速率（每秒记录数）
    conf.set("spark.streaming.kafka.maxRatePerPartition","120")


    val ssc = new StreamingContext(conf,Seconds(60))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"-> brokers)

    val message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicsSet
    )

    val logs = message.map(_._2)

    val formatLog = logs.map(line=>{
      val array = line.split("\t")
      OnlineLog(array(0),array(1),array(2),array(3),array(4))
    })

    /**过滤出error和warn日志*/
    val errorLogs = formatLog.filter(_.logType.equals("ERROR"))
    val warnLogs = formatLog.filter(_.logType.equals("WARN"))

    /**合并得到error和warn的日志*/
    val unionLogs = errorLogs.union(warnLogs)

    unionLogs.foreachRDD(rdd => {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      if(!rdd.isEmpty()){
        val onlineLogDF = rdd.map(x => OnlineLog(x.hostName,x.serviceName,x.lineTimestamp,x.logType,x.logInfo)).toDF()

        onlineLogDF.createOrReplaceTempView("OnLineLogAanlysis")

        val OnLineLogAanlysisDF = spark.sql(
          "select hostName,serviceName,logType,count(logType) as num from OnLineLogAanlysis group by hostName,serviceName,logType"
        )
        OnLineLogAanlysisDF.show(10)

        OnLineLogAanlysisDF.foreachPartition(partitions=>{
          var filedValue = ""
          partitions.foreach(info => {
            val hostName = info.getAs[String]("hostName")
            val serviceName = info.getAs[String]("serviceName")
            val logType = info.getAs[String]("logType")
            val count = info.getAs[Long]("num")
            val host_service_logtype = hostName + "_" + serviceName + "_" + logType
            filedValue = filedValue + "logtype_count,host_service_logtype="+host_service_logtype +
              " count="+count+"\n";
          })

          if(filedValue.length != 0){
            val connection = new InfluxDBConnection("amdin", "admin", "http://localhost:8086", "test", null)
            connection.batchInsert(ConsistencyLevel.ONE,filedValue)
          }
        })
      }
    })

    ssc.addStreamingListener(new MyStreamingListener(ssc))
    ssc.start()
//    ssc.awaitTermination()

    /**优雅停止sparkstreaming作业代码实现
      通过 hdfs dfs -touchz /tmp/shutdownmarker添加标记可以优雅停止人物*/
    val checkIntervalMillis = 10000
    var isStopped = false

    while (! isStopped) {
      println("calling awaitTerminationOrTimeout")

      /**等待执行停止。执行过程中发生的任何异常都会在此线程中抛出，如果执行停止了返回true，
      线程等待超时长，当超过timeout时间后，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false*/
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running...")
      /**检查标记是否存在*/
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        /**第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
        第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止*/
        ssc.stop(true, true)
        println("ssc is stopped!!!!!!!")
      }
    }
  }

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {
    /**@transient 是类型修饰符，只能用来修饰字段。
    在对象序列化过程中，被transient标记的变量不会被序列化*/
    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  /**检查hdfs上的shutdown标记是否存在*/
  def checkShutdownMarker = {
    if (!stopFlag) {
      val path  = new Path(shutdownMarker)
      val fs = path.getFileSystem(new Configuration())
//      val fs = FileSystem.get(new Configuration())
      stopFlag = fs.exists(path)
    }
  }
}
