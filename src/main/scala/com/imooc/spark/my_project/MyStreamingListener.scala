package com.imooc.spark.my_project

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class MyStreamingListener (ssc : StreamingContext) extends StreamingListener with Logging{

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    val execTime = batchInfo.processingDelay.getOrElse(0L)
    val schedulingTime = batchInfo.schedulingDelay.getOrElse(0L)
    logInfo(s"执行时间: $execTime 调度延时 : $schedulingTime")
  }
}
