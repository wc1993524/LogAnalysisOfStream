package com.imooc.spark.datasource

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

/**
  * 类型转换类
  */
object Util {
  def castTo(value : String, dataType : DataType) = {
    dataType match {
      case _ : IntegerType => value.toInt
      case _ : LongType => value.toLong
      case _ : StringType => value
    }
  }
}