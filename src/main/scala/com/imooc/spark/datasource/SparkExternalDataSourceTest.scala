package com.imooc.spark.datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Spark外部数据源
  */
object SparkExternalDataSourceTest {
  def main(args: Array[String]): Unit = {
   /* println("Application started...")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate()

    val df = spark.sqlContext.read.format("com.imooc.spark.datasource")
      .load("file:///home/wangc/software/files/externalDataSource.txt")

    df.createOrReplaceTempView("test")
    spark.sql("select * from test").show()

    println("Application Ended...")*/

    println("Application started...")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate()

    val df = spark.sqlContext.read.format("com.imooc.spark.datasource").load("file:///home/wangc/software/files/externalDataSource.txt")

    //print the schema
    df.printSchema()

    //print the data
//    df.show()

    //save the data
//    df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("com.imooc.spark.datasource").save("out_custom/")
    //  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_json/")
    //  df.write.mode(SaveMode.Overwrite).format("io.dcengines.rana.datasource").save("out_none/")

    //select some specific columns
    //  df.createOrReplaceTempView("test")
    //  spark.sql("select id, name, salary from test").show()

    //filter data
    df.createOrReplaceTempView("test")
    spark.sql("select * from test where salary = 50000").show()

    println("Application Ended...")
  }
}
