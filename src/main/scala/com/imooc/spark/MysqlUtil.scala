package com.imooc.spark

import java.sql.{Connection, DriverManager}
import _root_.kafka.common.TopicAndPartition


/**
  * Mysql工具类
  */
object MysqlUtil{

  //初始化数据连接
  var connection: Connection = _

  def getConnection(): Unit ={
    // 访问本地MySQL服务器，通过3306端口访问mysql数据库
    val url = "jdbc:mysql://localhost:3306/kafka?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //驱动名称
    val driver = "com.mysql.jdbc.Driver"

    //用户名
    val username = "root"
    //密码
    val password = "password"
    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  def select() ={
    //获取链接
    getConnection()

    var result = Map[TopicAndPartition, Long]()
    val statement = connection.createStatement
    //执行查询语句，并返回结果
    val rs = statement.executeQuery("SELECT * FROM kafka_offset")
    //打印返回结果
    while (rs.next) {
      val topic = rs.getString("topic")
      val partition = rs.getInt("partition")
      val offset = rs.getLong("offset")
      result = Map(TopicAndPartition(topic,partition)->offset)
    }

    //关闭连接，释放资源
    connection.close
    result
  }

  def save(topic:String,groupid:String,partition:Int,offset:Long): Unit ={
    //获取链接
    getConnection()
    val statement = connection.createStatement
    //插入数据
    val rs = statement.executeUpdate("INSERT INTO `kafka_offset` (`topic`, `groupid`, `partition`, `offset`) VALUES ('"+topic+"','"+groupid+"',"+partition+","+offset+")")
    println(rs.toString)
  }

  def main(args: Array[String]): Unit = {
    save("kafka_offset_manager_topic","ruoze.group.id",1,100)
  }
}
