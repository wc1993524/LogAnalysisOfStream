package com.imooc.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
  * spark自定义累加器:实现字符串的拼接
  *
  */
class  MyAccumulator extends AccumulatorV2[String,String]{

  private var res = ""

  override def isZero: Boolean = {res == ""}

  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case o : MyAccumulator => res += o.res
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def copy(): MyAccumulator = {
    val newMyAcc = new MyAccumulator
    newMyAcc.res = this.res
    newMyAcc
  }

  override def value: String = res

  override def add(v: String): Unit = res += v +"-"

  override def reset(): Unit = res = ""
}


object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorTest")
    val sc = new SparkContext(conf)

    val myAcc = new MyAccumulator
    sc.register(myAcc,"myAcc")

    val acc = sc.longAccumulator("avg")
    val nums = Array("1","2","3","4","5","6","7","8")
    val numsRdd = sc.parallelize(nums)

    numsRdd.foreach(num => myAcc.add(num))
    numsRdd.foreach(num => acc.add(num.toLong))
    //自定义累加器
    println(myAcc.value)
    //spark自带累加器
    println(acc.value)

    sc.stop()
  }
}
