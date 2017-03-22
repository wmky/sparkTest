package com.juanpi.spark.datasets

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wmky_kk on 2017/3/19.
 */
object ArrayTest {
//  通过spark-submit提交程序至测试环境测试成功。
//  spark-submit --class com.juanpi.bi.datasets.ArrayTest --master spark://bidev-cdh004:7077 sparkTest-1.0.jar /ArrayTest
  def main (args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://bidev-cdh004:7077").setAppName("ArrayTest")
    val sc = new SparkContext(conf)
    val data = Array(1,2,3,5,9)
    val distData = sc.parallelize(data)
    val sum = distData.reduce((a, b) => a + b)
    println("The sum of all elements in the array  = " + sum)
  }
}

