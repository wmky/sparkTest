package com.juanpi.spark.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}


/**
  * Created by wmky_kk on 22/03/2017.
  */
object NetWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://bidev-cdh004:7077").setAppName("NetWordCount")
    val scc = new StreamingContext(conf,Seconds(30))
    val lines = scc.socketTextStream("bidev-cdh001",1111) // 注意不能使用localhost,即使是在那台机器上，否则出现连不上nc
    val words = lines.flatMap(line => line.split(" "))
//    val words = lines.flatMap(_.split(""))
    val pair = words.map(word => (word,1))
//    val pair = words.map((_,1))
    val wordCounts = pair.reduceByKey((a,b) => a+b)
    println("runing result is ......")
    wordCounts.print()
    scc.start() // Start to computation 若不加这个条件则无法执行。
    scc.awaitTermination() // Wait for the computation to terminate
  }
}
