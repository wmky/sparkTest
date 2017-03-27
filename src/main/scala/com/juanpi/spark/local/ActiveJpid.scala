package com.juanpi.spark.local

import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.Json


/**
  * Created by wmky_kk on 25/03/2017.
  * 使用windows spark本地调试
  */
object ActiveJpid {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ActiveJpid").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 注意了一定是hdfs路径。
    val lines = sc.textFile("file:///H:\\wmky_kk\\Documents\\0TempFiles\\mb_bootstrap_log_20170324.txt")
    println("lines count" + lines.count())
    val jpidRDD =lines.map(line => {
      val row = Json.parse(line)
      println("kaikai  row" + row)
      val utm = (row\"utm").asOpt[String].getOrElse("")
      val os = (row\"os").asOpt[String].getOrElse("")
      val imei = (row\"imei").asOpt[String].getOrElse("")
      // 以jpid作为新激活的判定条件。当imei=0（占比8.8%）时，其中通过imsi又可区分80%,这样误差缩小到2.9% 同一个用户被计算激活2次以上的概率为2.9%
      var jpid: String = null
      if (os.equals("android")){
        // android有imei的可以优先使用imei，这样避免同一个user_id对应2个jpid，被当成2个新激活。date = '2017-03-24' and imei = 866473022175253
        if(imei.length > 2) jpid = imei
        // imei国际移动台设备识别码（90%的是15位，少数14位） imsi国际移动客户识别码（80%的是20位）
        // android imei=0时直接使用jpid就是gu_id 00000000-187b-9565-55c7-0059361687d4 （imei=deviceid=0）
        // ticks为时间戳
        else jpid = (row\"jpid").asOpt[String].getOrElse("")
      }
      else jpid = (row\"idfa").asOpt[String].getOrElse("")
      // os = "iOS"时，imei=""; os="android"时，imei为long整型数  ;
      // android 的imei可能为0也是有效数据，所以imei不能作为最佳判断。

      // os = "iOS"时，idfa字段和deviceid都有效(相等)，但是os="android",idfa却记录为mac地址，需要手工去除脏数据 置为空值。
      var idfa: String = null
      if (os.equals("iOS")) idfa = (row\"idfa").asOpt[String].getOrElse("") else idfa = ""
      val starttime = (row\"starttime").asOpt[String].getOrElse("")
      (utm,jpid,os,imei,idfa,starttime)
    })
    // 需要action操作才会执行map
    jpidRDD.collect().map(println(_))
    sc.stop()
  }
}
