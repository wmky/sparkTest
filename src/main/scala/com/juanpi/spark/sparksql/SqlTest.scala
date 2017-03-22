package com.juanpi.spark.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wmky_kk on 2017/3/19.
 */
object SqlTest {
  def main(args: Array[String]) {
//    spark-submit --class com.juanpi.bi.sparksql.SqlTest --master spark://bidev-cdh004:7077 sparkTest-1.0.jar /SqlTest
    val conf = new SparkConf().setAppName("SqlTest").setMaster("spark://bidev-cdh004:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
//    sqlContext.sql("select * from dw.dim_page").collect().foreach(println)
    val page_df = sqlContext.sql("select page_id,page_name,page_type_id,page_value from dw.dim_page")
    page_df.select("page_id","page_name").write.save("idAndName.parquet")
    page_df.printSchema()
    println("kaikai flag")
  }
}
