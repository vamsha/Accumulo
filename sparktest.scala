package com.db.accumulo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparktest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark test").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

      val sc = spark.sparkContext

    /*val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark test session example")
      .getOrCreate()

      val rdd = sparkSession.sparkContext.parallelize(list1)*/

    val list1 = List(1,2,3,4)

    val rdd = sc.parallelize(list1)

    rdd.foreach(x=> println(x))

    val list = List( ("110", 100, "v12", "v12"), ("210", 102, "v22", "v12"), ("310",  103, "v33", "v12"))

    import spark.implicits._
    val columns = List("key", "colq1", "colq2", "colq3")
    val df = spark.sparkContext.parallelize(list).toDF(columns:_ *)



  }

}
