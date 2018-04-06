package com.qiuchen.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 再学spark wordcount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val inPath = args(0)
    val outPath = args(1)
    val conf = new SparkConf()setAppName("SparkWordCount")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inPath)
    rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outPath)
  }
}
