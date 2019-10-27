package com.zhouq.sparkproject

import org.apache.spark.{SparkConf, SparkContext}

/**
 * scala 二次排序测试
 *
 * @Author: zhouq
 * @Date: 2019/10/27
 */
object SortKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SortKeyTest")

    val sc = new SparkContext(conf)

    val arr = Array(
      Tuple2(new SortKey(30, 35, 40), "1"),
      Tuple2(new SortKey(35, 30, 40), "2"),
      Tuple2(new SortKey(30, 38, 30), "3")
    )

    val rdd = sc.parallelize(arr, 1)
    val sortedRDD = rdd.sortByKey(false)

    for (tuple <- sortedRDD.collect()) {
      println(tuple)
    }
  }

}
