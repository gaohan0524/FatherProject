package org.apache.spark

import org.apache.spark.rdd.MapPartitionsRDD

object MapPartitionsRDDApp {
	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setAppName("SparkWCApp").setMaster("local[4]")
		val sc = new SparkContext(sparkConf)
		val rdd = sc.parallelize(1 to 10)

		new MapPartitionsRDD[Int, Int](rdd,
			(_, _, iterator) => {
				iterator.map(_ * 2)
			}).foreach(println)

		sc.stop()
	}
}
