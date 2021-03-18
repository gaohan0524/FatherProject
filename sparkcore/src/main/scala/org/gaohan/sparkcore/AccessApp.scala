package org.gaohan.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object AccessApp {

	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("SparkWCApp").setMaster("local[4]")
		val sc = new SparkContext(sparkConf)

		sc.textFile("hdfsapi/data/access.log")
				.map(x => {
					val splits = x.split("\t")
					val len = splits.length
					val phone = splits(1)
					val upper = splits(len-3).toInt
					val down = splits(len-2).toInt
					(phone, upper, down)
				})
				.foreach(println)

		sc.stop()
	}
}
