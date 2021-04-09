package sparkcore

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Random

/**
 * 20210328-20210410 作业七
 * 使用代码实现join窄依赖
 */
object JoinNarrowDependency {

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("JoinNarrowDependency").setMaster("local[4]")
		val sc = new SparkContext(conf)

		val random = Random
		val col1 = Range(1, 50).map(x => (random.nextInt(10), s"user$x"))
		val col2 = Array((0, "BJ"), (1, "SH"), (2, "GZ"), (3, "SZ"), (4, "TJ"), (5, "CQ"), (6, "HZ"), (7, "NJ"), (8, "WH"), (0, "CD"))

		// 初始化rdd
		val rdd1 = sc.makeRDD(col1)
		val rdd2 = sc.makeRDD(col2)

		// 直接将rdd1和rdd2做join
		val joinRDD1 = rdd1.join(rdd2)
//				.collect()
		println(joinRDD1.toDebugString)

		// 先将rdd1和rdd2的分区数调整一致，再做join
		val joinRDD2 = rdd1.partitionBy(new HashPartitioner(3))
				.join(rdd2.partitionBy(new HashPartitioner(3)))
//				.collect()
		println(joinRDD2.toDebugString)

		System.in.read
		sc.stop()
	}
}
