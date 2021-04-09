package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 20210321-20210327 作业三
 * RDD实现first_value和last_value
 */
object FirstValueAndLastValue {

	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("FirstValueAndLastValue")
		val sc = new SparkContext(sparkConf)

		val RDD = sc.parallelize(List(
			("zhangsan", "yuwen"),
			("zhangsan", "shuxue"),
			("zhangsan", "yingyu"),
			("lisi", "yuwen"),
			("lisi", "shuxue"),
			("lisi", "yingyu")
		))

		val firstvalue = RDD.groupByKey().sortByKey().map {
			case (name, iterableStr) =>
				(name, firstValue(iterableStr))
		}

		val lastvalue = RDD.groupByKey().sortByKey().map {
			case (name, iterableStr) =>
				(name, lastValue(iterableStr))
		}

		println("分组排序后，RDD的第一个元素为：")
		firstvalue.foreach(println)
		println("分组排序后，RDD的最后一个元素为：")
		lastvalue.foreach(println)

	}

	// 1. rdd实现first_value()
	def firstValue(values: Iterable[String]): String = {
		values.head
	}

	// 2. rdd实现last_value()
	def lastValue(values: Iterable[String]): String = {
		values.last
	}
}
