package ContinuousLogin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 20210321-20210327 作业二
 * RDD实现年度最大连续登录天数统计
 */
object ContinuousLogin {

	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("RewrittenAsRDD")
		val sc = new SparkContext(sparkConf)

		// 读取输入文件，获得包含所有数据的RDD
		val lines: RDD[String] = sc.textFile("tu/input/continuous_login_info.txt")

		val rdd = lines.map(x => {
			val name = x.split(",")(0)
			val date = x.split(",")(1)
			(name, date)
		})

		// 根据name分组，将同一个name的数据分到同一个组内
		val groupedRDD = rdd.groupByKey()

		// 组内排序
		val sortedRDD = groupedRDD.flatMapValues(x => {
			val sorted = x.toSet.toList.sorted

			// 定义一个日期工具类
			val calendar = Calendar.getInstance()
			val dateFormat = new SimpleDateFormat("yyyyMMdd")

			var index = 0
			sorted.map(dateStr => {
				val date = dateFormat.parse(dateStr)
				calendar.setTime(date)
				calendar.add(Calendar.DATE, -index)
				index +=1

				(dateStr, dateFormat.format(calendar.getTime))
			})
		})

		// 获得所有连续登录区间的天数
		val continuousResult = sortedRDD.map(x => ((x._1, x._2._2), x._2._1))
				.groupByKey()
				.mapValues(x => {
					val list = x.toList.sorted
					val times = list.size
					times
				})
				.map(x => (x._1._1, x._2))

		//(ruoze,3)
		//(ruoze,1)
		//(ruoze,1)
		//(pk,3)
		//(pk,2)
		//(pk,4)
		continuousResult.foreach(println)

		// 聚合
		val groupedContinuousResult = continuousResult.groupByKey()
		//(ruoze,CompactBuffer(1, 3, 1))
		//(pk,CompactBuffer(3, 4, 2))
		groupedContinuousResult.foreach(println)

		// 获得每个name的最大连续登录天数
		val maxContinuousResult = groupedContinuousResult.map(x => {
			val name = x._1
			val maxContinuousDays = x._2.toList.sortWith(_ > _)
			(name, maxContinuousDays.head)
		})

		//(ruoze,3)
		//(pk,4)
		maxContinuousResult.foreach(println)

		sc.stop()
	}
}
