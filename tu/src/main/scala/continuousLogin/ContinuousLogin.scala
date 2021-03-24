package continuousLogin

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

		val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("continuousLogin")
		val sc = new SparkContext(sparkConf)

		// 读取输入文件，获得包含所有数据的RDD
		val rdd = sc.textFile("tu/input/continuous_login_info.txt").map {
			lines =>
				val name = lines.split(",")(0)
				val date = lines.split(",")(1)
				(name, date)
		}

		// 根据name分组，将同一个name的数据分到同一个组内
		val groupedRDD = rdd.groupByKey()

		// 组内排序，然后通过递增的索引获得所有连续登录的区间
		val sortedRDD: RDD[(String, (String, String))] = groupedRDD.flatMapValues(x => {
			val sorted = x.toSet.toList.sorted

			// 定义一个日期工具类
			val calendar = Calendar.getInstance()
			val dateFormat = new SimpleDateFormat("yyyyMMdd")

			var index = 0
			sorted.map(dateStr => {
				val date = dateFormat.parse(dateStr)
				calendar.setTime(date)
				calendar.add(Calendar.DATE, -index)
				val dataBegin = dateFormat.format(calendar.getTime)
				index +=1

				(dateStr, dataBegin)
			})
		})

		// 获得所有连续登录区间的天数
		val groupedContinuousResult = sortedRDD.map{ case (name, (dateStr, dateBegin)) => ((name, dateBegin), dateStr)}
				.groupByKey()
				.mapValues(dateStr => {
					val continuousDays = dateStr.toList.size
					continuousDays
				})
				.map{case ((name, dateBegin), continuousDays) => (name, continuousDays)}
				.groupByKey()

		// 获得每个name的最大连续登录天数
		val maxContinuousResult = groupedContinuousResult.map {
			case (name, iterDays) =>
				val maxContinuousDays = iterDays.toList.sortWith(_ > _).head
				(name, maxContinuousDays)
		}

		println("每个用户连续最大登录天数为：")
		maxContinuousResult.foreach(println)

		sc.stop()
	}
}
