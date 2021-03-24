package rewrittenAsRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 20210321-20210327 作业一
 * 把那一堆MR改写成RDD，SparkCore实现
 */
object RewrittenAsRDD {

	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("rewrittenAsRDD")
		val sc = new SparkContext(sparkConf)

		// 读取输入文件，获得包含所有数据的RDD
		val rdd: RDD[String] = sc.textFile("tu/input/stu_score_info.txt")

		// 一共有多少个 小于/等于/大于 20岁的人参加考试？
		val count1 = rdd.map(x => x.split(" ")).filter(x => x(2).toInt < 20).groupBy(_(1)).count()
		val count2 = rdd.map(x => x.split(" ")).filter(x => x(2).toInt == 20).groupBy(_(1)).count()
		val count3 = rdd.map(x => x.split(" ")).filter(x => x(2).toInt > 20).groupBy(_(1)).count()

		// 一共有多个男生/女生参加考试？
		val count4 = rdd.map(x => x.split(" ")).filter(x => x(3).equals("男")).groupBy(_(1)).count()
		val count5 = rdd.map(x => x.split(" ")).filter(x => x(3).equals("女")).groupBy(_(1)).count()

		// 12班、13班 有多少人参加考试？
		val count6 = rdd.map(x => x.split(" ")).filter(x =>x(0).equals("12")).groupBy(_(1)).count()
		val count7 = rdd.map(x => x.split(" ")).filter(x =>x(0).equals("13")).groupBy(_(1)).count()

		// 语文、数学、英语科目的平均成绩是多少？
		val count8 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("chinese")).map(x => x(5).toInt).mean()
		val count9 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("math")).map(x => x(5).toInt).mean()
		val count10 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("english")).map(x => x(5).toInt).mean()

		// 每个人的平均成绩是多少？
		val count11 = rdd.map(x => x.split(" ")).map(x => (x(1), x(5).toInt)).groupByKey().map(x => (x._1, x._2.sum/x._2.size))

		// 12班平均成绩是多少？
		val count12 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("12")).map(x => x(5).toInt).mean()
		// 12班男生平均总成绩是多少？
		val count13 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("12") && x(3).equals("男")).map(x => (x(1), x(5).toInt)).groupByKey().map(x => x._2.sum).mean()
		// 12班女生平均总成绩是多少？
		val count14 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("12") && x(3).equals("女")).map(x => (x(1), x(5).toInt)).groupByKey().map(x => x._2.sum).mean()
		// 13班平均成绩是多少？
		val count15 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("13")).map(x => x(5).toInt).mean()
		// 13班男生平均总成绩是多少？
		val count16 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("13") && x(3).equals("男")).map(x => (x(1), x(5).toInt)).groupByKey().map(x => x._2.sum).mean()
		// 13班女生平均总成绩是多少？
		val count17 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("13") && x(3).equals("女")).map(x => (x(1), x(5).toInt)).groupByKey().map(x => x._2.sum).mean()

		// 全校语文成绩最高分是多少？
		val count18 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("chinese")).map(x => x(5)).max()

		// 12班语文成绩最低分是多少？
		val count19 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("chinese") && x(0).equals("12")).map(x => x(5)).min()

		// 13班数学最高成绩是多少？
		val count20 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("math") && x(0).equals("13")).map(x => x(5)).max()

		// 总成绩大于150分的12班的女生有几个？
		val count21 = rdd.map(x => x.split(" ")).filter(x => x(0).equals("12") && x(3).equals("女")).map(x => (x(1), x(5).toInt)).groupByKey().filter(x => x._2.sum > 150).count()

		// 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
		// step1:首先获得总成绩大于150的学生的平均成绩
		val join1 = rdd.map(x => x.split(" ")).map(x => (x(1), x(5).toInt)).reduceByKey(_+_).filter(x => x._2 > 150).map(x => (x._1, x._2/3))
		// step2:获得数学大于等于70，且年龄大于等于19岁的学生
		val join2 = rdd.map(x => x.split(" ")).filter(x => x(4).equals("math") && x(2).toInt >= 19 && x(5).toInt >= 70).map(x => (x(1), x(5).toInt))
		// step:两个RDD做join操作，获得满足所有条件学生的平均成绩
		val result = join1.join(join2).map(x => (x._1, x._2._1))

		println(s"一共有${count1}个小于20岁的人参加考试")
		println(s"一共有${count2}个等于20岁的人参加考试")
		println(s"一共有${count3}个大于20岁的人参加考试")
		println(s"一共有${count4}个男生的人参加考试")
		println(s"一共有${count5}个女生的人参加考试")
		println(s"12班一共有${count6}个人参加考试")
		println(s"13班一共有${count7}个人参加考试")
		println(s"语文科目的平均成绩是$count8")
		println(s"数学科目的平均成绩是$count9")
		println(s"英语科目的平均成绩是$count10")
		println("每个人的平均成绩是:")
		count11.foreach(println)
		println(s"12班全班平均成绩为$count12")
		println(s"12班男生平均总成绩为$count13")
		println(s"12班女生平均总成绩为$count14")
		println(s"13班全班平均成绩为$count15")
		println(s"13班男生平均总成绩为$count16")
		println(s"13班女生平均总成绩为$count17")
		println(s"全校语文成绩最高分是$count18")
		println(s"12班语文成绩最低分是$count19")
		println(s"13班数学最高成绩是$count20")
		println(s"总成绩大于150分的12班的女生有${count21}个")
		//println("总成绩大于150的学生以及平均成绩:")
		//join1.foreach(println)
		//println("数学大于等于70，且年龄大于等于19岁的学生以及数学成绩:")
		//join2.foreach(println)
		println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是:")
		result.foreach(println)

		sc.stop()
	}



}
