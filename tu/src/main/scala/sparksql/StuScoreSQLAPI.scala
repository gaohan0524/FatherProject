package sparksql

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, count, countDistinct, min, sum}

/**
 * 20210411-20210417 使用Spark SQL的API完成学生成绩统计作业
 */
object StuScoreSQLAPI {

	case class Table(grade:Int, name:String, age:Int, sex:String, course:String, score:Int)

	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder()
				.master("local[2]")
				.getOrCreate()

		import spark.implicits._

		val student = spark.sparkContext.textFile("tu/input/stu_score_info.txt")
				.map(_.split(" "))
				.map(x => Table(x(0).trim.toInt, x(1).trim, x(2).trim.toInt, x(3).trim, x(4).trim, x(5).trim.toInt))
				.toDF

		student.printSchema()
		student.show()

		//    student.foreach(println)

		/**
		 *  Table(12,张三,25,男,chinese,50)
        Table(12,张三,25,男,math,60)
        Table(12,张三,25,男,english,70)
        Table(12,李四,20,男,chinese,50)
        Table(12,李四,20,男,math,50)
        Table(12,李四,20,男,english,50)
        Table(12,王五,19,女,chinese,70)
        Table(12,王五,19,女,math,70)
        Table(12,王五,19,女,english,70)
        Table(13,赵四,25,男,chinese,60)
        Table(13,赵四,25,男,math,60)
        Table(13,赵四,25,男,english,70)
        Table(13,田七,20,男,chinese,50)
        Table(13,田七,20,男,math,60)
        Table(13,田七,20,男,english,50)
        Table(13,诸葛,19,女,chinese,70)
        Table(13,诸葛,19,女,math,80)
        Table(13,诸葛,19,女,english,70)
		 */

		/**
		一共有多少个小于20岁的人参加考试？
    一共有多少个等于20岁的人参加考试？
    一共有多少个大于20岁的人参加考试？
    一共有多个男生参加考试？
    一共有多少女生参加考试？
    12班有多少人参加考试？
    13班有多少人参加考试？
		 */


		/**
		|-- grade: integer (nullable = false)
    |-- name: string (nullable = true)
    |-- age: integer (nullable = false)
    |-- sex: string (nullable = true)
    |-- course: string (nullable = true)
    |-- score: integer (nullable = false)
		 */

		// 一共有多少个小于20岁的人参加考试？
		val qus1 = student.filter("age < 20").agg(countDistinct("name").as("qus1")).show()

		// 一共有多少个等于20岁的人参加考试？
		val qus2 = student.filter("age = 20").agg(countDistinct("name").as("qus2")).show()

		// 一共有多少个大于20岁的人参加考试？
		val qus3 = student.filter("age > 20").agg(countDistinct("name").as("qus3")).show()

		// 一共有多个男生参加考试？
		val qus4 = student.filter("sex = '男'").agg(countDistinct("name").as("qus4")).show()

		// 一共有多少女生参加考试？
		val qus5 = student.filter("sex = '女'").agg(countDistinct("name").as("qus5")).show()

		// 12班有多少人参加考试？
		val qus6 = student.filter("grade = 12").agg(countDistinct("name").as("qus6")).show()

		// 13班有多少人参加考试？
		val qus7 = student.filter("grade = 13").agg(countDistinct("name").as("qus7")).show()

		/**
		语文科目的平均成绩是多少？
    数学科目的平均成绩是多少？
    英语科目的平均成绩是多少？
    每个人平均成绩是多少？
    12班平均成绩是多少？
    12班男生平均总成绩是多少？
    12班女生平均总成绩是多少？
    13班平均成绩是多少？
    13班男生平均总成绩是多少？
    13班女生平均总成绩是多少？
		 */

		// 语文科目的平均成绩是多少？
		val qus8 = student.filter("course = 'chinese'").agg(avg("score").as("avgChinese")).show()

		// 数学科目的平均成绩是多少？
		val qus9 = student.filter("course = 'math'").agg(avg("score").as("avgMath")).show()

		// 英语科目的平均成绩是多少？
		val qus10 = student.filter("course = 'english'").agg(avg("score").as("avgEnglish")).show()


		// 每个人平均成绩是多少？
		val qus11 = student.groupBy("name").agg(avg("score").as("avgScore")).select("name", "avgScore").show()

		// 12班平均成绩是多少？
		val qus12 = student.filter("grade = 12").agg(avg("score").as("qus12")).show()

		// 12班男生平均总成绩是多少?
		val qus13 = student.filter("grade = 12").filter("sex = '男'").agg(avg("score").as("qus13")).show()

		// 12班女生平均总成绩是多少？
		val qus14 = student.filter("grade = 12").filter("sex = '女'").agg(avg("score").as("qus14")).show()

		// 13班平均成绩是多少？
		val qus15 = student.filter("grade = 13").agg(avg("score").as("qus15")).show()

		// 13班男生平均总成绩是多少？
		val qus16 = student.filter("grade = 13").filter("sex = '男'").agg(avg("score").as("qus16")).show()

		// 13班女生平均总成绩是多少？
		val qus17 = student.filter("grade = 13").filter("sex = '女'").agg(avg("score").as("qus17")).show()

		/**
		全校语文成绩最高分是多少？
      12班语文成绩最低分是多少？
      13班数学最高成绩是多少？
		 */

		// 全校语文成绩最高分是多少？
		val qus18 = student.filter("course = 'chinese'").agg(functions.max("score").as("qus18")).show()

		// 12班语文成绩最低分是多少？
		val qus19 = student.filter("grade = 12").filter("course = 'chinese'").agg(min("score").as("qus19")).show()

		// 13班数学最高成绩是多少？
		val qus20 = student.filter("grade = 13").filter("course = 'math'").agg(functions.max("score").as("qus20")).show()


		/**
		总成绩大于150分的12班的女生有几个？
	    总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
		 */

		// 总成绩大于150分的12班的女生有几个？
		val qus21 = student
				.filter("grade = 12").groupBy("name").agg(sum("score").as("sumScore"))
				.filter("sumScore > 150")
				.agg(countDistinct("name").as("qus21")).show()

		// 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
		val tmp = student.filter("age >= 19")
				.groupBy("name")
				.agg(sum("score").as("sumScore"), avg("score").as("avgScore"))
				.filter("sumScore > 150")


		val qus22 = student
				.join(tmp, "name")
				.filter("course = 'math' and score >= 70")
				.select("name", "avgScore")
				.show()

		spark.stop()
	}
}
