package sparksql

import org.apache.spark.sql.SparkSession

/**
 * 20210411-20210417 使用Spark SQL的SQL完成学生成绩统计作业
 */
object StuScoreSQL {

	case class Table(grade:Int, name:String, age:Int, sex:String, course:String, score:Int)

	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder()
				.master("local")
				.getOrCreate()

		import spark.implicits._

		val student = spark.sparkContext.textFile("tu/input/stu_score_info.txt")
				.map(x => {
					val splits = x.split(" ")
					val grade = splits(0).trim.toInt
					val name = splits(1).trim
					val age = splits(2).trim.toInt
					val gender = splits(3).trim
					val course = splits(4).trim
					val score = splits(5).trim.toInt
					(grade,name,age,gender,course,score)
				}).toDF("grade","name","age","gender","course","score")


		student.createOrReplaceTempView("student")

		//        student.printSchema()
		//        student.show()

		/**
		+-----+----+---+------+-------+-----+
    |grade|name|age|gender| course|score|
    +-----+----+---+------+-------+-----+
    |   12|张三| 25|    男|chinese|   50|
    |   12|张三| 25|    男|   math|   60|
    |   12|张三| 25|    男|english|   70|
    |   12|李四| 20|    男|chinese|   50|
    |   12|李四| 20|    男|   math|   50|
    |   12|李四| 20|    男|english|   50|
    |   12|王五| 19|    女|chinese|   70|
    |   12|王五| 19|    女|   math|   70|
    |   12|王五| 19|    女|english|   70|
    |   13|赵四| 25|    男|chinese|   60|
    |   13|赵四| 25|    男|   math|   60|
    |   13|赵四| 25|    男|english|   70|
    |   13|田七| 20|    男|chinese|   50|
    |   13|田七| 20|    男|   math|   60|
    |   13|田七| 20|    男|english|   50|
    |   13|诸葛| 19|    女|chinese|   70|
    |   13|诸葛| 19|    女|   math|   80|
    |   13|诸葛| 19|    女|english|   70|
    +-----+----+---+------+-------+-----+
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

		// 一共有多少个小于20岁的人参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus1 from student where age < 20
			  |""".stripMargin).show()

		// 一共有多少个等于20岁的人参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus2 from student where age = 20
			  |""".stripMargin).show()

		// 一共有多少个大于20岁的人参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus3 from student where age > 20
			  |""".stripMargin).show()

		// 一共有多个男生参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus4 from student where gender = '男'
			  |""".stripMargin).show()

		// 一共有多少女生参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus5 from student where gender = '女'
			  |""".stripMargin).show()

		// 12班有多少人参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus6 from student where grade = 12
			  |""".stripMargin).show()

		// 13班有多少人参加考试？
		spark.sql(
			"""
			  |select count(distinct(name)) qus7 from student where grade = 13
			  |""".stripMargin).show()

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
		spark.sql(
			"""
			  |select avg(score) qus8 from student where course = 'chinese'
			  |""".stripMargin).show()

		// 数学科目的平均成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus9 from student where course = 'math'
			  |""".stripMargin).show()

		// 英语科目的平均成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus10 from student where course = 'english'
			  |""".stripMargin).show()

		// 每个人平均成绩是多少？
		spark.sql(
			"""
			  |select name,avg(score) from student group by name
			  |""".stripMargin).show()

		// 12班平均成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus12 from student where grade = 12
			  |""".stripMargin).show()

		// 12班男生平均总成绩是多少?
		spark.sql(
			"""
			  |select avg(score) qus13 from student where grade = 12 and gender = '男'
			  |""".stripMargin).show()

		// 12班女生平均总成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus14 from student where grade = 12 and gender = '女'
			  |""".stripMargin).show()

		// 13班平均成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus15 from student where grade = 13
			  |""".stripMargin).show()

		// 13班男生平均总成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus16 from student where grade = 13 and gender = '男'
			  |""".stripMargin).show()

		// 13班女生平均总成绩是多少？
		spark.sql(
			"""
			  |select avg(score) qus17 from student where grade = 13 and gender = '女'
			  |""".stripMargin).show()

		/**
		全校语文成绩最高分是多少？
      12班语文成绩最低分是多少？
      13班数学最高成绩是多少？
		 */

		// 全校语文成绩最高分是多少？
		spark.sql(
			"""
			  |select max(score) qus18 from student where course = 'chinese'
			  |""".stripMargin).show()

		// 12班语文成绩最低分是多少？
		spark.sql(
			"""
			  |select min(score) qus19 from student where course = 'chinese' and grade = 12
			  |""".stripMargin).show()

		// 13班数学最高成绩是多少？
		spark.sql(
			"""
			  |select max(score) qus20 from student where course = 'math' and grade = 13
			  |""".stripMargin).show()

		/**
		总成绩大于150分的12班的女生有几个？
	    总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
		 */

		// 总成绩大于150分的12班的女生有几个？
		spark.sql(
			"""
			  |select count(name) qus21 from (
			  |select name,sum(score) sumScore from student where grade = 12 group by name having sumScore > 150
			  |)
			  |""".stripMargin).show()

		// 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
		spark.sql(
			"""
			  |select
			  |name, avgScore
			  |from
			  |(select
			  |name,sum(score) over(partition by name) sumScore,avg(score) over(partition by name) avgScore,score,course
			  |from student
			  |where age >=19
			  |)
			  |where sumScore > 150 and course = 'math' and score >= 70
			  |""".stripMargin).show()

		spark.stop()
	}
}
