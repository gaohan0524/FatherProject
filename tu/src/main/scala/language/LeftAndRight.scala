package language

/**
 * 20210328-20210410 作业六
 * Scala 中Either、Left、Right的用法代码实现
 */
object LeftAndRight {

	// 分别调用compute函数，传入两个值，看返回的两种不同结果
	def main(args: Array[String]): Unit = {
		displayResult(compute(4))
		displayResult(compute(-4))
	}

	// compute函数根据传入值，返回两种不同的类型结果
	def compute(input: Int): Either[String, Double] =
		if (input > 0)
			Right(math.sqrt(input))
		else
			Left("Error computing, invalid input")

	// 通过模式匹配，打印一下左值和右值
	def displayResult(result: Either[String, Double]): Unit = {
		println(s"Raw: $result")
		result match {
			case Right(value) => println(s"Result $value")
			case Left(error) => println(s"Error: $error")
		}
	}
}
