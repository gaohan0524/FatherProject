package org.gaohan.sparkcore

object ScalaApp {

	def main(args: Array[String]): Unit = {

		val list = List(1,2,3,4,5,6)

		myMap(list, _ * 2).foreach(println)
		myFilter(list, _ > 4).foreach(println)
		myForeach(list, println)
	}

	def myMap(list: List[Int], op:Int => Int): List[Int] = {
		for (ele <- list) yield op(ele)
	}

	def myFilter(list: List[Int], op: Int =>Boolean): List[Int] = {
		for (ele <- list if op(ele)) yield ele //yield 表示有返回值 把当前的元素记下来，保存在集合中，循环结束后将返回该集合
	}

	def myForeach(list: List[Int], op:Int => Unit):Unit = {
		for (ele <- list)
			op(ele)
	}


}
