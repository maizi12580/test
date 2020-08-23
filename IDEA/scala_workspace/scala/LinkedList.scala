package scala

object superLinkedList{
  def main(args: Array[String]): Unit = {
//    list函数测试
        //listTest()
//    单词计数
//    wordTest()
  }
  //list函数测试
  def listTest(): Unit ={
    var list = List("Tony","Go Go","Tom")
    println(list)
    println(list.flatMap(_.split(" ")))
    list.foreach(println(_))
  }
  def  wordTest(): Unit ={
    val line1 = scala.io.Source.fromFile("D:\\2.txt").mkString
    val line2 = scala.io.Source.fromFile("D:\\3.txt").mkString
    val lines = List(line1,line2)
    println(lines)
    println(lines.flatMap(_.split(" ")).map((_,1)).map(_._2).reduceLeft[Int](_ +_))
  }
}
