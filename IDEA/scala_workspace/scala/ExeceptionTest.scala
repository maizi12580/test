package scala

object ExeceptionTest {
  def main(args: Array[String]): Unit = {
    try {
      //throw new IllegalArgumentException("x")
      throw new IllegalArgumentException("y")
    }catch {
      case x:IllegalArgumentException => println("sorry ,IllegalArgumentException x")
      case y:IllegalArgumentException => println("sorry ,IllegalArgumentException y")
    }finally {
      print("释放资源")
    }
  }
}
