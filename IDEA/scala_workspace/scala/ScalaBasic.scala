package scala

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
//import scala.actors.{Actor, Future}

object ScalaBasic {
  def main(args: Array[String]): Unit = {
    sayHello("小明", 23)
//    sayHello(age = 23)
//    println(fab(3))
//    println(sum(2, 3, 4))
//    println(sum(1 to 5: _*))

    //      数列
    ArrayTest()
//    moveMinus()
//    Map和tuple的使用
//    MapTest
//    类的使用
//    val student = new scala.Student
//    println(student.Name)
//    student.Name = "xiaoming"
//    println(student.Name)
//    println(student.age)
//    student.age = 11
//    println(student.age)
//      伴生函数和伴生对象
//    val t1 = Teacher("li")
//    println(t1.name)
    //父类和子类
//    val  teacher2 = new Teacher2
//    println(teacher2.getAge)
    //父类强制转换为子类,需要先判断!
//    val t:Teacher = new Teacher2
//    println(t.isInstanceOf[Teacher])
    //t等于Teacher这个类?false
//    println(t.getClass == classOf[Teacher])
    //t等于Teacher2这个类?true
//    println(t.getClass == classOf[Teacher2])
//    if(t.isInstanceOf[Teacher2])t.asInstanceOf[Teacher2]
    //进行模式匹配
//    t match {
//      case per: Teacher => println("这是Teacher类")
//      case _ => println("未知类型")
//    }
    //进行接口测试
//    val helloWord = new HelloWord
//    helloWord.SayHello("xiaoming")
//    helloWord.log("123")
    //高阶函数
//      greeting(sayHelloWorld,"xiaoming")
//      greeting1(2)
    //函数match
//      matchTest("adds","if2" )
//      matchList(ListBuffer("xiaoming","xiaohong","xiaolv"))
//      matchArray(Array("xiaoming","xiaohong","xiaolv"))
    //泛型实例化
//    val zhurou = new Meat("猪肉")
//    val yangrou = new Meat("羊肉")
//    val baicai  = new Vegetable(name = "白菜")
//    val meatPackeag = packageFood(zhurou,yangrou,baicai)
//    println(meatPackeag)
    //actor的使用
/*    val user = new UserActor
    user.start()
    user!Register("xiao","123");
    user!Login("ming","456")*/
  }
  /*Actor的使用*/
/*  case class Login(username:String, password:String)
  case class Register(username:String, password:String)
  class UserActor extends Actor{
    def act(): Unit ={
      while (true){
        receive(){
          case Login(username:String, password:String) => println("login"+ username)
          case Register(username:String, password:String) => println("register"+username)
        }
      }
    }
  }*/
  /*逆变和斜变*/
  class Master
  class Professional extends Master
  //大师以上才可以进入
  class Card[+T](val name:String)
  def enterMeet(card: Card[Master]): Unit ={}
  //专家就可以进入
  class Card1[-T](val name:String)
  def enterMeet1(card: Card[Professional]): Unit ={}

  /*泛型实例化,必须使用Manifest,打包饭菜*/
  class Meat(val name:String)
  class Vegetable(val name:String)
  def packageFood[T:Manifest](food: T*){
    val foodPackage = new Array[T](food.length)
    for (i <- 0 until food.length)
      foodPackage(i)=food(i)
  }
  /*match函数测试*/
  def matchTest(groud : String, name:String): Unit ={
    groud match {
      case "ab" => println("ab")
      case "cd" => println("cd")
//      case "ab" if name=="if" => println(name + "a")
      case _ if name=="if" => println(name + "a")
      case _ => println("找不到")

    }
  }
  def matchList(list:ListBuffer[String]): Unit ={
      list match {
        case _ if("xiaoming"==list) => println("Hi,xiaoming")
        case _ => println("不玩了")
      }
  }
  def matchArray(array:Array[String]): Unit ={
    array match {
      case Array("xiaoming") => println("Hi,xiaoming")
      case Array("xiaohong") => println("Hi,xiaohong")
      case _ => println("你真没用")
    }
  }
  /*Currying函数,将函数结果作为参数输入*/
  def sum(a:Int)(b:Int) = a+b
  //匿名函数
  val sayHelloWorld = (name:String) => println("我是匿名函数:" + name)
  /*高阶函数*/
  def greeting(func:(String) => Unit,name: String): Unit ={
    func(name)
  }
  def greeting1(num:Int): Unit ={
    //迭代输出元素
    Array(1,2,3,4,5).map((num:Int)=>num*num).foreach(println _)
    //reduceLeft算法,从左边开始,第一个元素乘以第二个,结果乘以第三个...
    println((1 to 5).reduceLeft(_*_))
    //进行队列排序
    var b = new ArrayBuffer[Int]()
    b ++= Array(3,2,5,4,10,1).sortWith(_<_)
    print(b)
  }

  /*triat接口*/
  trait SayHello{
    def SayHello(name:String)
    //拥有具体方法
    def log(msg:String) = println("log:"+msg)
  }
  trait SayWorld{
    def SayWorld(t:Teacher)
  }
  class HelloWord() extends SayHello with SayWorld{
    //重写抽象方法
    override def SayHello(name: String): Unit = println("Hello "+ name)
    override def SayWorld(t: Teacher): Unit = println("World")
  }
  /*伴生函数和伴生对象*/
  class Teacher(){
    val name = ""
    private val age = 23
    def getAge = age
  }
  object  Teacher{
    def apply(name: String): Teacher = new Teacher()
  }
  class Teacher2 extends Teacher{
    override def getAge: Int = super.getAge + 3
  }
/*进行Map的定义*/
  def MapTest(): Unit ={
    //不可变的Map
    val map = Map("xiaoming"->23,"xiaohong"->25)
    //创建可变的Map
    val map1  = scala.collection.mutable.Map("xiaoming"->23,"xiaohong"->25)
    //使用另一种方式定义Map
    val map2 = Map(("xiaoming",23),("xiaohong",25))
    val map3 = scala.collection.mutable.Map(("xiaoming",23),("xiaohong",25))
    //使用contains函数检查key是否存在
    for((key,value)<-map)println(key + value)
    for(key<-map.keySet)println(key)
    for(value<-map.values)println(value)
    //自动排序:SortedMap类(不维护) LinkedHashMap
    val map4 = scala.collection.mutable.SortedMap[String,Int]()
    map4("xiaoming")=3
    map4("xiaohong")=4
    val map5 = scala.collection.mutable.LinkedHashMap(("xiaoming",23),("xiaohong",25))
    //Tuple操作
    val  names = Array("Tuplexiaoming","Tuplexiaohong","Tuplexiaolv")
    val ages = Array(23,12,35)
    val nameAges = names.zip(ages)
    for((name,age)<- nameAges)println(name+":"+age)
  }

  //定义函数
  def sayHello(name:String="啦啦", age:Int): Unit ={
    lazy val j = 0
    var i=0
    while(i<5)
    {
      i+=1
      print(name+"開始輸出：")
      val a = scala.io.StdIn.readInt()
      if (a > 18) {
        println("小红")
      }
      else {
        println("小绿")
      }
    }
  }
  //
  def fab(n: Int): Int = {
    if(n <= 1) 1
    else fab(n - 1) + fab(n - 2)
  }
  //定义可变参数
  def sum(nums: Int*):Int={
/*
    for(num <- nums){
        result += num
    }
    result
   */
    if (nums.length == 0)0
      //使用nums.tail: _*，因为有多个参数
    else
      nums.head + sum(nums.tail: _*)
  }
  //定义使用数组
  def ArrayTest() {
    var a = new Array[String](10)
    a(0)="hello"
    print(a(0))
    import scala.collection.mutable.ArrayBuffer
    var b = new ArrayBuffer[Int]()
//    添加元素
    b += 1
    b += (2,3,4,5)
    b ++= Array(6,7,8,9)
    print(b)
    //第一個代表索引位置，第二個代表數值
    b.insert(5,6)
    //删除元素,第二个代表删除几个
    b.remove(5,1)
//    b.toBuffer()
//    b.toArray()
    /*数组函数*/
    println(b.sum +","+b.max +","+ b.toString() )
    /*yield函数*/
    val b2 = for(ele <- b)yield ele*ele
    println(b2)
    val b3 = for(ele <- b if ele%2 == 0 )yield ele*ele
    println(b3)
    /*函数式编程,将除以2的无余数的乘于2*/
    print(b.filter(_%2 == 0).map(2*_))
  }
  /*移除第一个负数之后的所有负数*/
  def moveMinus(): Unit ={
    import scala.collection.mutable.ArrayBuffer
    var array = new ArrayBuffer[Int]()
    array += (1,2,3,4,5,-1,-3,-5,-9)
    //记录所有不需要移除元素的索引，稍后一次性移除所有元素
    var foundFirstNegative = false
    val keepIndexes = for(i<-0 until array.length
                          if !foundFirstNegative ||array(i)>=0)yield{
      if(array(i)<0)foundFirstNegative = true
      i
    }
    for (i<-0 until keepIndexes.length){
      array(i) = array(keepIndexes(i))
    }
    array.trimEnd(array.length-keepIndexes.length)
    print(array)
  }
}
/*泛型的使用*/
class Student(){

  var Name = "mai" //自动生成getter和setter方法
  private var Age  = 23
  def age = "你的年龄是:" + Age
  def age_=(newAge:Int){
    println("你的年龄无法修改")
  }
}

/*定义泛型,且<:代表student1必须为Student或者Student的子类,即为上边界*/
class  Student1[T <: scala.Student]{}
/*定义泛型,且<:代表student2必须为Student或者Student的父类,即为下边界*/
class  Student2[T >: scala.Student]{}


