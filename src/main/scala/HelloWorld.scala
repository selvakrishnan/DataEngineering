object HelloWorld extends App {
  //var a: Int =12
  //println(a)
  //var means variable value can be changed
  //val means varaiable value cannot be changed
  val x= {val a: Int = 100; val b : Int = 300; a+b}
  println(x)
  println("Hello World: Scala is Working")

  //lazy assigment wont use the memory for declared variable
  lazy val y=500

  println(y*500)
}
