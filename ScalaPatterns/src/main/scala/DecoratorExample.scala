
trait Speak {
  def speak(): Unit = { println("I can speak") }
}

object DecoratorExample {

  def main(args: Array[String]): Unit = {
    val talkingDog=new Dog with Speak

    talkingDog.speak
    println(talkingDog.toString)

    //val regularDog=new Dog //Does not compile
    //regularDog.speak
  }

}
