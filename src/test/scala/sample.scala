
import junit.framework.TestCase
import org.junit.Assert._
import com.knoldus.training.test21
import org.junit.Test

import scala.collection.mutable.ListBuffer

class ExampleSuite extends TestCase {

  var sb: StringBuilder = _
  var lb: ListBuffer[String] = _

  override def setUp() {
    sb = new StringBuilder("ScalaTest is ")
    lb = new ListBuffer[String]
    println("Setup")
  }

  @Test
  def testone: Unit = {
    assertEquals("1","1")
  }

  @Test
  def testtwo :Unit = {
    assertEquals(test21.human401(1),"1I am a monkey")
  }

  override def tearDown(): Unit = {
    println("End")
    super.tearDown()
  }
}