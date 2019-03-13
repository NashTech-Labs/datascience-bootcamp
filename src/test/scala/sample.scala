
import junit.framework.TestCase
import org.junit.Assert._
import org.junit.Test
import com.knoldus.training.Sample
import scala.collection.mutable.ListBuffer

class ExampleSuite extends TestCase {

  var sb: StringBuilder = _
  var lb: ListBuffer[String] = _

  override def setUp() :Unit = {
    sb = new StringBuilder("ScalaTest is ")
    lb = new ListBuffer[String]
    println("Setup")
  }

  @Test
  def testone: Unit = {
    assertEquals("1","1")
  }

  override def tearDown(): Unit = {
    println("End")
    super.tearDown()
  }
}