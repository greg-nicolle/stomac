package test

import com.google.common.base.Stopwatch
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class StomacTest extends FunSuite
  with MockitoSugar
  with BeforeAndAfter
  with BeforeAndAfterEachTestData
  with BeforeAndAfterAll
  with Matchers {

  val session: SparkSession = SparkSession.builder()
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.memory.fraction", "0.2")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = session.sparkContext


  override def run(testName: Option[String], args: Args): Status = {
    val sw = new Stopwatch
    sw.start()
    val status = super.run(testName, args)
    sw.stop()
    status
  }

  override protected def beforeAll(): Unit = {
  }
}
