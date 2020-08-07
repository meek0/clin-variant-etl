import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLFunctionsSpecs extends AnyFlatSpec with WithSparkSession with Matchers {

  "canonical" should "return expected values" in {
    val df = Seq("YES")
  }

}
