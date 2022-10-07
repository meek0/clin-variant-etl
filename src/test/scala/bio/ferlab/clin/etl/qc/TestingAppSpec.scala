package bio.ferlab.clin.etl.qc

import bio.ferlab.clin.etl.qc.TestingApp.{combineErrors, shouldNotContainNull}
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class FooBar(foo: Option[String], bar: Option[String])
class TestingAppSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {
  import spark.implicits._
  "shouldNotContainNull" should "return errors" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainNull(df, "foo") shouldBe Some("Column(s) foo should not contain null")
  }

  it should "return no errors" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo2"), Some("bar2"))
    ).toDF

    shouldNotContainNull(df, "foo") shouldBe empty
  }

  it should "return errors for bar" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(Some("foo2"), Some("bar2"))
    ).toDF

    shouldNotContainNull(df, "bar") shouldBe Some("Column(s) bar should not contain null")
  }

  it should "return errors for foo and bar" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(None, Some("bar"))
    ).toDF

    shouldNotContainNull(df, "foo", "bar") shouldBe Some("Column(s) foo, bar should not contain null")
  }

  it should "return errors for foo and bar with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(None, Some("bar"))
    ).toDF

    shouldNotContainNull(df) shouldBe Some("Column(s) foo, bar should not contain null")
  }

  "combineErrors" should "combine several errors" in {
    combineErrors(Some("error 1"), Some("error 2"), None) shouldBe Some(
      """error 1
        |error 2""".stripMargin)
  }
}
