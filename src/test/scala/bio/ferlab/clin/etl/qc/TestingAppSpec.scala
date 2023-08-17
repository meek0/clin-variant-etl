package bio.ferlab.clin.etl.qc

import bio.ferlab.clin.etl.qc.TestingApp._
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.DataFrame

case class FooBar(foo: Option[String], bar: Option[String])

class TestingAppSpec extends SparkSpec {
  import spark.implicits._

  "shouldBeEmpty" should "return errors" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo2"), Some("bar2"))
    ).toDF

    shouldBeEmpty(df) shouldBe Some("DataFrame should be empty")
  }

  it should "return no errors" in {
    val df: DataFrame = spark.emptyDataFrame

    shouldBeEmpty(df) shouldBe empty
  }

  "shouldNotContainNull" should "return errors for foo" in {
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
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainNull(df, "foo", "bar") shouldBe Some("Column(s) foo, bar should not contain null")
  }

  it should "return errors for foo and bar with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainNull(df) shouldBe Some("Column(s) foo, bar should not contain null")
  }

  it should "return errors for foo with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainNull(df) shouldBe Some("Column(s) foo should not contain null")
  }

  "shouldNotContainOnlyNull" should "return errors for foo" in {
    val df: DataFrame = Seq(
      FooBar(None, Some("bar")),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainOnlyNull(df, "foo") shouldBe Some("Column(s) foo should not contain only null")
  }

  it should "return no errors" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainOnlyNull(df, "foo") shouldBe empty
  }

  it should "return errors for bar" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), None),
      FooBar(Some("foo2"), None)
    ).toDF

    shouldNotContainOnlyNull(df, "bar") shouldBe Some("Column(s) bar should not contain only null")
  }

  it should "return errors for foo and bar" in {
    val df: DataFrame = Seq(
      FooBar(None, None),
      FooBar(None, None)
    ).toDF

    shouldNotContainOnlyNull(df, "foo", "bar") shouldBe Some("Column(s) foo, bar should not contain only null")
  }

  it should "return errors for foo and bar with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(None, None),
      FooBar(None, None)
    ).toDF

    shouldNotContainOnlyNull(df) shouldBe Some("Column(s) foo, bar should not contain only null")
  }

  it should "return errors for foo with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(None, Some("bar")),
      FooBar(None, Some("bar2"))
    ).toDF

    shouldNotContainOnlyNull(df) shouldBe Some("Column(s) foo should not contain only null")
  }

  "shouldNotContainSameValue" should "return errors for foo" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo"), Some("bar2"))
    ).toDF

    shouldNotContainSameValue(df, "foo") shouldBe Some("Column(s) foo should not contain same value")
  }

  it should "return no errors" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo2"), Some("bar2"))
    ).toDF

    shouldNotContainSameValue(df, "foo") shouldBe empty
  }

  it should "return errors for bar" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo2"), Some("bar"))
    ).toDF

    shouldNotContainSameValue(df, "bar") shouldBe Some("Column(s) bar should not contain same value")
  }

  it should "return errors for foo and bar" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo"), Some("bar"))
    ).toDF

    shouldNotContainSameValue(df, "foo", "bar") shouldBe Some("Column(s) foo, bar should not contain same value")
  }

  it should "return errors for foo and bar with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo"), Some("bar"))
    ).toDF

    shouldNotContainSameValue(df) shouldBe Some("Column(s) foo, bar should not contain same value")
  }

  it should "return errors for foo with no parameters" in {
    val df: DataFrame = Seq(
      FooBar(Some("foo"), Some("bar")),
      FooBar(Some("foo"), Some("bar2"))
    ).toDF

    shouldNotContainSameValue(df) shouldBe Some("Column(s) foo should not contain same value")
  }

  it should "return errors for foo with empty table" in {
    val df: DataFrame = spark.emptyDataFrame
    shouldNotBeEmpty(df, "foo") shouldBe Some("DataFrame foo should not be empty")
  }

  it should "return no errors for foo with none-empty table" in {
    val df: DataFrame = Seq("1", "2").toDF
    shouldNotBeEmpty(df, "foo") shouldBe None
  }

  "combineErrors" should "combine several errors" in {
    combineErrors(Some("error 1"), Some("error 2"), None) shouldBe Some(
      """error 1
        |error 2""".stripMargin)
  }
}
