package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.utils.FrequencyUtils._
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrequencyUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {
  import spark.implicits._

  val occurrences: DataFrame = Seq(
    (Seq(0, 1)  , Array("PASS"), 10, 30),
    (Seq(1, 1)  , Array("PASS"), 10, 30),
    (Seq(0, 0)  , Array("PASS"), 10, 30),
    (Seq(1, -1) , Array("PASS"), 10, 30),
    (Seq(-1)    , Array("PASS"), 10, 30),
    (Seq(0, 1)  , Array("PASS"), 10, 30),
  ).toDF("calls", "filters", "ad_alt", "gq")

  "ac" should "return sum of allele count" in {
    occurrences
      .select(
        ac
      ).as[Long].collect() should contain only 5

  }

  "an" should "return sum of allele numbers" in {
    occurrences
      .select(
        FrequencyUtils.an
      ).as[Long].collect() should contain only 12

  }

  "pc" should "return number of patient with at least 1 alternate allele" in {
    occurrences
      .select(
        FrequencyUtils.pc
      ).as[Long].collect() should contain only 4

  }

  "pn" should "return number of patient with a pass filter" in {
    occurrences
      .select(
        FrequencyUtils.pn
      ).as[Long].collect() should contain only 6

  }

  "hom" should "return number of patients with homozygotes alternate alleles" in {
    import spark.implicits._
    Seq(
      ("HET"    , Array("PASS"), 10, 30),
      ("HOM REF", Array("PASS"), 10, 30),
      ("HOM"    , Array("PASS"), 10, 30),
      ("HOM"    , Array("PASS"), 10, 30),
      ("UNK"    , Array("PASS"), 10, 30)
    )
      .toDF("zygosity", "filters", "ad_alt", "gq")
      .select(
        hom
      ).as[Long].collect() should contain only 2

  }
}
