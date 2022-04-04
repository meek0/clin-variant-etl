package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.utils.FrequencyUtils._
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VcfUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(
      (Seq(0, 1), Seq("PASS")),
      (Seq(1, 1), Seq("PASS")),
      (Seq(0, 0), Seq("PASS")),
      (Seq(1, -1), Seq("PASS")),
      (Seq(-1, -1), Seq("PASS")),
      (Seq(0, 1), Seq("FAILED"))
    ).toDF("calls", "filters")
    occurrences
      .select(
        ac
      ).as[Long].collect() should contain only 4

  }

  "an" should "return sum of allele numbers" in {
    import spark.implicits._
    val occurrences = Seq(
      (Seq(0, 1), Seq("PASS")),
      (Seq(1, 1), Seq("PASS")),
      (Seq(0, 0), Seq("PASS")),
      (Seq(1, -1), Seq("PASS")),
      (Seq(-1, -1), Seq("PASS")),
      (Seq(0, 1), Seq("FAILED"))
    ).toDF("calls", "filters")
    occurrences
      .select(
        FrequencyUtils.an
      ).as[Long].collect() should contain only 7

  }

  "pc" should "return number of patient with at least 1 alternate allele" in {
    import spark.implicits._
    val occurrences = Seq(
      ("HET", Seq("PASS")),
      ("HET", Seq("PASS")),
      ("HOM REF", Seq("PASS")),
      ("HOM", Seq("PASS")),
      ("HOM", Seq("FAILED")),
      ("UNK", Seq("PASS"))
    ).toDF("zygosity", "filters")

    occurrences
      .select(
        FrequencyUtils.pc
      ).as[Long].collect() should contain only 3

  }

  "pn" should "return number of patient with a pass filter" in {
    import spark.implicits._
    val occurrences = Seq(
      ("HET", Seq("PASS")),
      ("HET", Seq("PASS")),
      ("HOM REF", Seq("PASS")),
      ("HOM", Seq("PASS")),
      ("HOM", Seq("FAILED")),
      ("UNK", Seq("PASS"))
    ).toDF("zygosity", "filters")

    occurrences
      .select(
        FrequencyUtils.pn
      ).as[Long].collect() should contain only 5

  }
  "hom" should "return number of patients with homozygotes alternate alleles" in {
    import spark.implicits._
    val occurrences = Seq(
      ("HET", Seq("PASS")),
      ("HOM REF", Seq("PASS")),
      ("HOM", Seq("PASS")),
      ("HOM", Seq("FAILED")),
      ("UNK", Seq("PASS"))
    ).toDF("zygosity", "filters")
    occurrences
      .select(
        hom
      ).as[Long].collect() should contain only 1

  }
}
