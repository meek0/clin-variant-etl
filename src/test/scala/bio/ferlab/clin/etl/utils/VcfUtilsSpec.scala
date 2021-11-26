package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.utils.VcfUtils._
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VcfUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0), Seq(1, -1), Seq(-1, -1), Seq(0, -1)).toDF("calls")
    occurrences
      .select(
        ac
      ).as[Long].collect() should contain only 4

  }

  "an" should "return sum of allele numbers" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0), Seq(1, -1), Seq(-1, -1), Seq(0, -1)).toDF("calls")
    occurrences
      .select(
        VcfUtils.an
      ).as[Long].collect() should contain only 8

  }

  "participant_number" should "return number of patient with at least 1 alternate allele" in {
    import spark.implicits._
    val occurrences = Seq("HET", "HET", "HOM REF", "HOM","HOM", "UNK").toDF("zygosity")
    occurrences
      .select(
        VcfUtils.pc
      ).as[Long].collect() should contain only 4

  }
  "hom" should "return number of patients with homozygotes alternate alleles" in {
    import spark.implicits._
    val occurrences = Seq("HET", "HOM REF", "HOM", "HOM", "UNK").toDF("zygosity")
    occurrences
      .select(
        hom
      ).as[Long].collect() should contain only 2

  }
}
