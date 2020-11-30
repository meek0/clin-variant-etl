package bio.ferlab.clin.etl

import bio.ferlab.clin.testutils.WithSparkSession
import columns._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLFunctionsSpecs extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "canonical" should "return expected values" in {
    val df = Seq("YES", "NO", "Other", null).toDF("CANONICAL").select(struct("CANONICAL") as "annotation")

    df.select(canonical).as[Boolean].collect() should contain theSameElementsAs Seq(true, false, false, false)
  }

  "zygosity" should "return HOM" in {
    val df = Seq(Seq(1, 1)).toDF("calls")

    df.select(zygosity).as[String].collect() should contain only "HOM"
  }

  it should "return HET" in {
    val df = Seq(Seq(0, 1), Seq(1, 0)).toDF("calls")
    df.select(zygosity).as[String].collect().toSet should contain only "HET"
  }

  it should "return HOM REF" in {
    val df = Seq(Seq(0, 0)).toDF("calls")
    df.select(zygosity).as[String].collect() should contain only "HOM REF"
  }

  it should "return UNK" in {
    val df = Seq(
      Seq(0, -1),
      Seq(-1, 0),
      Seq(-1, 1),
      Seq(1, -1)
    ).toDF("calls")
    df.select(zygosity).as[String].collect().toSet should contain only "UNK"
  }


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
        columns.an
      ).as[Long].collect() should contain only 8

  }

  "pn" should "return number of patient with at least 1 alternate allele" in {
    import spark.implicits._
    val occurrences = Seq("HET", "HET", "HOM REF", "HOM","HOM", "UNK").toDF("zygosity")
    occurrences
      .select(
        columns.pn
      ).as[Long].collect() should contain only 4

  }
  "hc" should "return number of patients with homozygotes alternate alleles" in {
    import spark.implicits._
    val occurrences = Seq("HET", "HOM REF", "HOM", "HOM", "UNK").toDF("zygosity")
    occurrences
      .select(
        columns.hc
      ).as[Long].collect() should contain only 2

  }

  "format_consequences" should  "removed _variant and replaced _ by space)" in {
    val csq = Seq(
      Seq("intron_variant", "splice_acceptor", "exon"),
      Seq("intron_variant")
    )toDF("consequences")

    csq.select(formatted_consequences as "conequences").as[Seq[String]].collect() should contain theSameElementsAs Seq(
      Seq("intron", "splice acceptor", "exon"),
      Seq("intron")
    )

  }
}
