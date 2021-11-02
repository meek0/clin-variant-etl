package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val localConf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile)))

  val job1 = new Consequences("BAT1", "chr1")
  val job2 = new Consequences("BAT2", "chr1")

  import spark.implicits._
  val raw_variant_calling: DatasetConf = localConf.getDataset("raw_variant_calling")

  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput()).toDF()
  )

  override def beforeAll(): Unit = {
    HadoopFileSystem.remove(job1.destination.location)
  }

  "consequences job" should "transform data in expected format" in {

    val result = job1.transform(data).as[ConsequenceRawOutput].collect().head
    result shouldBe
      ConsequenceRawOutput("1", 69897, 69898, "T", "C", "rs200676709", List("synonymous_variant"), "LOW", "OR4F5", "ENSG00000186092",
        "ENST00000335137", "ENST00000335137", None, "Transcript", 1, "protein_coding", "SNV", EXON(Some(1), Some(1)), INTRON(None, None),
        Some("ENST00000335137.4:c.807T>C"), Some("ENSP00000334393.3:p.Ser269%3D"), "chr1:g.69897T>C", Some(807), Some(843), Some(269),
        AMINO_ACIDS(Some("S"), None), CODONS(Some("tcT"), Some("tcC")), true, true, None, Some("807T>C"), 2, "BAT1",
        `created_on` = result.`created_on`, `updated_on` = result.`updated_on`)
  }

  "consequences job" should "run" in {

    val firstLoad = Seq(VCFInput(
      `names` = List("rs200676709")
    )).toDF()
    val secondLoad = Seq(VCFInput(
      `names` = List("rs200676710")
    )).toDF()
    val date1 = LocalDateTime.of(2021, 1, 1, 1, 1, 1)
    val date2 = LocalDateTime.of(2021, 1, 2, 1, 1, 1)


    val job1Df = job1.transform(Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    job1Df.show(false)
    job1Df.as[ConsequenceRawOutput].collect() should contain allElementsOf Seq(
      ConsequenceRawOutput("1", 69897, 69898, "T", "C", "rs200676709", List("synonymous_variant"), "LOW", "OR4F5", "ENSG00000186092",
        "ENST00000335137", "ENST00000335137", None, "Transcript", 1, "protein_coding", "SNV", EXON(Some(1), Some(1)), INTRON(None, None),
        Some("ENST00000335137.4:c.807T>C"), Some("ENSP00000334393.3:p.Ser269%3D"), "chr1:g.69897T>C", Some(807), Some(843), Some(269),
        AMINO_ACIDS(Some("S"), None), CODONS(Some("tcT"), Some("tcC")), true, true, None, Some("807T>C"), 2, "BAT1",
        `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date1))
    )

    job1.load(job1Df)

    val job2Df = job2.transform(Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
    job2.load(job2Df)
    val resultDf = job2.destination.read
    resultDf.show(false)

    resultDf.as[ConsequenceRawOutput].collect() should contain allElementsOf Seq(
      ConsequenceRawOutput("1", 69897, 69898, "T", "C", "rs200676710", List("synonymous_variant"), "LOW", "OR4F5", "ENSG00000186092",
        "ENST00000335137", "ENST00000335137", None, "Transcript", 1, "protein_coding", "SNV", EXON(Some(1), Some(1)), INTRON(None, None),
        Some("ENST00000335137.4:c.807T>C"), Some("ENSP00000334393.3:p.Ser269%3D"), "chr1:g.69897T>C", Some(807), Some(843), Some(269),
        AMINO_ACIDS(Some("S"), None), CODONS(Some("tcT"), Some("tcC")), true, true, None, Some("807T>C"), 2, "BAT2",
        `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date2))
    )
  }
}
