package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{VCFInput, VariantRawOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile)
    ))

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_variant_calling")
  val job1 = new Variants("BAT1")
  val job2 = new Variants("BAT2")

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_variant_calling.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.destination.location)
  }


  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput()).toDF()
  )

  "variants job" should "transform data in expected format" in {

    val resultDf = job1.transform(data).as[VariantRawOutput]
    resultDf.show(false)
    val result = resultDf.collect().head

    result shouldBe VariantRawOutput(
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`,
      `normalized_variants_oid` = result.`normalized_variants_oid`)
  }

  "variants job" should "scd1 using locus as key" in {

    val firstLoad = Seq(VCFInput("chr1"), VCFInput("chr2")).toDF()
    val secondLoad = Seq(VCFInput("chr1"), VCFInput("chr3")).toDF()
    val date1 = LocalDateTime.of(2021, 1, 1, 1, 1, 1)
    val date2 = LocalDateTime.of(2021, 1, 2, 1, 1, 1)

    val job1Df = job1.transform(Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    job1.load(job1Df)

    val job2Df = job2.transform(Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
    job2Df.show(false)
    job2.load(job2Df)
    val resultDf = job2.destination.read
    resultDf.show(false)

    resultDf.as[VariantRawOutput].collect() should contain allElementsOf Seq(
      VariantRawOutput("1", `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date2), `batch_id` = "BAT2", `normalized_variants_oid` = Timestamp.valueOf(date2), `locus` = "1-69897-T-C"),
      VariantRawOutput("2", `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date1), `batch_id` = "BAT1", `normalized_variants_oid` = Timestamp.valueOf(date1), `locus` = "2-69897-T-C"),
      VariantRawOutput("3", `created_on` = Timestamp.valueOf(date2), `updated_on` = Timestamp.valueOf(date2), `batch_id` = "BAT2", `normalized_variants_oid` = Timestamp.valueOf(date2), `locus` = "3-69897-T-C")
    )
  }
}
