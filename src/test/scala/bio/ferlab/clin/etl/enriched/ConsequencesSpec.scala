package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.{EnrichedConsequences, Dbnsfp_originalOutput, EnsemblMappingOutput, ManeSummaryOutput, NormalizedConsequences}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("normalized_dbnsfp_original")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val normalized_mane_summary: DatasetConf = conf.getDataset("normalized_mane_summary")

  val data = Map(
    normalized_consequences.id -> Seq(NormalizedConsequences()).toDF(),
    dbnsfp_original.id -> Seq(Dbnsfp_originalOutput()).toDF,
    normalized_ensembl_mapping.id -> Seq(EnsemblMappingOutput()).toDF,
    normalized_mane_summary.id -> Seq(ManeSummaryOutput(`ensembl_transcript_id` = "ENST00000450305.12", `ensembl_gene_id` = "ENSG00000223972.3")).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(enriched_consequences.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val resultDf = new Consequences().transform(data)
    val result = resultDf.as[EnrichedConsequences].collect().head

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedConsequences", resultDf, "src/test/scala/")

    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }

  "consequences job" should "run" in {
    new Consequences().run()

    val result = enriched_consequences.read.as[EnrichedConsequences].collect().head
    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }
}


