package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.VcfUtils.columns
import bio.ferlab.clin.etl.utils.VcfUtils.columns.ac
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.{LoadResolver, LoadType}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")
  val `1000_genomes`: DatasetConf = conf.getDataset("1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("gnomad_genomes_3_0")
  val dbsnp: DatasetConf = conf.getDataset("dbsnp")
  val clinvar: DatasetConf = conf.getDataset("clinvar")
  val genes: DatasetConf = conf.getDataset("genes")

  val normalized_occurrencesDf: DataFrame = Seq(OccurrenceRawOutput(), OccurrenceRawOutput(`organization_id` = "OR00202")).toDF
  val normalized_variantsDf: DataFrame = Seq(VariantRawOutput()).toDF()
  val genomesDf: DataFrame = Seq(OneKGenomesOutput()).toDF
  val topmed_bravoDf: DataFrame = Seq(Topmed_bravoOutput()).toDF
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(GnomadGenomes21Output()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(GnomadExomes21Output()).toDF
  val gnomad_genomes_3_0Df: DataFrame = Seq(GnomadGenomes30Output()).toDF
  val dbsnpDf: DataFrame = Seq(DbsnpOutput()).toDF
  val clinvarDf: DataFrame = Seq(ClinvarOutput()).toDF
  val genesDf: DataFrame = Seq(GenesOutput()).toDF()

  val data = Map(
    normalized_variants.id -> normalized_variantsDf,
    normalized_occurrences.id -> normalized_occurrencesDf,
    `1000_genomes`.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_3_0.id -> gnomad_genomes_3_0Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_normalized")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .resolve(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0)).toDF("calls")
    occurrences
      .select(
        ac,
        columns.an
      ).collect() should contain theSameElementsAs Seq(Row(3, 6))

  }

  "variants job" should "transform data in expected format" in {

    val result = new Variants("BAT0").transform(data)
      .as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = List(DONORS(), DONORS(`organization_id` = "OR00202")),
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }

  "variants job" should "run" in {

    new Variants("BAT0").run()

    val result = spark.table("clin.variants").as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = List(DONORS(), DONORS(`organization_id` = "OR00202")),
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }
}

