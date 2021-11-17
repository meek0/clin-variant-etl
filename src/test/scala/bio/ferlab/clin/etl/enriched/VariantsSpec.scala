package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  val normalized_occurrencesDf: DataFrame = Seq(
    OccurrenceRawOutput(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    OccurrenceRawOutput(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father"))
  ).toDF
  val normalized_variantsDf: DataFrame = Seq(VariantRawOutput()).toDF()
  val genomesDf: DataFrame = Seq(OneKGenomesOutput()).toDF
  val topmed_bravoDf: DataFrame = Seq(Topmed_bravoOutput()).toDF
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(GnomadGenomes211Output()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(GnomadExomes211Output()).toDF
  val gnomad_genomes_3_0Df: DataFrame = Seq(GnomadGenomes30Output()).toDF
  val gnomad_genomes_3_1_1Df: DataFrame = Seq(GnomadGenomes311Output()).toDF
  val dbsnpDf: DataFrame = Seq(DbsnpOutput()).toDF
  val clinvarDf: DataFrame = Seq(ClinvarOutput()).toDF
  val genesDf: DataFrame = Seq(GenesOutput()).toDF()

  val data = Map(
    normalized_variants.id -> normalized_variantsDf,
    normalized_occurrences.id -> normalized_occurrencesDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_3_0.id -> gnomad_genomes_3_0Df,
    gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(enriched_variants.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_normalized")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  val expectedDonors =
    List(
      DONORS(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
      DONORS(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father"))
    )

  "variants job" should "transform data in expected format" in {

    val result = new Variants("1", "first_load").transform(data)
      .as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = expectedDonors,
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`,
      `participant_total_number` = 2,
      `participant_number` = 2,
      `participant_frequency` = 1.0)
  }

  "variants job" should "aggregate transmissions and parental origin per lab" in {
    val occurrences = Seq(
      OccurrenceRawOutput(`patient_id` = "PA0001", `transmission` = None      , `parental_origin` = Some("mother"), `organization_id` = "OG2"),
      OccurrenceRawOutput(`patient_id` = "PA0002", `transmission` = Some("AD"), `parental_origin` = Some("father"), `organization_id` = "OG2"),
      OccurrenceRawOutput(`patient_id` = "PA0003", `transmission` = Some("AR"), `parental_origin` = Some("father"), `organization_id` = "OG2"),
      OccurrenceRawOutput(`patient_id` = "PA0004", `transmission` = Some("AR"), `parental_origin` = Some("father"), `organization_id` = "OG2"),
      OccurrenceRawOutput(`patient_id` = "PA0005", `transmission` = Some("AR"), `parental_origin` = Some("mother"), `organization_id` = "OG2"),
      OccurrenceRawOutput(`patient_id` = "PA0006", `transmission` = Some("AR"), `parental_origin` = None          , `organization_id` = "OG1"),
      OccurrenceRawOutput(`patient_id` = "PA0007", `transmission` = Some("AR"), `parental_origin` = Some("father"), `organization_id` = "OG1")
    )

    val transmissionData = data + (normalized_occurrences.id -> occurrences.toDF())

    val result = new Variants("1", "first_load").transform(transmissionData)
      .as[VariantEnrichedOutput].collect().head

    result.`parental_origins` shouldBe Map(
      "mother" -> 2,
      "father" -> 4
    )

    result.`parental_origins_by_lab` shouldBe Map(
      "OG2" -> Map("mother" -> 2, "father" -> 3),
      "OG1" -> Map("father" -> 1)
    )

    result.`transmissions` shouldBe Map(
      "AR" -> 5,
      "AD" -> 1
    )

    result.`transmissions_by_lab` shouldBe Map(
      "OG2" -> Map("AR" -> 3, "AD" -> 1),
      "OG1" -> Map("AR" -> 2)
    )
  }

  "variants job" should "run" in {

    new Variants("1", "first_load").run()

    val result = spark.table("clin.variants").as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = expectedDonors,
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`,
      `participant_total_number` = 2,
      `participant_number` = 2,
      `participant_frequency` = 1.0)
  }
}

