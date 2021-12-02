package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.RunType.FIRST_LOAD
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

  val expectedFrequencies = Map("MN-PG" -> Map("affected" -> Frequency(), "total" -> Frequency()))

  "variants job" should "transform data in expected format" in {

    val result = new Variants("1").transform(data)
      .as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors

    result.copy(
      `frequencies_by_analysis` = Map(),
      `frequency_RQDM` = AnalysisFrequencies(),
      `donors` = List()) shouldBe
      VariantEnrichedOutput(
        `frequencies_by_analysis` = Map(),
        `frequency_RQDM` = AnalysisFrequencies(),
        `donors` = List(),
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`)
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

    val result = new Variants("1").transform(transmissionData)
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

  "variants job" should "compute frequencies by analysis" in {

    val occurrencesDf = Seq(
      OccurrenceRawOutput(patient_id = "PA0001", analysis_code = "ID"  , filters = List("PASS"), calls = List(0, 1)    , zygosity = "HET", affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0002", analysis_code = "ID"  , filters = List("PASS"), calls = List(1, 1)    , zygosity = "HOM", affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0003", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0)    , zygosity = "WT" , affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0004", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0)    , zygosity = "WT" , affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0005", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0)    , zygosity = "WT" , affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0006", analysis_code = "MMPG", filters = List("PASS"), calls = List(1, 1)    , zygosity = "HOM", affected_status = true),
      OccurrenceRawOutput(patient_id = "PA0007", analysis_code = "MMPG", filters = List()      , calls = List(0, 1)    , zygosity = "HET", affected_status = false),
      OccurrenceRawOutput(patient_id = "PA0008", analysis_code = "ID"  , filters = List("PASS"), calls = List(-1, -1)  , zygosity = "UNK", affected_status = false),
      OccurrenceRawOutput(patient_id = "PA0009", analysis_code = "ID"  , filters = List("PASS"), calls = List(-1, -1)  , zygosity = "UNK", affected_status = true)
    ).toDF()

    val inputData = data ++ Map(normalized_occurrences.id -> occurrencesDf)
    val df = new Variants("").transform(inputData)
    val result = df.as[VariantEnrichedOutput].collect().head

    result.`donors`.length shouldBe 3

    result.`frequencies_by_analysis` shouldBe Map(
      "ID" -> Map(
        "total" -> Frequency(3, 4, 0.75, 2, 4, 0.5, 1),
        "non_affected" -> Frequency(0, 0, 0.0, 0, 1, 0.0, 0),
        "affected" -> Frequency(3, 4, 0.75, 2, 3, 0.6666666666666666, 1)),
      "MMPG" -> Map(
        "total" -> Frequency(2, 8, 0.25, 1, 4, 0.25, 1),
        "non_affected" -> Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        "affected" -> Frequency(2, 8, 0.25, 1, 4, 0.25, 1))
    )

    result.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(5, 12, 0.4166666666666667, 3, 7, 0.42857142857142855, 2),
      Frequency(0, 0, 0.0, 0, 1, 0.0, 0),
      Frequency(5, 12, 0.4166666666666667, 3, 8, 0.375, 2)
    )
  }

  "variants job" should "run" in {

    new Variants("1").run(FIRST_LOAD)

    val result = spark.table("clin.variants")
      .as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors

    result.copy(
      `donors` = List(),
      `frequencies_by_analysis` = Map()
    ) shouldBe VariantEnrichedOutput(
      `donors` = List(),
      `frequencies_by_analysis` = Map(),
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`)
  }
}

