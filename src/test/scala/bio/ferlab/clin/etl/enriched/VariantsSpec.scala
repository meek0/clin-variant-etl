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
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val varsome: DatasetConf = conf.getDataset("normalized_varsome")

  val normalized_occurrencesDf: DataFrame = Seq(
    SNVRawOutput(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    SNVRawOutput(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father"))
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
  val normalized_panelsDf: DataFrame = Seq(PanelOutput()).toDF()
  val varsomeDf: DataFrame = Seq(VarsomeOutput()).toDF()

  val data = Map(
    normalized_variants.id -> normalized_variantsDf,
    normalized_snv.id -> normalized_occurrencesDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_3_0.id -> gnomad_genomes_3_0Df,
    gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf,
    normalized_panels.id -> normalized_panelsDf,
    varsome.id -> varsomeDf
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

    val result = new Variants().transform(data)
      .as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors

    result.copy(
      `frequencies_by_analysis` = List(),
      `frequency_RQDM` = AnalysisFrequencies(),
      `donors` = List()) shouldBe
      VariantEnrichedOutput(
        `frequencies_by_analysis` = List(),
        `frequency_RQDM` = AnalysisFrequencies(),
        `donors` = List(),
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`)
  }

  "variants job" should "compute frequencies by analysis" in {

    val occurrencesDf = Seq(
      SNVRawOutput(patient_id = "PA0001", analysis_display_name = "Intel Disorder", analysis_code = "ID", filters = List("PASS"), calls = List(0, 1), zygosity = "HET", affected_status = true),
      SNVRawOutput(patient_id = "PA0002", analysis_display_name = "Intel Disorder", analysis_code = "ID", filters = List("PASS"), calls = List(1, 1), zygosity = "HOM", affected_status = true),
      SNVRawOutput(patient_id = "PA0003", analysis_display_name = "Maladies muscu", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0), zygosity = "WT", affected_status = true),
      SNVRawOutput(patient_id = "PA0004", analysis_display_name = "Maladies muscu", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0), zygosity = "WT", affected_status = true),
      SNVRawOutput(patient_id = "PA0005", analysis_display_name = "Maladies muscu", analysis_code = "MMPG", filters = List("PASS"), calls = List(0, 0), zygosity = "WT", affected_status = true),
      SNVRawOutput(patient_id = "PA0006", analysis_display_name = "Maladies muscu", analysis_code = "MMPG", filters = List("PASS"), calls = List(1, 1), zygosity = "HOM", affected_status = true),
      SNVRawOutput(patient_id = "PA0007", analysis_display_name = "Maladies muscu", analysis_code = "MMPG", filters = List("LowDepth"), calls = List(0, 1), zygosity = "HET", affected_status = false),
      SNVRawOutput(patient_id = "PA0008", analysis_display_name = "Intel Disorder", analysis_code = "ID", filters = List("PASS"), calls = List(-1, -1), zygosity = "UNK", affected_status = false),
      SNVRawOutput(patient_id = "PA0009", analysis_display_name = "Intel Disorder", analysis_code = "ID", filters = List("PASS"), calls = List(-1, -1), zygosity = "UNK", affected_status = true)
    ).toDF()

    val inputData = data ++ Map(normalized_snv.id -> occurrencesDf)
    val df = new Variants().transform(inputData)
    val result = df.as[VariantEnrichedOutput].collect().head

    result.`donors`.length shouldBe 4

    result.`frequencies_by_analysis` shouldBe List(
      AnalysisCodeFrequencies(
        analysis_code = "ID",
        analysis_display_name = "Intel Disorder",
        affected = Frequency(3, 4, 0.75, 2, 3, 0.6666666666666666, 1),
        non_affected = Frequency(0, 0, 0.0, 0, 1, 0.0, 0),
        total = Frequency(3, 4, 0.75, 2, 4, 0.5, 1)
      ),
      AnalysisCodeFrequencies(
        analysis_code = "MMPG",
        analysis_display_name = "Maladies muscu",
        affected = Frequency(2, 8, 0.25, 1, 4, 0.25, 1),
        non_affected = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        total = Frequency(2, 8, 0.25, 1, 4, 0.25, 1)
      ))

    result.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(5, 12, 0.4166666666666667, 3, 7, 0.42857142857142855, 2),
      Frequency(0, 0, 0.0, 0, 1, 0.0, 0),
      Frequency(5, 12, 0.4166666666666667, 3, 8, 0.375, 2)
    )
  }

  "variants job" should "run" in {

    new Variants().run(RunStep.initial_load)

    val result = spark.table("clin.variants")
      .as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors

    result.copy(
      `donors` = List(),
      `frequencies_by_analysis` = List()
    ) shouldBe VariantEnrichedOutput(
      `donors` = List(),
      `frequencies_by_analysis` = List(),
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`)
  }
}

