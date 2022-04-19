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
import java.sql.Date

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
    NormalizedSNV(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    NormalizedSNV(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father"))
  ).toDF
  val normalized_variantsDf: DataFrame = Seq(NormalizedVariants()).toDF()
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
      DONORS(1, 2, List(0, 1), 8.07, true,List("PASS"),0,1,1,1.0,"HET","chr1:g.69897T>C","SNV","BAT1","SR0095","14-696","SP_696",Date.valueOf("2022-04-06"),"germline","PA0001","FM00001","PPR00101","OR00201","WXS","11111","MM_PG","Maladies musculaires (Panel global)","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("mother"),Some("AD")),
      DONORS(1, 2, List(0, 1), 8.07, true,List("PASS"),0,1,1,1.0,"HET","chr1:g.69897T>C","SNV","BAT1","SR0095","14-696","SP_696",Date.valueOf("2022-04-06"),"germline","PA0002","FM00001","PPR00101","OR00202","WXS","11111","MM_PG","Maladies musculaires (Panel global)","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("father"),Some("AR"))
      //DONORS(1, 2, List(0, 1), 8.07,List("PASS"),0,1,1,1.0,"HET","SR0095","14-696","SP_696","PA0001","WXS","11111", "MM_PG","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("mother"),Some("AD")),
      //DONORS(1, 2, List(0, 1), 8.07,List("PASS"),0,1,1,1.0,"HET","SR0095","14-696","SP_696","PA0002","WXS","11111", "MM_PG","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("father"),Some("AR"))
  )

  val expectedFrequencies = Map("MN-PG" -> Map("affected" -> Frequency(), "total" -> Frequency()))

  "variants job" should "aggregate frequencies from normalized_variants" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = true , `patient_id` = "1"),
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = false, `patient_id` = "2"),
      NormalizedSNV(`analysis_code` = "ACHO" ,`affected_status` = true , `patient_id` = "3"),
      NormalizedSNV(`analysis_code` = "ACHO" ,`affected_status` = true , `patient_id` = "4")
    ).toDF

    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `frequencies_by_analysis` = List(AnalysisCodeFrequencies(
          analysis_code = "MM_PG",
          analysis_display_name = "Maladies musculaires (Panel global)",
          affected =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
          non_affected = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          total =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          affected =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
          non_affected = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          total =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            analysis_code = "MM_PG",
            analysis_display_name = "Maladies musculaires (Panel global)",
            affected =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            non_affected = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            total =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            analysis_code = "ACHO",
            analysis_display_name = "Achondroplasia",
            affected =     Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
            non_affected = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            total =        Frequency(1, 4, 0.0, 1, 2, 0.5, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          affected =     Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
          non_affected = Frequency(2, 2, 1.0 , 1, 1, 1.0, 1),
          total =        Frequency(3, 6, 0.5 , 2, 3, 0.666666667, 1))),
    ).toDF()

    val expectedFrequencies = List(
      AnalysisCodeFrequencies(
        analysis_code = "MM_PG",
        analysis_display_name = "Maladies musculaires (Panel global)",
        affected =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        non_affected = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        total =        Frequency(2, 4, 0.5, 2, 2, 1.0, 0)),
      AnalysisCodeFrequencies(
        analysis_code = "ACHO",
        analysis_display_name = "Achondroplasia",
        affected =     Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
        non_affected = Frequency(0, 0, 0.0 , 0, 0, 0.0, 0),
        total =        Frequency(1, 4, 0.25, 1, 2, 0.5, 0))
    )

    val expectedRQDMFred = AnalysisFrequencies(
      affected =     Frequency(2, 6, 0.3333333333333333, 2, 3, 0.6666666666666666, 0),
      non_affected = Frequency(1, 2, 0.5               , 1, 1, 1.0 , 0),
      total =        Frequency(3, 8, 0.375             , 3, 4, 0.75, 0))

    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))
    val result = resultDf.as[VariantEnrichedOutput].collect().head

    result.`frequencies_by_analysis` should contain allElementsOf expectedFrequencies
    result.`frequency_RQDM` shouldBe expectedRQDMFred
  }

  "variants job" should "run" in {

    new Variants().run(RunStep.initial_load)

    val resultDf = spark.table("clin.variants")
    val result = resultDf.as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors
    result.`frequencies_by_analysis` should contain allElementsOf List(AnalysisCodeFrequencies())

    result.copy(
      `donors` = List(),
      `frequencies_by_analysis` = List()
    ) shouldBe VariantEnrichedOutput(
      `pubmed` = Some(List("29135816")),
      `donors` = List(),
      `frequencies_by_analysis` = List(),
      `frequency_RQDM` = AnalysisFrequencies(),
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`
    )
  }
}

