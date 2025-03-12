package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants._
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched._
import bio.ferlab.clin.model.normalized.{GENES, SPLICEAI, _}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.models.enriched.EnrichedVariant.CMC
import bio.ferlab.datalake.testutils.models.normalized.NormalizedCosmicMutationSet
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll

import java.sql.Date

class VariantsSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll with BeforeAndAfterAll {

  import spark.implicits._

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val snv: DatasetConf = conf.getDataset("enriched_snv")
  val snv_somatic: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val gnomad_genomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  val gnomad_exomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  val gnomad_joint_v4: DatasetConf = conf.getDataset("normalized_gnomad_joint_v4")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")
  val franklin: DatasetConf = conf.getDataset("normalized_franklin")

  val job = Variants(TestETLContext(RunStep.initial_load))
  override val dbToCreate: List[String] = List("clin", "clin_normalized")
  override val dsToClean: List[DatasetConf] = List(snv, enriched_variants, genes)

  val occurrencesDf: DataFrame = Seq(
    EnrichedSNV(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    EnrichedSNV(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father")),
    EnrichedSNV(`patient_id` = "PA0003", `has_alt` = false, `zygosity` = "UNK", `calls` = List(0, 0))
  ).toDF
  val occurrencesSomaticDf: DataFrame = Seq(
    EnrichedSNVSomatic(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    EnrichedSNVSomatic(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father")),
    EnrichedSNVSomatic(`patient_id` = "PA0003", `has_alt` = false, `zygosity` = "UNK", `calls` = List(0, 0))
  ).toDF
  val normalized_variantsDf: DataFrame = Seq(NormalizedVariants()).toDF()
  val genomesDf: DataFrame = Seq(OneKGenomesOutput()).toDF
  val topmed_bravoDf: DataFrame = Seq(Topmed_bravoOutput()).toDF
  val gnomad_constraintDf: DataFrame = Seq(GnomadConstraintOutput()).toDF()
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(GnomadGenomes211Output()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(GnomadExomes211Output()).toDF
  val gnomad_genomes_3_0Df: DataFrame = Seq(GnomadGenomes30Output()).toDF
  val gnomad_genomes_3_1_1Df: DataFrame = Seq(GnomadGenomes311Output()).toDF
  val gnomad_joint_4Df: DataFrame = Seq(GnomadJoint4Output()).toDF
  val dbsnpDf: DataFrame = Seq(DbsnpOutput()).toDF
  val clinvarDf: DataFrame = Seq(ClinvarOutput()).toDF
  val genesDf: DataFrame = Seq(EnrichedGenes()).toDF()
  val normalized_panelsDf: DataFrame = Seq(NormalizedPanels()).toDF()
  val cosmicDf: DataFrame = Seq(NormalizedCosmicMutationSet(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF()
  val franklinDf: DataFrame = Seq(NormalizedFranklin(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF()

  val data = Map(
    normalized_variants.id -> normalized_variantsDf,
    snv.id -> occurrencesDf,
    snv_somatic.id -> occurrencesSomaticDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_constraint.id -> gnomad_constraintDf,
    gnomad_genomes_v2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_v2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_3_0.id -> gnomad_genomes_3_0Df,
    gnomad_genomes_v3.id -> gnomad_genomes_3_1_1Df,
    gnomad_joint_v4.id -> gnomad_joint_4Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf,
    normalized_panels.id -> normalized_panelsDf,
    cosmic.id -> cosmicDf,
    franklin.id -> franklinDf
  )

  val expectedDonors =
    List(
      DONORS(1, Some(30), None, List(0, 1), Some(8.07), Some(30), Some(8.07), Some(30), Some(8.07), true, List("PASS"), 0, 30, 30, 1.0, "HET", "chr1:g.69897T>C", "SNV", "BAT1", "SR0095", "14-696", "SP_696", Date.valueOf("2022-04-06"), "germline", "GEAN", "PA0001", "FM00001", "PPR00101", "OR00201", "WXS", "11111", "MM_PG", "Maladies musculaires (Panel global)", "PA0003", "PA0002", Some("33333"), Some("22222"), Some(List(0, 1)), Some(List(0, 0)), Some(true), Some(false), Some("HET"), Some("WT"), Some("mother"), Some("AD")),
      DONORS(1, Some(30), None, List(0, 1), Some(8.07), Some(30), Some(8.07), Some(30), Some(8.07), true, List("PASS"), 0, 30, 30, 1.0, "HET", "chr1:g.69897T>C", "SNV", "BAT1", "SR0095", "14-696", "SP_696", Date.valueOf("2022-04-06"), "germline", "GEAN", "PA0002", "FM00001", "PPR00101", "OR00202", "WXS", "11111", "MM_PG", "Maladies musculaires (Panel global)", "PA0003", "PA0002", Some("33333"), Some("22222"), Some(List(0, 1)), Some(List(0, 0)), Some(true), Some(false), Some("HET"), Some("WT"), Some("father"), Some("AR"))
    )

  "variants job" should "union of all available enriched SNV" in {
    val resultDf = job.transformSingle(data)
    val result = resultDf.as[EnrichedVariant].collect()
    result.length shouldBe 1
    result(0).`variant_type`.size shouldBe 2
  }

  "variants job" should "aggregate frequencies from normalized_variants" in {
    /*
    +---------------------------------------------------------------+
    |         Table of aggregation combinations (use cases)         |
    +--------+---------+---------+----------------+-------+---------+
    |Use case|#Variants|#Analysis|#Affected status|#Batchs|#Patients|
    +--------+---------+---------+----------------+-------+---------+
    |1 et 2  |1        |1        |1               |1      |1        |
    |3       |1        |1        |1               |1      |2        |
    |-       |1        |1        |1               |2      |1        |
    |4       |1        |1        |1               |2      |2        |
    |-       |1        |1        |2               |1      |1        |
    |5       |1        |1        |2               |1      |2        |
    |-       |1        |1        |2               |2      |1        |
    |6       |1        |1        |2               |2      |2        |
    |-       |1        |2        |1               |1      |1        |
    |7       |1        |2        |1               |1      |2        |
    |-       |1        |2        |1               |2      |1        |
    |8       |1        |2        |1               |2      |2        |
    |-       |1        |2        |2               |1      |1        |
    |9       |1        |2        |2               |1      |2        |
    |-       |1        |2        |2               |2      |1        |
    |10      |1        |2        |2               |2      |2        |
    |11      |2        |1        |1               |1      |1        |
    |12      |2        |1        |1               |1      |2        |
    |-       |2        |1        |1               |2      |1        |
    |13      |2        |1        |1               |2      |2        |
    |-       |2        |1        |2               |1      |1        |
    |14      |2        |1        |2               |1      |2        |
    |-       |2        |1        |2               |2      |1        |
    |15      |2        |1        |2               |2      |2        |
    |-       |2        |2        |1               |1      |1        |
    |16      |2        |2        |1               |1      |2        |
    |-       |2        |2        |1               |2      |1        |
    |17      |2        |2        |1               |2      |2        |
    |-       |2        |2        |2               |1      |1        |
    |18      |2        |2        |2               |1      |2        |
    |-       |2        |2        |2               |2      |1        |
    |19      |2        |2        |2               |2      |2        |
    +--------+---------+---------+----------------+-------+---------+
    */

    val occurrencesDf: DataFrame = Seq(
      EnrichedSNV(`analysis_code` = "UseCase01", `affected_status` = true, `patient_id` = "PA01", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 101),
      EnrichedSNV(`analysis_code` = "UseCase02", `affected_status` = true, `patient_id` = "PA02", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 102),
      EnrichedSNV(`analysis_code` = "UseCase03", `affected_status` = true, `patient_id` = "PA03", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 103),
      EnrichedSNV(`analysis_code` = "UseCase03", `affected_status` = true, `patient_id` = "PA04", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 103),
      EnrichedSNV(`analysis_code` = "UseCase04", `affected_status` = true, `patient_id` = "PA05", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 104),
      EnrichedSNV(`analysis_code` = "UseCase04", `affected_status` = true, `patient_id` = "PA06", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 104),
      EnrichedSNV(`analysis_code` = "UseCase05", `affected_status` = true, `patient_id` = "PA07", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 105),
      EnrichedSNV(`analysis_code` = "UseCase05", `affected_status` = false, `patient_id` = "PA08", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 105),
      EnrichedSNV(`analysis_code` = "UseCase06", `affected_status` = true, `patient_id` = "PA09", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 106),
      EnrichedSNV(`analysis_code` = "UseCase06", `affected_status` = false, `patient_id` = "PA10", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 106),
      EnrichedSNV(`analysis_code` = "UseCase07a", `affected_status` = true, `patient_id` = "PA11", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 107),
      EnrichedSNV(`analysis_code` = "UseCase07b", `affected_status` = true, `patient_id` = "PA12", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 107),
      EnrichedSNV(`analysis_code` = "UseCase08a", `affected_status` = true, `patient_id` = "PA13", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 108),
      EnrichedSNV(`analysis_code` = "UseCase08b", `affected_status` = true, `patient_id` = "PA14", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 108),
      EnrichedSNV(`analysis_code` = "UseCase09a", `affected_status` = true, `patient_id` = "PA15", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 109),
      EnrichedSNV(`analysis_code` = "UseCase09b", `affected_status` = false, `patient_id` = "PA16", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 109),
      EnrichedSNV(`analysis_code` = "UseCase10a", `affected_status` = true, `patient_id` = "PA17", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 110),
      EnrichedSNV(`analysis_code` = "UseCase10b", `affected_status` = false, `patient_id` = "PA18", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 110),
      EnrichedSNV(`analysis_code` = "UseCase11", `affected_status` = true, `patient_id` = "PA19", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 111),
      EnrichedSNV(`analysis_code` = "UseCase11", `affected_status` = true, `patient_id` = "PA19", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 211),
      EnrichedSNV(`analysis_code` = "UseCase12", `affected_status` = true, `patient_id` = "PA20", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 112),
      EnrichedSNV(`analysis_code` = "UseCase12", `affected_status` = true, `patient_id` = "PA21", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 212),
      EnrichedSNV(`analysis_code` = "UseCase13", `affected_status` = true, `patient_id` = "PA22", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 113),
      EnrichedSNV(`analysis_code` = "UseCase13", `affected_status` = true, `patient_id` = "PA23", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 213),
      EnrichedSNV(`analysis_code` = "UseCase14", `affected_status` = true, `patient_id` = "PA24", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 114),
      EnrichedSNV(`analysis_code` = "UseCase14", `affected_status` = false, `patient_id` = "PA25", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 214),
      EnrichedSNV(`analysis_code` = "UseCase15", `affected_status` = true, `patient_id` = "PA26", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 115),
      EnrichedSNV(`analysis_code` = "UseCase15", `affected_status` = false, `patient_id` = "PA27", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 215),
      EnrichedSNV(`analysis_code` = "UseCase16a", `affected_status` = true, `patient_id` = "PA28", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 116),
      EnrichedSNV(`analysis_code` = "UseCase16b", `affected_status` = true, `patient_id` = "PA29", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 216),
      EnrichedSNV(`analysis_code` = "UseCase17a", `affected_status` = true, `patient_id` = "PA30", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 117),
      EnrichedSNV(`analysis_code` = "UseCase17b", `affected_status` = true, `patient_id` = "PA31", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 217),
      EnrichedSNV(`analysis_code` = "UseCase18a", `affected_status` = true, `patient_id` = "PA32", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 118),
      EnrichedSNV(`analysis_code` = "UseCase18b", `affected_status` = false, `patient_id` = "PA33", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 218),
      EnrichedSNV(`analysis_code` = "UseCase19a", `affected_status` = true, `patient_id` = "PA34", `ad_alt` = 30, `batch_id` = "BAT1", `start` = 119),
      EnrichedSNV(`analysis_code` = "UseCase19b", `affected_status` = false, `patient_id` = "PA35", `ad_alt` = 30, `batch_id` = "BAT2", `start` = 219),
    ).toDF

    val occurrencesDfSomatic = Seq(
      EnrichedSNVSomatic(`analysis_code` = "UseCaseSomatic", `affected_status` = false, `patient_id` = "PA36", `ad_alt` = 30, `batch_id` = "BAT3", `start` = 219),
      EnrichedSNVSomatic(`analysis_code` = "UseCaseSomatic", `affected_status` = false, `patient_id` = "PA36", `ad_alt` = 30, `batch_id` = "BAT3", `start` = 301),
      EnrichedSNVSomatic(`analysis_code` = "UseCaseSomatic", `affected_status` = false, `patient_id` = "PA36", `ad_alt` = 30, `batch_id` = "BAT3", `start` = 302)
    ).toDF

    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase01",
            `analysis_display_name` = "Analysis for the use case 01",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 102,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase02",
            `analysis_display_name` = "Analysis for the use case 02",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 103,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase03",
            `analysis_display_name` = "Analysis for the use case 03",
            `affected` = Frequency(2, 4, 0.5, 2, 2, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 104,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase04",
            `analysis_display_name` = "Analysis for the use case 04",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 104,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase04",
            `analysis_display_name` = "Analysis for the use case 04",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 105,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase05",
            `analysis_display_name` = "Analysis for the use case 05",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 106,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase06",
            `analysis_display_name` = "Analysis for the use case 06",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 106,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase06",
            `analysis_display_name` = "Analysis for the use case 06",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 107,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase07a",
            `analysis_display_name` = "Analysis A for the use case 07",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase07b",
            `analysis_display_name` = "Analysis B for the use case 07",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 108,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase08a",
            `analysis_display_name` = "Analysis A for the use case 08",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 108,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase08b",
            `analysis_display_name` = "Analysis B for the use case 08",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 109,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase09a",
            `analysis_display_name` = "Analysis A for the use case 09",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase09b",
            `analysis_display_name` = "Analysis B for the use case 09",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 110,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase10a",
            `analysis_display_name` = "Analysis A for the use case 10",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 110,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase10b",
            `analysis_display_name` = "Analysis B for the use case 10",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 111,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase11",
            `analysis_display_name` = "Analysis for the use case 11",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 211,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase11",
            `analysis_display_name` = "Analysis for the use case 11",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 112,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase12",
            `analysis_display_name` = "Analysis for the use case 12",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 212,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase12",
            `analysis_display_name` = "Analysis for the use case 12",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 113,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase13",
            `analysis_display_name` = "Analysis for the use case 13",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 213,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase13",
            `analysis_display_name` = "Analysis for the use case 13",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 114,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase14",
            `analysis_display_name` = "Analysis for the use case 14",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 214,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase14",
            `analysis_display_name` = "Analysis for the use case 14",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 115,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase15",
            `analysis_display_name` = "Analysis for the use case 15",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 215,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase15",
            `analysis_display_name` = "Analysis for the use case 15",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 116,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase16a",
            `analysis_display_name` = "Analysis A for the use case 16",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 216,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase16b",
            `analysis_display_name` = "Analysis B for the use case 16",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 117,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase17a",
            `analysis_display_name` = "Analysis A for the use case 17",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 217,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase17b",
            `analysis_display_name` = "Analysis B for the use case 17",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 118,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase18a",
            `analysis_display_name` = "Analysis A for the use case 18",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 218,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase18b",
            `analysis_display_name` = "Analysis B for the use case 18",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 119,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase19a",
            `analysis_display_name` = "Analysis A for the use case 19",
            `affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 219,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCase19b",
            `analysis_display_name` = "Analysis B for the use case 19",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
            `total` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))),
      NormalizedVariants(
        `batch_id` = "BAT3",
        `start` = 219,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCaseSomatic",
            `analysis_display_name` = "Analysis Somatic with both germline and somatic",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          ))),
      normalized.NormalizedVariants(
        `batch_id` = "BAT3",
        `start` = 301,
        `frequency_RQDM` = null,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCaseSomatic",
            `analysis_display_name` = "Analysis Somatic with missing frequency_RQDM",
            `affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          ))),
      normalized.NormalizedVariants(
        `batch_id` = "BAT3",
        `start` = 302,
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` = Frequency(1, 0, 2.0, 0, 0, 3.0, 0),
          `non_affected` = Frequency(0, 5, 0.0, 0, 4, 0.0, 0),
          `total` = Frequency(0, 0, 0.0, 6, 0, 0.0, 7)
        ),
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "UseCaseSomatic",
            `analysis_display_name` = "Analysis Somatic with non-empty frequencies",
            `affected` = Frequency(1, 0, 2.0, 0, 0, 3.0, 0),
            `non_affected` = Frequency(0, 5, 0.0, 0, 4, 0.0, 0),
            `total` = Frequency(0, 0, 0.0, 6, 0, 0.0, 7)
          ))),
    ).toDF()

    val resultDf = job.transformSingle(data ++ Map(normalized_variants.id -> variantDf, snv.id -> occurrencesDf, snv_somatic.id -> occurrencesDfSomatic))
    val result = resultDf.as[EnrichedVariant].collect()

    // Use case #1: A variant is present in batch #1 and absent from batch #2
    val result101 = result.find(_.`start` == 101).head
    result101.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase01", "Analysis for the use case 01",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result101.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))

    // Use case #2: A variant is absent from batch #1 and present in batch #2
    val result102 = result.find(_.`start` == 102).head
    result102.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase02", "Analysis for the use case 02",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result102.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #3: See table above for aggregation characteristics for this use case
    val result103 = result.find(_.`start` == 103).head
    result103.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase03", "Analysis for the use case 03",
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))
    result103.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(2, 54, 0.037037037037037035, 2, 27, 0.07407407407407407, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #4: See table above for aggregation characteristics for this use case
    val result104 = result.find(_.`start` == 104).head
    result104.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase04", "Analysis for the use case 04",
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))
    result104.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(2, 54, 0.037037037037037035, 2, 27, 0.07407407407407407, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #5: See table above for aggregation characteristics for this use case
    val result105 = result.find(_.`start` == 105).head
    result105.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase05", "Analysis for the use case 05",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))
    result105.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #6: See table above for aggregation characteristics for this use case
    val result106 = result.find(_.`start` == 106).head
    result106.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase06", "Analysis for the use case 06",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(2, 4, 0.5, 2, 2, 1.0, 1)))
    result106.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #7: See table above for aggregation characteristics for this use case
    val result107 = result.find(_.`start` == 107).head
    result107.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase07a", "Analysis A for the use case 07",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
      AnalysisCodeFrequencies(
        "UseCase07b", "Analysis B for the use case 07",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result107.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(2, 54, 0.037037037037037035, 2, 27, 0.07407407407407407, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #8: See table above for aggregation characteristics for this use case
    val result108 = result.find(_.`start` == 108).head
    result108.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase08a", "Analysis A for the use case 08",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
      AnalysisCodeFrequencies(
        "UseCase08b", "Analysis B for the use case 08",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result108.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(2, 54, 0.037037037037037035, 2, 27, 0.07407407407407407, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #9: See table above for aggregation characteristics for this use case
    val result109 = result.find(_.`start` == 109).head
    result109.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase09a", "Analysis A for the use case 09",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
      AnalysisCodeFrequencies(
        "UseCase09b", "Analysis B for the use case 09",
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result109.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #10: See table above for aggregation characteristics for this use case
    val result110 = result.find(_.`start` == 110).head
    result110.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase10a", "Analysis A for the use case 10",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
      AnalysisCodeFrequencies(
        "UseCase10b", "Analysis B for the use case 10",
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result110.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(2, 70, 0.02857142857142857, 2, 35, 0.05714285714285714, 1))

    // Use case #11: See table above for aggregation characteristics for this use case
    val result111 = result.find(_.`start` == 111).head
    result111.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase11", "Analysis for the use case 11",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result111.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result211 = result.find(_.`start` == 211).head
    result211.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase11", "Analysis for the use case 11",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result211.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #12: See table above for aggregation characteristics for this use case
    val result112 = result.find(_.`start` == 112).head
    result112.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase12", "Analysis for the use case 12",
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0)))
    result112.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result212 = result.find(_.`start` == 212).head
    result212.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase12", "Analysis for the use case 12",
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1)))
    result212.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #13: See table above for aggregation characteristics for this use case
    val result113 = result.find(_.`start` == 113).head
    result113.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase13", "Analysis for the use case 13",
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0)))
    result113.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result213 = result.find(_.`start` == 213).head
    result213.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase13", "Analysis for the use case 13",
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1)))
    result213.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #14: See table above for aggregation characteristics for this use case
    val result114 = result.find(_.`start` == 114).head
    result114.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase14", "Analysis for the use case 14",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0)))
    result114.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result214 = result.find(_.`start` == 214).head
    result214.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase14", "Analysis for the use case 14",
        Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1)))
    result214.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 54, 0.0, 0, 27, 0.0, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #15: See table above for aggregation characteristics for this use case
    val result115 = result.find(_.`start` == 115).head
    result115.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase15", "Analysis for the use case 15",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 0)))
    result115.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result215 = result.find(_.`start` == 215).head
    result215.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase15", "Analysis for the use case 15",
        Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 4, 0.25, 1, 2, 0.5, 1)))
    result215.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 54, 0.0, 0, 27, 0.0, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #16: See table above for aggregation characteristics for this use case
    val result116 = result.find(_.`start` == 116).head
    result116.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase16a", "Analysis A for the use case 16",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result116.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result216 = result.find(_.`start` == 216).head
    result216.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase16b", "Analysis B for the use case 16",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result216.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #17: See table above for aggregation characteristics for this use case
    val result117 = result.find(_.`start` == 117).head
    result117.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase17a", "Analysis A for the use case 17",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result117.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result217 = result.find(_.`start` == 217).head
    result217.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase17b", "Analysis B for the use case 17",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result217.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 1),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #18: See table above for aggregation characteristics for this use case
    val result118 = result.find(_.`start` == 118).head
    result118.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase18a", "Analysis A for the use case 18",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result118.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))
    val result218 = result.find(_.`start` == 218).head
    result218.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase18b", "Analysis B for the use case 18",
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result218.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 54, 0.0, 0, 27, 0.0, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))

    // Use case #19: See table above for aggregation characteristics for this use case
    val result119 = result.find(_.`start` == 119).head
    result119.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase19a", "Analysis A for the use case 19",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result119.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 54, 0.018518518518518517, 1, 27, 0.037037037037037035, 0),
      Frequency(0, 16, 0.0, 0, 8, 0.0, 0),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 0))

    val result219 = result.find(_.`start` == 219).head
    result219.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "UseCase19b", "Analysis B for the use case 19",
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 1)))
    result219.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 54, 0.0, 0, 27, 0.0, 0),
      Frequency(1, 16, 0.0625, 1, 8, 0.125, 1),
      Frequency(1, 70, 0.014285714285714285, 1, 35, 0.02857142857142857, 1))
    // is both germline and somatic_tumor_only
    result219.`variant_type` should contain allElementsOf List("germline", "somatic")

    // should have empty frequencies
    val result301 = result.find(_.`start` == 301).head
    result301.`frequencies_by_analysis`.size shouldBe 0
    result301.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0))
    result301.`variant_type` should contain allElementsOf List("somatic")

    // should have empty frequencies
    val result302 = result.find(_.`start` == 302).head
    result302.`frequencies_by_analysis`.size shouldBe 0
    result302.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
      Frequency(0, 0, 0.0, 0, 0, 0.0, 0))
    result302.`variant_type` should contain allElementsOf List("somatic")
  }

  "variants job" should "transform to EnrichedVariant" in {
    val resultDf = job.transformSingle(data = data)
    val result = resultDf.as[EnrichedVariant].collect().head

    //    resultDf.select(explode($"donors").as[DONORS]).show(false)
    //    expectedDonors.toDF().show(false)

    result.`donors` should contain allElementsOf expectedDonors
    result.`frequencies_by_analysis` should contain allElementsOf List(AnalysisCodeFrequencies(
      `affected` = Frequency(4, 6, 0.6666666666666666, 2, 3, 0.6666666666666666, 2),
      `total` = Frequency(4, 6, 0.6666666666666666, 2, 3, 0.6666666666666666, 2)))

    result.copy(
      `donors` = List(),
      `frequencies_by_analysis` = List()
    ) shouldBe enriched.EnrichedVariant(
      `pubmed` = Some(List("29135816")),
      `donors` = List(),
      `frequencies_by_analysis` = List(),
      `frequency_RQDM` = AnalysisFrequencies(
        `affected` = Frequency(4, 6, 0.6666666666666666, 2, 3, 0.6666666666666666, 2),
        `total` = Frequency(4, 6, 0.6666666666666666, 2, 3, 0.6666666666666666, 2)),
      `freq_rqdm_tumor_only` = SOMATIC_FREQUENCY(1, 1, 1.0),
      `freq_rqdm_tumor_normal` = SOMATIC_FREQUENCY(0, 0, 0.0),
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`
    )
  }

  "variants job" should "prioritize hotspot true values" in {
    val normalizedVariantsDf = Seq(
      // Only true
      NormalizedVariants(`batch_id` = "BAT1", `start` = 101, `hotspot` = Some(true)),
      // True over false
      NormalizedVariants(`batch_id` = "BAT1", `start` = 102, `hotspot` = Some(true)),
      NormalizedVariants(`batch_id` = "BAT2", `start` = 102, `hotspot` = Some(false)),
      // True over false and null
      NormalizedVariants(`batch_id` = "BAT1", `start` = 103, `hotspot` = Some(false)),
      NormalizedVariants(`batch_id` = "BAT2", `start` = 103, `hotspot` = None),
      NormalizedVariants(`batch_id` = "BAT3", `start` = 103, `hotspot` = Some(true)),
      // False over null
      NormalizedVariants(`batch_id` = "BAT1", `start` = 104, `hotspot` = None),
      NormalizedVariants(`batch_id` = "BAT2", `start` = 104, `hotspot` = Some(false)),
    ).toDF()

    val snvDf = Seq(
      EnrichedSNV(`batch_id` = "BAT2", `start` = 103),
      EnrichedSNV(`batch_id` = "BAT1", `start` = 104),
    ).toDF()

    val SNVSomaticDf = Seq(
      EnrichedSNVSomatic(`batch_id` = "BAT1", `start` = 101),
      EnrichedSNVSomatic(`batch_id` = "BAT1", `start` = 102),
      EnrichedSNVSomatic(`batch_id` = "BAT2", `start` = 102),
      EnrichedSNVSomatic(`batch_id` = "BAT1", `start` = 103),
      EnrichedSNVSomatic(`batch_id` = "BAT3", `start` = 103),
      EnrichedSNVSomatic(`batch_id` = "BAT2", `start` = 104),
    ).toDF()

    val testData = data ++ Map(
      normalized_variants.id -> normalizedVariantsDf,
      snv.id -> snvDf,
      snv_somatic.id -> SNVSomaticDf,
    )
    val resultDf = job.transformSingle(testData).select("start", "hotspot")
    val result = resultDf.as[(Long, Boolean)].collect()

    result should contain theSameElementsAs Seq(
      (101, true),
      (102, true),
      (103, true),
      (104, false),
    )
  }

  "variantsWithDonors" should "enrich variants with donor info" in {
    val variants = Seq(
      EnrichedVariant(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C"),
      EnrichedVariant(chromosome = "1", start = 2, end = 3, reference = "G", alternate = "A"),
      EnrichedVariant(chromosome = "1", start = 3, end = 4, reference = "C", alternate = "T"),
    ).toDF()

    // Remove donor from variants df
    val variantsWithoutDonors = variants.drop("donors", "variant_type")

    val germlineOccurrences = Seq(
      EnrichedSNV(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "11111", variant_type = "germline"),
      EnrichedSNV(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "22222", variant_type = "germline"),

      EnrichedSNV(chromosome = "1", start = 2, end = 3, reference = "G", alternate = "A", aliquot_id = "11111", variant_type = "germline"),
    ).toDF()

    val somaticOccurrences = Seq(
      EnrichedSNVSomatic(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "33333", variant_type = "somatic", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "33333", variant_type = "somatic", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),

      EnrichedSNVSomatic(chromosome = "1", start = 3, end = 4, reference = "C", alternate = "T", aliquot_id = "11111", variant_type = "somatic", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO"))
    ).toDF()

    // Mimic what is done in transformSingle method before calling variantsWithDonors()
    val allOccurrences = germlineOccurrences
      .unionByName(somaticOccurrences, allowMissingColumns = true)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")


    val result = job.variantsWithDonors(variantsWithoutDonors, allOccurrences)

    val expected = Seq(
      EnrichedVariant(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", variant_type = Set("germline", "somatic"),
        donors = List(
          DONORS(aliquot_id = "11111", variant_type = "germline", bioinfo_analysis_code = "GEAN", all_analyses = None),
          DONORS(aliquot_id = "22222", variant_type = "germline", bioinfo_analysis_code = "GEAN", all_analyses = None),
          DONORS(aliquot_id = "33333", variant_type = "somatic", bioinfo_analysis_code = "TEBA", all_analyses = Some(Set("TO", "TN"))),
          DONORS(aliquot_id = "33333", variant_type = "somatic", bioinfo_analysis_code = "TNEBA", all_analyses = Some(Set("TO", "TN")))
        )),
      EnrichedVariant(chromosome = "1", start = 2, end = 3, reference = "G", alternate = "A", variant_type = Set("germline"),
        donors = List(
          DONORS(aliquot_id = "11111", variant_type = "germline", bioinfo_analysis_code = "GEAN", all_analyses = None)
        )),
      EnrichedVariant(chromosome = "1", start = 3, end = 4, reference = "C", alternate = "T", variant_type = Set("somatic"),
        donors = List(
          DONORS(aliquot_id = "11111", variant_type = "somatic", bioinfo_analysis_code = "TEBA", all_analyses = Some(Set("TO")))
        )),
    ).toDF()
      .selectLocus($"variant_type", $"donors.aliquot_id", $"donors.variant_type", $"donors.bioinfo_analysis_code", $"donors.all_analyses")
      .collect()

    result
      .selectLocus($"variant_type", $"donors.aliquot_id", $"donors.variant_type", $"donors.bioinfo_analysis_code", $"donors.all_analyses")
      .collect() should contain theSameElementsAs expected
  }

  "variantsWithDonors" should "not group donors by family id" in {
    val variants = Seq(
      EnrichedVariant(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C"),
    ).toDF()

    val variantsWithoutDonors = variants.drop("donors", "variant_type")

    val occurrences = Seq(
      EnrichedSNV(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "11111", variant_type = "germline", `family_id` = "FAM_0000"),
      EnrichedSNV(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", aliquot_id = "11112", variant_type = "germline", `family_id` = "FAM_0000"),
    ).toDF()

    val result = job.variantsWithDonors(variantsWithoutDonors, occurrences)

    val expected = Seq(
      EnrichedVariant(chromosome = "1", start = 1, end = 2, reference = "T", alternate = "C", variant_type = Set("germline"),
        donors = List(
          DONORS(aliquot_id = "11111", variant_type = "germline", `family_id` = "FAM_0000"),
          DONORS(aliquot_id = "11112", variant_type = "germline", `family_id` = "FAM_0000"))),
    ).toDF().selectLocus($"variant_type", $"donors.aliquot_id", $"donors.variant_type").collect()

    result
      .selectLocus($"variant_type", $"donors.aliquot_id", $"donors.variant_type")
      .collect() should contain theSameElementsAs expected
  }

  "withFranklin" should "enrich variants with Franklin scores" in {
    val variants = Seq(
      EnrichedVariant(`chromosome` = "1", `donors` = List(
        DONORS(`aliquot_id` = "11111", `franklin_combined_score` = Some(0.5)),
        DONORS(`aliquot_id` = "22222", `franklin_combined_score` = Some(0.99)),
        DONORS(`aliquot_id` = "33333", `franklin_combined_score` = Some(0.15)),
        DONORS(`aliquot_id` = "44444", `franklin_combined_score` = None))), // No Franklin data for this donor
      EnrichedVariant(`chromosome` = "2", `donors` = List(DONORS(`aliquot_id` = "11111", `franklin_combined_score` = Some(0.6)))),
      EnrichedVariant(`chromosome` = "3", `donors` = List(DONORS(`aliquot_id` = "22222", `franklin_combined_score` = None))), // No Franklin data for this variant
    ).toDF().drop("franklin_max")

    val franklin = Seq(
      NormalizedFranklin(`chromosome` = "1", `start` = 69897, `end` = 69898, `reference` = "T", `alternate` = "C", `aliquot_id` = Some("11111"), `score` = 0.5, `acmg_classification` = "PATHOGENIC", `acmg_evidence` = Set("PS1", "PS2")),
      NormalizedFranklin(`chromosome` = "1", `start` = 69897, `end` = 69898, `reference` = "T", `alternate` = "C", `aliquot_id` = Some("22222"), `score` = 0.99, `acmg_classification` = "PATHOGENIC", `acmg_evidence` = Set("PS1", "PS2")),
      NormalizedFranklin(`chromosome` = "1", `start` = 69897, `end` = 69898, `reference` = "T", `alternate` = "C", `aliquot_id` = Some("33333"), `score` = 0.15, `acmg_classification` = "PATHOGENIC", `acmg_evidence` = Set("PS1", "PS2")),
      NormalizedFranklin(`chromosome` = "2", `start` = 69897, `end` = 69898, `reference` = "T", `alternate` = "C", `aliquot_id` = Some("11111"), `score` = 0.6, `acmg_classification` = "BENIGN", `acmg_evidence` = Set("PS1", "PM2", "PVS1")),
    ).toDF()

    val result = variants.withFranklin(franklin)

    val expected = Seq(
      EnrichedVariant(`chromosome` = "1", `franklin_max` = Some(FRANKLIN_MAX(`acmg_classification` = "PATHOGENIC", `acmg_evidence` = Set("PS1", "PS2"), `combined_score` = 0.99))),
      EnrichedVariant(`chromosome` = "2", `franklin_max` = Some(FRANKLIN_MAX(`acmg_classification` = "BENIGN", `acmg_evidence` = Set("PS1", "PM2", "PVS1"), `combined_score` = 0.6))),
      EnrichedVariant(`chromosome` = "3", `franklin_max` = None),
    ).toDF().selectLocus($"franklin_max").collect()

    result
      .selectLocus($"franklin_max")
      .collect() should contain theSameElementsAs expected
  }

  "withClinVariantExternalReference" should "add all external references to the variant" in {
    val variants = Seq(
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", `rsnumber` = "rs200676709", `pubmed` = Some(List("29135816")), `clinvar` = CLINVAR(), `cmc` = CMC(), `franklin_max` = Some(FRANKLIN_MAX())),
      EnrichedVariant(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T",  `rsnumber` = null, `pubmed` = None, `clinvar` = CLINVAR(), `cmc` = CMC(), `franklin_max` = Some(FRANKLIN_MAX()), `external_frequencies` = FREQUENCIES(gnomad_genomes_4 = null, gnomad_exomes_4 = null, gnomad_joint_4 = null)),
      EnrichedVariant(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", `rsnumber` = null, `pubmed` = None, `clinvar` = null, `cmc` = null, `franklin_max` = None, `external_frequencies` = FREQUENCIES(gnomad_genomes_4 = null, gnomad_exomes_4 = null, gnomad_joint_4 = null)),
    ).toDF().drop("variant_external_reference")

    val result = variants.withClinVariantExternalReference

    val expected = Seq(
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", `variant_external_reference` = Set("DBSNP", "PubMed", "Clinvar", "Cosmic", "Franklin", "gnomAD"), `rsnumber` = "rs200676709", `pubmed` = Some(List("29135816")), `clinvar` = CLINVAR(), `cmc` = CMC(), `franklin_max` = Some(FRANKLIN_MAX())),
      EnrichedVariant(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", `variant_external_reference` = Set("Clinvar", "Cosmic", "Franklin"), `rsnumber` = null, `pubmed` = None, `clinvar` = CLINVAR(), `cmc` = CMC(), `franklin_max` = Some(FRANKLIN_MAX()), `external_frequencies` = FREQUENCIES(gnomad_genomes_4 = null, gnomad_exomes_4 = null, gnomad_joint_4 = null)),
      EnrichedVariant(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", `variant_external_reference` = Set(), `rsnumber` = null, `pubmed` = None, `clinvar` = null, `cmc` = null, `franklin_max` = None, `external_frequencies` = FREQUENCIES(gnomad_genomes_4 = null, gnomad_exomes_4 = null, gnomad_joint_4 = null)),
    )

    result
      .as[EnrichedVariant]
      .collect() should contain theSameElementsAs expected
  }

  "withExomiser" should "enrich variants with max_exomiser field" in {
    val variants = Seq(
      EnrichedVariant(chromosome = "1"),
      EnrichedVariant(chromosome = "2"),
      EnrichedVariant(chromosome = "3")
    ).toDF().drop("exomiser_max")

    val donors = Seq(
      EnrichedSNV(chromosome = "1", aliquot_id = "11111", `exomiser` = Some(EXOMISER(`gene_combined_score` = 0.5f, `variant_score` = 0.75f))),
      EnrichedSNV(chromosome = "1", aliquot_id = "22222", `exomiser` = Some(EXOMISER(`gene_combined_score` = 0.99f, `variant_score` = 0.75f))),

      EnrichedSNV(chromosome = "2", aliquot_id = "11111", `exomiser` = Some(EXOMISER(`gene_combined_score` = 0.856f, `variant_score` = 1f))),
      EnrichedSNV(chromosome = "2", aliquot_id = "33333", `exomiser` = None),

      EnrichedSNV(chromosome = "3", aliquot_id = "33333", `exomiser` = None)
    ).toDF()

    val result = variants.withExomiser(donors)

    val expected = Seq(
      EnrichedVariant(chromosome = "1", exomiser_max = Some(EXOMISER_MAX(`gene_combined_score` = 0.99f, `variant_score` = 0.75f))),
      EnrichedVariant(chromosome = "2", exomiser_max = Some(EXOMISER_MAX(`gene_combined_score` = 0.856f, `variant_score` = 1f))),
      EnrichedVariant(chromosome = "3", `exomiser_max` = None)
    ).toDF().selectLocus($"exomiser_max").collect()

    result
      .selectLocus($"exomiser_max")
      .collect() should contain theSameElementsAs expected
  }

  "withSomaticFrequencies" should "enrich variants with TEBA and TNEBA frequencies" in {
    val occurrencesSnv = Seq(
      EnrichedSNV(`patient_id` = "1", `sample_id` = "1", chromosome = "1", `bioinfo_analysis_code` = "GEAN"), // Excluded because germline
      EnrichedSNV(`patient_id` = "1", `sample_id` = "1", chromosome = "3", `bioinfo_analysis_code` = "GEAN"), // Excluded because germline
    ).toDF()

    val occurrencesSnvSomatic = Seq(
      EnrichedSNVSomatic(`patient_id` = "1", `sample_id` = "1", chromosome = "1", `bioinfo_analysis_code` = "TEBA"),
      EnrichedSNVSomatic(`patient_id` = "1", `sample_id` = "1", chromosome = "1", `bioinfo_analysis_code` = "TNEBA"),
      EnrichedSNVSomatic(`patient_id` = "2", `sample_id` = "2", chromosome = "1", `bioinfo_analysis_code` = "TEBA", `ad_alt` = 1), // Excluded because ad_alt < 2
      EnrichedSNVSomatic(`patient_id` = "1", `sample_id` = "1", chromosome = "2", `bioinfo_analysis_code` = "TEBA"),
      EnrichedSNVSomatic(`patient_id` = "2", `sample_id` = "22", chromosome = "2", `bioinfo_analysis_code` = "TEBA"), // Patient 2 with two sample ids
      EnrichedSNVSomatic(`patient_id` = "3", `sample_id` = "3", chromosome = "2", `bioinfo_analysis_code` = "TNEBA"),
    ).toDF()

    val occurrencesAll = occurrencesSnv.unionByName(occurrencesSnvSomatic, allowMissingColumns = true)

    val variants = Seq(
      EnrichedVariant(`chromosome` = "1"),
      EnrichedVariant(`chromosome` = "2"),
      EnrichedVariant(`chromosome` = "3")
    ).toDF().drop("freq_rqdm_tumor_only", "freq_rqdm_tumor_normal")

    val result = job.joinWithSomaticFrequencies(variants, occurrencesAll)

    val expected = Seq(
      EnrichedVariant(`chromosome` = "1",
        `freq_rqdm_tumor_only` = SOMATIC_FREQUENCY(pc = 1, pn = 3, pf = 0.3333333333333333),
        `freq_rqdm_tumor_normal` = SOMATIC_FREQUENCY(pc = 1, pn = 2, pf = 0.5)),
      EnrichedVariant(`chromosome` = "2",
        `freq_rqdm_tumor_only` = SOMATIC_FREQUENCY(pc = 2, pn = 3, pf = 0.6666666666666666),
        `freq_rqdm_tumor_normal` = SOMATIC_FREQUENCY(pc = 1, pn = 2, pf = 0.5)),
      EnrichedVariant(`chromosome` = "3",
        `freq_rqdm_tumor_only` = SOMATIC_FREQUENCY(pc = 0, pn = 3, pf = 0.0),
        `freq_rqdm_tumor_normal` = SOMATIC_FREQUENCY(pc = 0, pn = 2, pf = 0.0))
    ).toDF().selectLocus($"freq_rqdm_tumor_only", $"freq_rqdm_tumor_normal").collect()

    result
      .selectLocus($"freq_rqdm_tumor_only", $"freq_rqdm_tumor_normal")
      .collect() should contain theSameElementsAs expected
  }

  "joinWithGenes" should "merge normalized_variants genes with enriched_genes table" in {
    val variants = Seq(
      NormalizedVariants(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", variant_class = "SNV", `genes_symbol` = List("gene1", "gene2"), `genes` = List(
        GENES(`symbol` = "gene1", `spliceai` = Some(SPLICEAI(`ds` = 2.0, `type` = Some(Seq("AL"))))),
        GENES(`symbol` = "gene2", `spliceai` = Some(SPLICEAI(`ds` = 0.0, `type` = None))),
      )),
      NormalizedVariants(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT", variant_class = "insertion", `genes_symbol` = List("gene1"), `genes` = List(GENES(`symbol` = "gene1", `spliceai` = Some(SPLICEAI(`ds` = 1.0, `type` = Some(Seq("AG", "AL"))))))),
      NormalizedVariants(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", variant_class = "SNV", `genes_symbol` = List("gene3"), `genes` = List(GENES(`symbol` = "gene3", `spliceai` = None))),
    ).toDF()

    val genes = Seq(
      EnrichedGenes(chromosome = "1", symbol = "gene1"),
      EnrichedGenes(chromosome = "1", symbol = "gene2"),
      EnrichedGenes(chromosome = "2", symbol = "gene3"),
    ).toDF()

    val result = job.joinWithGenes(variants, genes)

    result
      .selectLocus($"genes")
      .as[(String, Long, String, String, List[enriched.GENES])]
      .collect() should contain theSameElementsAs Seq(
      ("1", 1, "T", "C", List(
        enriched.GENES(`symbol` = Some("gene1"), `spliceai` = Some(enriched.SPLICEAI(`ds` = 2.0, `type` = Some(Seq("AL"))))),
        enriched.GENES(`symbol` = Some("gene2"), `spliceai` = Some(enriched.SPLICEAI(`ds` = 0.0, `type` = None))),
      )),
      ("1", 1, "T", "AT", List(enriched.GENES(`symbol` = Some("gene1"), `spliceai` = Some(enriched.SPLICEAI(`ds` = 1.0, `type` = Some(Seq("AG", "AL"))))))),
      ("2", 1, "A", "T", List(enriched.GENES(`symbol` = Some("gene3"), `spliceai` = None))),
    )
  }
}


object VariantsSpec {
  def removeNestedField(df: DataFrame, field: String, parent: String): DataFrame = {
    df.select(col("*"), explode_outer(col(parent)) as "temp")
      .withColumn("temp", col("temp").dropFields(field))
      .groupByLocus()
      .agg(
        first(struct(df.drop(parent)("*"))) as "df",
        collect_list("temp") as parent
      )
      .select("df.*", parent)
  }
}
