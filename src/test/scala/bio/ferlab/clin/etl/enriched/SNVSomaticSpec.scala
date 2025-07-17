package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.{EnrichedClinical, EnrichedSNVSomatic}
import bio.ferlab.clin.model.normalized.{NormalizedCNVSomaticTumorOnly, NormalizedSNVSomatic}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.clin.testutils.LoadResolverUtils.write
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}

import java.sql.Date

class SNVSomaticSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val enriched_snv_somatic: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job: Option[String] => SNVSomatic = batch => SNVSomatic(TestETLContext(), batch)


  /*
    Test scenario overview:

    We simulate a realistic situation where some batches have already been processed ("enriched") and others are new and need to be processed.
    - "Enriched" means the batch's analyses are already present in the `enriched_snv_somatic` table.
    - We suppose that the normalization step is done, so the data is available in `normalized_snv_somatic` and `normalized_cnv_somatic_tumor_only`.
    - Some analyses may appear in more than one batch because, for somatic cases (cancer), both tumor only and tumor normal data files are
      produced from the same sequencing. Due to technical and organizational constraints, these different data types are received in separate
      batches, but share the same analysis and sequencing identifiers.
    - As a result, the same analysis can appear in more than one batch, each with a different data type (bioinfo_analysis_code).
      When processing, both data types must be considered together to ensure the enriched data is complete and accurate. Some metrics require
      information from both types, while others are specific to either "tumor only" or "tumor normal" data, so we need to handle each case
      appropriately.
    - Both types of data must be processed together to ensure that the enriched data is complete and accurate,
      as some metrics require information from both data types. Some metric, however, are specific to one type
      of data (tumor only or tumor normal), so we need to handle them accordingly.

    Batches in this scenario:
      - BATCH1 (TEBA): 3 analyses already enriched (SRA1, SRA2, SRA3); CNV data available for analysis SRA2.
      - BATCH2 (TEBA): 1 analysis already enriched (SR4); CNV data available.
      - BATCH3 (TEBA): 2 analyses NOT yet enriched (SR5, SR6)
      - BATCH4 (TNEBA): 2 analyses NOT yet enriched (SR2, SR7), including one analysis (SRA2) also present in BATCH1.
      - BATCH5 (TNEBA): 1 analysis NOT yet enriched (SR8)

    This setup allows us to test:
      - How the ETL handles already enriched data (batch 1 and 2) vs. new data (batch 3, 4, 5).
      - How it manages analyses that appear in multiple batches (via batches 1 and 4, for analysis SRA2)
      - The integration of CNV data for cnv_count metric (analysis SR2 and SR4)
  */
  val existingClinicalData = Seq(
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA1", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "1", sequencing_id = "SR1"),
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA2", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "2", sequencing_id = "SR2"),
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA3", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "3", sequencing_id = "SR3"),
    EnrichedClinical(`batch_id` = "BATCH2", `analysis_id` = "SRA4", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "4", sequencing_id = "SR4"),
    EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA5", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "5", sequencing_id = "SR5"),
    EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA6", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "6", sequencing_id = "SR6"),
    EnrichedClinical(`batch_id` = "BATCH4", `analysis_id` = "SRA7", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "7", sequencing_id = "SR7"),
    EnrichedClinical(`batch_id` = "BATCH4", `analysis_id` = "SRA2", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "2", sequencing_id = "SR2"),
    EnrichedClinical(`batch_id` = "BATCH5", `analysis_id` = "SRA8", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "8", sequencing_id = "SR8")
  )

  // We suppose the normalize step was run and data from all batches is available in normalized_snv_somatic
  val existingNormalizedData = Seq(
    NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", sequencing_id = "SR1"),
    NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", sequencing_id = "SR2"),
    NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA", sequencing_id = "SR3"),
    NormalizedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA", sequencing_id = "SR4"),
    NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA", sequencing_id = "SR5"),
    NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA", sequencing_id = "SR6"),
    NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR7"),
    NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR2"),
    NormalizedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR8")
  )


  val existingNormalizedCnvSomaticTumorOnly = Seq(
    NormalizedCNVSomaticTumorOnly(chromosome = "1", start = 69890, end = 70000, reference = "N", alternate = "<DUP>", aliquot_id = "2", sequencing_id = "SR2", analysis_id = "SRA2", batch_id = "BATCH1"),
    NormalizedCNVSomaticTumorOnly(chromosome = "1", start = 69890, end = 70000, reference = "N", alternate = "<DUP>", aliquot_id = "4", sequencing_id = "SR4", analysis_id = "SRA4", batch_id = "BATCH2"),
  )
  val existingEnrichedData = Seq(
    EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", sequencing_id = "SR4", bioinfo_analysis_code = "TEBA")
  )

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(normalized_snv_somatic, normalized_cnv_somatic_tumor_only, enriched_snv_somatic,
    enriched_clinical)

  override def beforeEach(): Unit = {
    super.beforeEach()
    write(enriched_snv_somatic, existingEnrichedData.toDF())
    write(normalized_snv_somatic, existingNormalizedData.toDF())
    write(normalized_cnv_somatic_tumor_only, existingNormalizedCnvSomaticTumorOnly.toDF())
    write(enriched_clinical, existingClinicalData.toDF())
  }


  "extract" should "return relevant data -  new batch, no multi-batch analysis, no cnv data" in {

    // This test uses batch 3, representing a simple scenario with two new germline analyses (SRA5 and SRA6).
    // There is no CNV data associated with these analyses, and no analysis appears in other batches.
    val result = job(Some("BATCH3")).extract()

    result.size shouldBe 2

    // Since these are new analyses present only in batch3, we should see normalized somatic data only
    // from batch3
    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA", sequencing_id = "SR5"),
      NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA", sequencing_id = "SR6")
    )

    // no cnv data for this batch
    result(normalized_cnv_somatic_tumor_only.id).collect() shouldBe empty
  }

  it should "return relevant data -  new batch, with multi-batch analysis and cnv data available" in {

    // This test uses batch4, which contains two analyses (SRA2 and SRA7).
    // The analysis SRA2 also appear in batch 1
    // There is CNV data available for analysis SRA2
    val result = job(Some("BATCH4")).extract()

    result.size shouldBe 2

    // Should return normalized snv somatic data from both batch4 (SRA2, SRA7) and batch1 (SRA2)
    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      // FROM batch 4
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR7"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR2"),

      // FROM batch 1
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", sequencing_id = "SR2")
    )

    //  Should return cnv somatic tumor only data for batch1 (SRA2)
    result(normalized_cnv_somatic_tumor_only.id)
      .as[NormalizedCNVSomaticTumorOnly]
      .collect() should contain theSameElementsAs Seq(
      NormalizedCNVSomaticTumorOnly(chromosome = "1", start = 69890, end = 70000, reference = "N", alternate = "<DUP>", aliquot_id = "2", sequencing_id = "SR2", analysis_id = "SRA2", batch_id = "BATCH1"),
    )
  }


  it should "return relevant data - reingesting a batch, with cnv data" in {

    // This test uses batch 2, which contains a single analysis (SRA4). 
    // This batch is already enriched and has CNV data available.
    val result = job(Some("BATCH2")).extract()

    result.size shouldBe 2

    // Should return normalized snv somatic data for the batch
    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      NormalizedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA", sequencing_id = "SR4")
    )

    // Should return cnv somatic tumor only data for the batch
    result(normalized_cnv_somatic_tumor_only.id)
      .as[NormalizedCNVSomaticTumorOnly]
      .collect() should contain theSameElementsAs Seq(
      NormalizedCNVSomaticTumorOnly(chromosome = "1", start = 69890, end = 70000, reference = "N", alternate = "<DUP>", aliquot_id = "4", sequencing_id = "SR4", analysis_id = "SRA4", batch_id = "BATCH2")
    )
  }


  it should "return relevant data - no batch specified" in {
    val result = job(None).extract()

    result.size shouldBe 2

    // Should return all normalized snv somatic data from all batches
    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs existingNormalizedData

    // Should return all cnv somatic tumor only data from all batches
    result(normalized_cnv_somatic_tumor_only.id)
      .as[NormalizedCNVSomaticTumorOnly]
      .collect() should contain theSameElementsAs existingNormalizedCnvSomaticTumorOnly
  }


  "transform" should "produce enriched output - no overlapping analysis, no cnv data" in {
    // This test simulates a scenario with 2 batches, one tumor only and one tumor normal.
    // There are no analyses present in more than one batch and no CNV data involved.
    val normalizedSnvSomaticData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", sequencing_id = "SR1"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", sequencing_id = "SR2"),
      NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA", sequencing_id = "SR3"),
      NormalizedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", bioinfo_analysis_code = "TNEBA", sequencing_id = "SR8"),
    )
    val result = job(None).transform( // Using None as batch_id here as it is not relevant for this test
      Map(
        normalized_snv_somatic.id -> normalizedSnvSomaticData.toDF(),
        normalized_cnv_somatic_tumor_only.id -> Seq.empty[NormalizedCNVSomaticTumorOnly].toDF()
      )
    )

    result.size shouldBe 1

    // The all_analyses column should be ["TO"] for analysis from Batch1 (tumor only) and 
    // ["TN"] for analysis from Batch5 (tumor normal)
    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", sequencing_id = "SR8", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN"))
    )
  }


  it should "produce enriched output - overlapping analysis, no cnv data" in {
    // This test simulates 2 batches (BATCH1 and BATCH4) where the same analysis (SRA2) exists in both batches.
    // BATCH1 is tumor only (TEBA) and BATCH4 is tumor normal (TNEBA).
    val normalizedSnvSomaticData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", sequencing_id = "SR7", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TNEBA")
    )

    val result = job(None).transform( // Using None as batch_id here as it is not relevant for this test
      Map(
        normalized_snv_somatic.id -> normalizedSnvSomaticData.toDF(),
        normalized_cnv_somatic_tumor_only.id -> Seq.empty[NormalizedCNVSomaticTumorOnly].toDF()
      )
    )

    result.size shouldBe 1

    // The all_analyses column should be ["TO"] for analysis that appear only in Batch1 (SRA1, SRA3),
    // ["TN"] for analysis only in Batch4 (SRA7), and ["TO", "TN"] for the analysis in both Batch1 and Batch4 (SRA2).
    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Set(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", sequencing_id = "SR7", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN"))
    )
  }

  it should "compute cnv count for each SNV based on overlap between SNV and CNV data" in {
    // Using only tumor only data from the same batch. The test focuses on overlapping logic.
    // Other scenarios with both tumor only and tumor data are covered in other tests (see tests on method run).
    val data = Map(
      normalized_snv_somatic.id -> Seq(
        // cover 0 CNV
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_00", `sequencing_id` = "SR_000"),
        // cover 1 CNV
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_01", `sequencing_id` = "SR_001"),
        // cover 3 CNV
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_02", `sequencing_id` = "SR_001"),
        // cover 0 CNV
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_03", `sequencing_id` = "SR_001"),
        // cover 0 CNV (cause different service_request_id)
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_04", `sequencing_id` = "SR_002"),
      ).toDF(),
      normalized_cnv_somatic_tumor_only.id -> Seq(
        NormalizedCNVSomaticTumorOnly(`chromosome` = "1", `start` = 90, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01", `sequencing_id` = "SR_001"),
        NormalizedCNVSomaticTumorOnly(`chromosome` = "1", `start` = 110, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_02", `sequencing_id` = "SR_001"),
        NormalizedCNVSomaticTumorOnly(`chromosome` = "1", `start` = 130, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_03", `sequencing_id` = "SR_001"),
        NormalizedCNVSomaticTumorOnly(`chromosome` = "1", `start` = 220, `end` = 500, `alternate` = "A", reference = "REF", `name` = "CNV_04", `sequencing_id` = "SR_001"),
      ).toDF()
    )

    val result = job(None).transformSingle(data)

    result.as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_00", `cnv_count` = 0, `sequencing_id` = "SR_000"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_01", `cnv_count` = 1, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_02", `cnv_count` = 3, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_03", `cnv_count` = 0, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_04", `cnv_count` = 0, `sequencing_id` = "SR_002"),
    )
  }


  "run" should "correctly update and persist enriched SNV somatic data" in {
    // Using batch 4 because it includes an analysis present in an already existing batch.
    // It also includes cnv data.
    SNVSomatic(TestETLContext(RunStep.default_load), Some("BATCH4")).run()

    val enrichedDf = enriched_snv_somatic.read
    enrichedDf.as[EnrichedSNVSomatic].collect() should contain theSameElementsAs Seq(
      // same as existing data before the run
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", sequencing_id = "SR4", bioinfo_analysis_code = "TEBA"),

      // Updated because this analysis exists in batch 4. The cnv count is still 1 because we have tumor only CNV data for this analysis.
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN"), cnv_count = 1),

      // Added from batch 4. The cnv_count is 0 for SRA2 because we do not have tumor normal CNV data
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN"), cnv_count = 0),
      EnrichedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", sequencing_id = "SR7", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN"))
    )
  }


  // This test was added to guard against regressions related to duplicate rows in the enriched_snv_somatic table.
  // It ensures that re-processing an existing batch does not introduce duplicates.
  it should "correctly re-process an already existing batch" in {
    // We simulate a scenario where the normalized_snv_somatic data has been updated
    val newNormalizedSomaticData = existingNormalizedData.map { ns => ns.copy(last_update = Date.valueOf("2025-01-01")) }
    write(normalized_snv_somatic, newNormalizedSomaticData.toDF())

    // Using batch 2 because it has existing data and we want to test re-processing.
    // Note that this batch includes cnv data.
    SNVSomatic(TestETLContext(RunStep.default_load), Some("BATCH2")).run()

    val enrichedDf = enriched_snv_somatic.read
    enrichedDf.as[EnrichedSNVSomatic].collect() should contain theSameElementsAs Seq(
      // Existing data that should not change
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", sequencing_id = "SR1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", sequencing_id = "SR2", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", sequencing_id = "SR3", bioinfo_analysis_code = "TEBA"),

      // Existing data that should be updated. THe cnv count is 1 because there is somatic tumor only cnv data for the analysis.
      EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", sequencing_id = "SR4", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO"), last_update = Date.valueOf("2025-01-01"), cnv_count = 1)
    )
  }
}
