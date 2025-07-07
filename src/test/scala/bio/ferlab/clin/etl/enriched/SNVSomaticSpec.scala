package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.{EnrichedClinical, EnrichedSNVSomatic}
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedSNVSomatic}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.clin.testutils.LoadResolverUtils.write
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}

import java.sql.Date

class SNVSomaticSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val enriched_snv_somatic: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job: Option[String] => SNVSomatic = batch => SNVSomatic(TestETLContext(), batch)

  // We simulate a scenario with 2 batches already in enriched_snv_somatic and 3 new batches to be processed.
  // BATCH1: TEBA, 3 analysis already enriched
  // BATCH2: TEBA, 1 analysis already enriched, with cnv data
  // BATCH3: TEBA, 2 analysis NOT enriched
  // BATCH4: TNEBA, 2 analysis NOT enriched, one analysis shared with BATCH1
  // BATCH5: TNEBA, 1 analysis NOT enriched
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
    NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA"),
    NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
    NormalizedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", bioinfo_analysis_code = "TNEBA")
  )

  // CNV data for tumor-only batches (TEBA code) should not exist in the normalized_cnv dataset.
  // We include it here only to cover the current extract logic, so we have a baseline for CNV count testing logic.
  val existingNormalizedCnv = Seq(
    NormalizedCNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "4", sequencing_id = "SR4", analysis_id = "SRA4", batch_id = "BATCH2"),
  )

  val existingEnrichedData = Seq(
    EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA")
  )

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(normalized_snv_somatic, normalized_cnv, enriched_snv_somatic, enriched_clinical)

  override def beforeEach(): Unit = {
    super.beforeEach()
    write(enriched_snv_somatic, existingEnrichedData.toDF())
    write(normalized_snv_somatic, existingNormalizedData.toDF())
    write(normalized_cnv, existingNormalizedCnv.toDF())
    write(enriched_clinical, existingClinicalData.toDF())
  }

  "extract" should "return data relevant to the batch if batch id is provided" in {
    val result = job(Some("BATCH3")).extract()

    result.size shouldBe 2

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA")
    )

    // no cnv data for this batch
    result(normalized_cnv.id).collect() shouldBe empty
  }

  it should "include data from other batches if the analysis exists in multiple batches" in {
    val result = job(Some("BATCH4")).extract()

    result.size shouldBe 2

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      // FROM batch 4
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),

      // FROM batch 1
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA")
    )

    // no cnv data for this batch
    result(normalized_cnv.id).collect() shouldBe empty
  }

  it should "include cnv data if there is cnv data applicable to the batch" in {
    val result = job(Some("BATCH2")).extract()

    result.size shouldBe 2

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      NormalizedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA")
    )

    result(normalized_cnv.id)
      .as[NormalizedCNV]
      .collect() should contain theSameElementsAs Seq(
      NormalizedCNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "4", sequencing_id = "SR4", analysis_id = "SRA4", batch_id = "BATCH2")
    )
  }

  it should "return data for all analyses when no batch id is specified" in {
    val result = job(None).extract()

    result.size shouldBe 2

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs existingNormalizedData

    result(normalized_cnv.id)
      .as[NormalizedCNV]
      .collect() should contain theSameElementsAs existingNormalizedCnv
  }

  "transform" should "produce enriched output for all SNV somatic inputs" in {
    val normalizedSnvSomaticData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", bioinfo_analysis_code = "TNEBA"),
    )
    val result = job(None).transform( // Using None as batch_id here as it is not relevant for this test
      Map(
        normalized_snv_somatic.id -> normalizedSnvSomaticData.toDF(),
        normalized_cnv.id -> Seq.empty[NormalizedCNV].toDF()
      )
    )

    result.size shouldBe 1
    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "8", batch_id = "BATCH5", analysis_id = "SRA8", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN"))
    )
  }

  it should "merge unique values in all_analyses column when an analysis exists in multiple batches" in {
    val normalizedSnvSomaticData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA")
    )

    val result = job(None).transform( // Using None as batch_id here as it is not relevant for this test
      Map(
        normalized_snv_somatic.id -> normalizedSnvSomaticData.toDF(),
        normalized_cnv.id -> Seq.empty[NormalizedCNV].toDF()
      )
    )

    result.size shouldBe 1
    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Set(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN")),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN"))
    )
  }

  it should "enrich SNV Somatic data with cnv count" in {
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
      normalized_cnv.id -> Seq(
        NormalizedCNV(`chromosome` = "1", `start` = 90, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 110, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_02", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 130, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_03", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 220, `end` = 500, `alternate` = "A", reference = "REF", `name` = "CNV_04", `sequencing_id` = "SR_001"),
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
    //Using batch 4 because it includes an analysis present in an already existing batch
    SNVSomatic(TestETLContext(RunStep.default_load), Some("BATCH4")).run()

    val enrichedDf = enriched_snv_somatic.read
    enrichedDf.as[EnrichedSNVSomatic].collect() should contain theSameElementsAs Seq(
      // same as existing data before the run
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA"),

      // Updated because this analysis exists in batch 4
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),

      // Added from batch 4
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH4", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(aliquot_id = "7", batch_id = "BATCH4", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN"))
    )
  }

  // This test was added to guard against regressions related to duplicate rows in the enriched_snv_somatic table.
  // It ensures that re-processing an existing batch does not introduce duplicates.
  it should "correctly re-process an already existing batch" in {
    // We simulate a scenario where the normalized_snv_somatic data has been updated
    val newNormalizedSomaticData = existingNormalizedData.map { ns => ns.copy(last_update = Date.valueOf("2025-01-01")) }
    write(normalized_snv_somatic, newNormalizedSomaticData.toDF())

    // Using batch 2 because it has existing data and we want to test re-processing
    SNVSomatic(TestETLContext(RunStep.default_load), Some("BATCH2")).run()

    val enrichedDf = enriched_snv_somatic.read
    enrichedDf.as[EnrichedSNVSomatic].collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO"), last_update = Date.valueOf("2025-01-01"))
    )
  }
}
