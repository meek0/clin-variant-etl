package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.{EnrichedClinical, EnrichedSNVSomatic}
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedSNVSomatic}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}

class SNVSomaticSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val enriched_snv_somatic: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job: Option[String] => SNVSomatic = batch => SNVSomatic(TestETLContext(), batch)

  val existingClinicalData = Seq(
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA1", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "1"),
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA2", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "1"),
    EnrichedClinical(`batch_id` = "BATCH1", `analysis_id` = "SRA3", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "1"),
    EnrichedClinical(`batch_id` = "BATCH2", `analysis_id` = "SRA4", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "1"),
  )

  val existingNormalizedData = Seq(
    NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
    NormalizedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA")
  )

  val existingNormalizedCnv = Seq(
    NormalizedCNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
  )

  val existingEnrichedData = Seq(
    EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
    EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA")
  )

  override val dsToClean: List[DatasetConf] = List(normalized_snv_somatic, normalized_cnv, enriched_snv_somatic, enriched_clinical)

  override def beforeEach(): Unit = {
    super.beforeEach()
    LoadResolver
      .write
      .apply(enriched_snv_somatic.format, enriched_snv_somatic.loadtype)
      .apply(enriched_snv_somatic, existingEnrichedData.toDF())
    LoadResolver
      .write
      .apply(normalized_cnv.format, normalized_cnv.loadtype)
      .apply(normalized_cnv, existingNormalizedCnv.toDF())
  }

  "extract" should "only return current analyses when no past analyses exist" in {
    val currentBatchClinicalData = Seq(
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA5", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "5"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA6", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "6"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA7", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "7"),
    )
    val clinicalDf = (existingClinicalData ++ currentBatchClinicalData).toDF()
    LoadResolver
      .write
      .apply(enriched_clinical.format, enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)

    val currentBatchNormalizedData = Seq(
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH3", analysis_id = "SRA7", bioinfo_analysis_code = "TEBA"),
    )
    val normalizedSnvSomaticDf = (existingNormalizedData ++ currentBatchNormalizedData).toDF()
    LoadResolver
      .write
      .apply(normalized_snv_somatic.format, normalized_snv_somatic.loadtype)
      .apply(normalized_snv_somatic, normalizedSnvSomaticDf)

    val result = job(Some("BATCH3")).extract()

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs currentBatchNormalizedData

    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() shouldBe empty
  }

  "extract" should "return current and past analyses when the latter exists" in {
    val currentBatchClinicalData = Seq(
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA1", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "1"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA2", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "2"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA5", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "5"),
    )
    val clinicalDf = (existingClinicalData ++ currentBatchClinicalData).toDF()
    LoadResolver
      .write
      .apply(enriched_clinical.format, enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)

    val currentBatchNormalizedData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA")
    )
    val normalizedSnvSomaticDf = (existingNormalizedData ++ currentBatchNormalizedData).toDF()
    LoadResolver
      .write
      .apply(normalized_snv_somatic.format, normalized_snv_somatic.loadtype)
      .apply(normalized_snv_somatic, normalizedSnvSomaticDf)

    val result = job(Some("BATCH3")).extract()

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs currentBatchNormalizedData

    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA")
    )
  }

  "extract" should "return all past analyses when no batch id is submitted" in {
    LoadResolver
      .write
      .apply(enriched_clinical.format, enriched_clinical.loadtype)
      .apply(enriched_clinical, existingClinicalData.toDF())
    LoadResolver
      .write
      .apply(normalized_snv_somatic.format, normalized_snv_somatic.loadtype)
      .apply(normalized_snv_somatic, existingNormalizedData.toDF())

    val result = job(None).extract()

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs existingNormalizedData

    result(enriched_snv_somatic.id).collect() shouldBe empty
  }

  "transform" should "compute all_analyses from normalized data when no past analyses exist" in {
    val data = Map(
      normalized_snv_somatic.id -> Seq(
        // BATCH 1
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),

        // BATCH 2
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH2", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA"),
        NormalizedSNVSomatic(chromosome = "2", aliquot_id = "2", batch_id = "BATCH2", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
      ).toDF(),
      normalized_cnv.id -> existingNormalizedCnv.toDF(),
      enriched_snv_somatic.id -> spark.emptyDataFrame
    )

    val result = job(None).transformSingle(data)

    result
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      // SRA 1
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH2", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),

      // SRA 2
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      EnrichedSNVSomatic(chromosome = "2", aliquot_id = "2", batch_id = "BATCH2", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN")),
    )
  }

  "transform" should "update all_analyses for existing data" in {
    val data = Map(
      normalized_snv_somatic.id -> Seq(
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA"),
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
        NormalizedSNVSomatic(chromosome = "2", aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
        NormalizedSNVSomatic(chromosome = "1", aliquot_id = "3", batch_id = "BATCH3", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA")
      ).toDF(),
      normalized_cnv.id -> existingNormalizedCnv.toDF(),
      enriched_snv_somatic.id -> Seq(
        EnrichedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
        EnrichedSNVSomatic(chromosome = "2", aliquot_id = "1", batch_id = "BATCH2", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
        EnrichedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH2", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
      ).toDF()
    )

    val result = job(Some("BATCH3")).transformSingle(data)

    result
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      // SRA 1
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "2", aliquot_id = "1", batch_id = "BATCH2", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),

      // SRA 2
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH2", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),
      EnrichedSNVSomatic(chromosome = "2", aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN")),

      // SRA 3
      EnrichedSNVSomatic(chromosome = "1", aliquot_id = "3", batch_id = "BATCH3", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO")),
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
      ).toDF(),
      enriched_snv_somatic.id -> spark.emptyDataFrame
    )

    val result = job(None).transformSingle(data)

    result.as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_00", `cnv_count` = 0, `sequencing_id` = "SR_000",
      ),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_01", `cnv_count` = 1, `sequencing_id` = "SR_001",
      ),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_02", `cnv_count` = 3, `sequencing_id` = "SR_001",
      ),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_03", `cnv_count` = 0, `sequencing_id` = "SR_001",
      ),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_04", `cnv_count` = 0, `sequencing_id` = "SR_002",
      ),
    )
  }
}

