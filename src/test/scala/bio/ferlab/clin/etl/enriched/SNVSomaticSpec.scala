package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.{EnrichedClinical, EnrichedSNVSomatic}
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedSNVSomatic}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.clin.testutils.LoadResolverUtils.write
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class SNVSomaticSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

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

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(normalized_snv_somatic, normalized_cnv, enriched_snv_somatic, enriched_clinical)

  override def beforeEach(): Unit = {
    super.beforeEach()
    write(enriched_snv_somatic, existingEnrichedData.toDF())
    write(normalized_cnv, existingNormalizedCnv.toDF())
  }

  "extract" should "only return current analyses when no past analyses exist" in {
    val currentBatchClinicalData = Seq(
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA5", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "5"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA6", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "6"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA7", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "7"),
    )
    val clinicalDf = (existingClinicalData ++ currentBatchClinicalData).toDF()
    write(enriched_clinical, clinicalDf)

    val currentBatchNormalizedData = Seq(
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH3", analysis_id = "SRA7", bioinfo_analysis_code = "TEBA"),
    )
    val normalizedSnvSomaticDf = (existingNormalizedData ++ currentBatchNormalizedData).toDF()
    write(normalized_snv_somatic, normalizedSnvSomaticDf)

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
    write(enriched_clinical, clinicalDf)

    val currentBatchNormalizedData = Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH3", analysis_id = "SRA2", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "5", batch_id = "BATCH3", analysis_id = "SRA5", bioinfo_analysis_code = "TEBA")
    )
    val normalizedSnvSomaticDf = (existingNormalizedData ++ currentBatchNormalizedData).toDF()
    write(normalized_snv_somatic, normalizedSnvSomaticDf)

    val result = job(Some("BATCH3")).extract()

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic].collect().foreach(row => println(row + "\n\n"))

    result(normalized_snv_somatic.id)
      .as[NormalizedSNVSomatic]
      .collect() should contain theSameElementsAs currentBatchNormalizedData ++ Seq(
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      NormalizedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
    )

    result(enriched_snv_somatic.id)
      .as[EnrichedSNVSomatic]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA")
    )
  }

  "extract" should "return all past analyses when no batch id is submitted" in {
    write(enriched_clinical, existingClinicalData.toDF())
    write(normalized_snv_somatic, existingNormalizedData.toDF())

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
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_00", `cnv_count` = 0, `sequencing_id` = "SR_000"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_01", `cnv_count` = 1, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_02", `cnv_count` = 3, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_03", `cnv_count` = 0, `sequencing_id` = "SR_001"),
      EnrichedSNVSomatic(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_04", `cnv_count` = 0, `sequencing_id` = "SR_002"),
    )
  }

  "run" should "properly merge new batch data with existing analyses from other batches" in {

    // We simulate a new batch (BATCH3) containing somatic tumor normal data for an existing analysis and new analysis as well.
    val currentBatchClinicalData = Seq(
      // same analysis exists in a different batch, but with a different bioinfo analysis code
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA1", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "1"),

      // new analysis
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA6", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "6"),
      EnrichedClinical(`batch_id` = "BATCH3", `analysis_id` = "SRA7", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "7")
    )
    val clinicalDf = (existingClinicalData ++ currentBatchClinicalData).toDF()
    write(enriched_clinical, clinicalDf)

    val currentBatchNormalizedData = Seq(
      // same analysis exists in a different batch, but with a different bioinfo analysis code
      NormalizedSNVSomatic(aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA"),

      // new analysis
      NormalizedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TNEBA"),
      NormalizedSNVSomatic(aliquot_id = "7", batch_id = "BATCH3", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA"),
    )
    val normalizedSnvSomaticDf = (existingNormalizedData ++ currentBatchNormalizedData).toDF()
    write(normalized_snv_somatic, normalizedSnvSomaticDf)

    // Running the job
    val result = SNVSomatic(TestETLContext(RunStep.default_load), Some("BATCH3")).run()

    // We expect the enriched_snv_somatic table to contain the merged data
    result.size shouldBe 1
    val enrichedDf = result(enriched_snv_somatic.id)
    enrichedDf.as[EnrichedSNVSomatic].collect() should contain theSameElementsAs Seq(
      // Changed:  all_analyses column contains TO and TN
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH1", analysis_id = "SRA1", bioinfo_analysis_code = "TEBA", all_analyses = Set("TO", "TN")),

      // Added
      EnrichedSNVSomatic(aliquot_id = "1", batch_id = "BATCH3", analysis_id = "SRA1", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TO", "TN")),

      // Not changed
      EnrichedSNVSomatic(aliquot_id = "2", batch_id = "BATCH1", analysis_id = "SRA2", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "3", batch_id = "BATCH1", analysis_id = "SRA3", bioinfo_analysis_code = "TEBA"),
      EnrichedSNVSomatic(aliquot_id = "4", batch_id = "BATCH2", analysis_id = "SRA4", bioinfo_analysis_code = "TEBA"),

      // Added
      EnrichedSNVSomatic(aliquot_id = "6", batch_id = "BATCH3", analysis_id = "SRA6", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN")),
      EnrichedSNVSomatic(aliquot_id = "7", batch_id = "BATCH3", analysis_id = "SRA7", bioinfo_analysis_code = "TNEBA", all_analyses = Set("TN"))
    )
  }
}
