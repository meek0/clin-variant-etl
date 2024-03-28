package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.{SNV_SOMATIC_GENOTYPES, VCF_SNV_Somatic_Input}
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedSNVSomatic
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.{DatasetConf, LoadType}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CreateDatabasesBeforeAll, DeprecatedTestETLContext, SparkSpec}
import org.apache.spark.sql.DataFrame

import java.sql.Date
import java.time.LocalDate

class SNVSomaticSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll {

  import spark.implicits._

  val tumorOnlyBatchId = "BAT1"
  val tumorNormalBatchId = "BAT2"

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val rare_variants: DatasetConf = conf.getDataset("enriched_rare_variant")

  override val dbToCreate: List[String] = List("clin")

  val tumorOnlyJob = SNVSomatic(DeprecatedTestETLContext(), tumorOnlyBatchId)
  val tumorNormalJob = SNVSomatic(DeprecatedTestETLContext(), tumorNormalBatchId)

  val clinicalDf: DataFrame = Seq(
    // TEBA (tumor only analysis)
    EnrichedClinical(`patient_id` = "PA0001", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0001", `batch_id` = tumorOnlyBatchId, `aliquot_id` = "11111", `is_proband` = true, `gender` = "Male", `bioinfo_analysis_code` = "TEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_001", `specimen_id` = "SP_001", `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"), `mother_aliquot_id` = Some("33333"), `father_aliquot_id` = Some("22222")),
    EnrichedClinical(`patient_id` = "PA0002", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0002", `batch_id` = tumorOnlyBatchId, `aliquot_id` = "22222", `is_proband` = false, `gender` = "Male", `bioinfo_analysis_code` = "TEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "SA_002", `specimen_id` = "SP_002", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
    EnrichedClinical(`patient_id` = "PA0003", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0003", `batch_id` = tumorOnlyBatchId, `aliquot_id` = "33333", `is_proband` = false, `gender` = "Female", `bioinfo_analysis_code` = "TEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_003", `specimen_id` = "SP_003", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),

    // TNEBA (tumor normal analysis)
    EnrichedClinical(`patient_id` = "PA0001", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0001", `batch_id` = tumorNormalBatchId, `aliquot_id` = "11111", `is_proband` = true, `gender` = "Male", `bioinfo_analysis_code` = "TNEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_001", `specimen_id` = "SP_001", `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"), `mother_aliquot_id` = Some("33333"), `father_aliquot_id` = Some("22222")),
    EnrichedClinical(`patient_id` = "PA0002", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0002", `batch_id` = tumorNormalBatchId, `aliquot_id` = "22222", `is_proband` = false, `gender` = "Male", `bioinfo_analysis_code` = "TNEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "SA_002", `specimen_id` = "SP_002", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
    EnrichedClinical(`patient_id` = "PA0003", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0003", `batch_id` = tumorNormalBatchId, `aliquot_id` = "33333", `is_proband` = false, `gender` = "Female", `bioinfo_analysis_code` = "TNEBA", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_003", `specimen_id` = "SP_003", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    enriched_clinical.id -> clinicalDf,
    rare_variants.id -> Seq(RareVariant()).toDF()
  )

  val dataWithVariantCalling: Map[String, DataFrame] = data + (raw_variant_calling.id -> Seq(VCF_SNV_Somatic_Input(
    `genotypes` = List(
      SNV_SOMATIC_GENOTYPES(`sampleId` = "11111"), //proband
      SNV_SOMATIC_GENOTYPES(`sampleId` = "22222", `calls` = List(0, 0), `alleleDepths` = List(30, 0)), //father
      SNV_SOMATIC_GENOTYPES(`sampleId` = "33333")) //mother
  )).toDF())

  override def beforeAll(): Unit = {
    super.beforeAll()

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  it should "only extract tumor_only VCFs for a tumor_only batch" in {
    val result = tumorOnlyJob.extract()

    result(raw_variant_calling.id)
      .as[VCF_SNV_Somatic_Input]
      .collect()
      .length shouldBe 49
  }

  it should "only extract tumor_normal VCFs for a tumor_normal batch" in {
    val result = tumorNormalJob.extract()

    result(raw_variant_calling.id)
      .as[VCF_SNV_Somatic_Input]
      .collect()
      .length shouldBe 151
  }

  it should "transform somatic tumor_only data to expected format" in {
    val results = tumorOnlyJob.transform(dataWithVariantCalling)
    val result = results("normalized_snv_somatic").as[NormalizedSNVSomatic].collect()

    result.length shouldBe 2
    val probandSnv = result.find(_.patient_id == "PA0001")
    probandSnv shouldBe Some(NormalizedSNVSomatic(
      analysis_code = "MMG",
      specimen_id = "SP_001",
      sample_id = "SA_001",
      organization_id = "CHUSJ",
      hc_complement = List(),
      possibly_hc_complement = List(),
      service_request_id = "SRS0001",
      last_update = Date.valueOf(LocalDate.now()),
      batch_id = tumorOnlyBatchId,
      bioinfo_analysis_code = "TEBA"
    ))

    val motherSnv = result.find(_.patient_id == "PA0003")
    motherSnv shouldBe Some(NormalizedSNVSomatic(
      patient_id = "PA0003",
      gender = "Female",
      aliquot_id = "33333",
      analysis_code = "MMG",
      specimen_id = "SP_003",
      sample_id = "SA_003",
      organization_id = "CHUSJ",
      service_request_id = "SRS0003",
      hc_complement = List(),
      possibly_hc_complement = List(),
      is_proband = false,
      mother_id = null,
      father_id = null,
      mother_aliquot_id = None,
      father_aliquot_id = None,
      mother_calls = None,
      father_calls = None,
      mother_affected_status = None,
      father_affected_status = None,
      mother_zygosity = None,
      father_zygosity = None,
      parental_origin = Some("unknown"),
      transmission = Some("unknown_parents_genotype"),
      last_update = Date.valueOf(LocalDate.now()),
      batch_id = tumorOnlyBatchId,
      bioinfo_analysis_code = "TEBA"
    ))
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedSNVSomatic", result, "src/test/scala/")
  }

  it should "transform somatic tumor_normal data to expected format" in {
    val results = tumorNormalJob.transform(dataWithVariantCalling)
    val result = results("normalized_snv_somatic").as[NormalizedSNVSomatic].collect()

    result.length shouldBe 2
    val probandSnv = result.find(_.patient_id == "PA0001")
    probandSnv shouldBe Some(NormalizedSNVSomatic(
      analysis_code = "MMG",
      specimen_id = "SP_001",
      sample_id = "SA_001",
      organization_id = "CHUSJ",
      hc_complement = List(),
      possibly_hc_complement = List(),
      service_request_id = "SRS0001",
      last_update = Date.valueOf(LocalDate.now()),
      batch_id = tumorNormalBatchId,
      bioinfo_analysis_code = "TNEBA"
    ))

    val motherSnv = result.find(_.patient_id == "PA0003")
    motherSnv shouldBe Some(NormalizedSNVSomatic(
      patient_id = "PA0003",
      gender = "Female",
      aliquot_id = "33333",
      analysis_code = "MMG",
      specimen_id = "SP_003",
      sample_id = "SA_003",
      organization_id = "CHUSJ",
      service_request_id = "SRS0003",
      hc_complement = List(),
      possibly_hc_complement = List(),
      is_proband = false,
      mother_id = null,
      father_id = null,
      mother_aliquot_id = None,
      father_aliquot_id = None,
      mother_calls = None,
      father_calls = None,
      mother_affected_status = None,
      father_affected_status = None,
      mother_zygosity = None,
      father_zygosity = None,
      parental_origin = Some("unknown"),
      transmission = Some("unknown_parents_genotype"),
      last_update = Date.valueOf(LocalDate.now()),
      batch_id = tumorNormalBatchId,
      bioinfo_analysis_code = "TNEBA"
    ))
  }

  "transform" should "work with an empty input VCF Dataframe" in {
    val results = tumorOnlyJob.transform(data ++ Map(raw_variant_calling.id -> spark.emptyDataFrame))
    val result = results("normalized_snv_somatic").as[NormalizedSNVSomatic].collect()
    result.length shouldBe 0
  }

  "transform" should "ignore invalid contigName" in {
    val results = tumorOnlyJob.transform(data ++ Map(raw_variant_calling.id -> Seq(
      VCF_SNV_Somatic_Input(`contigName` = "chr2"),
      VCF_SNV_Somatic_Input(`contigName` = "chrY"),
      VCF_SNV_Somatic_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_snv_somatic").as[NormalizedSNVSomatic].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

}
