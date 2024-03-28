package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.{SNV_GENOTYPES, VCF_SNV_Input}
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedSNV
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{DeprecatedTestETLContext, SparkSpec}
import org.apache.spark.sql.DataFrame

import java.sql.Date
import java.time.LocalDate

class SNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val rare_variants: DatasetConf = conf.getDataset("enriched_rare_variant")

  val job = SNV(DeprecatedTestETLContext(), "BAT1")

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`patient_id` = "PA0001", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0001", `batch_id` = "BAT1", `aliquot_id` = "11111", `is_proband` = true, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_001", `specimen_id` = "SP_001", `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"), `mother_aliquot_id` = Some("33333"), `father_aliquot_id` = Some("22222")),
    EnrichedClinical(`patient_id` = "PA0002", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0002", `batch_id` = "BAT1", `aliquot_id` = "22222", `is_proband` = false, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "SA_002", `specimen_id` = "SP_002", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
    EnrichedClinical(`patient_id` = "PA0003", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0003", `batch_id` = "BAT1", `aliquot_id` = "33333", `is_proband` = false, `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_003", `specimen_id` = "SP_003", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Input(
      `genotypes` = List(
        SNV_GENOTYPES(), //proband
        SNV_GENOTYPES(`sampleId` = "22222", `calls` = List(0, 0), `alleleDepths` = List(30, 0)), //father
        SNV_GENOTYPES(`sampleId` = "33333")) //mother
    )).toDF(),
    enriched_clinical.id -> clinicalDf,
    rare_variants.id -> Seq(RareVariant()).toDF()
  )

  "occurrences transform" should "transform data in expected format" in {
    val results = job.transform(data)
    val result = results("normalized_snv").as[NormalizedSNV].collect()

    result.length shouldBe 2
    val probandSnv = result.find(_.patient_id == "PA0001")
    probandSnv shouldBe Some(NormalizedSNV(
      analysis_code = "MMG",
      specimen_id = "SP_001",
      sample_id = "SA_001",
      organization_id = "CHUSJ",
      hc_complement = List(),
      possibly_hc_complement = List(),
      service_request_id = "SRS0001",
      last_update = Date.valueOf(LocalDate.now())
    ))

    val motherSnv = result.find(_.patient_id == "PA0003")
    motherSnv shouldBe Some(NormalizedSNV(
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
      mother_gq = None,
      mother_qd = None,
      father_gq = None,
      father_qd = None,
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
      last_update = Date.valueOf(LocalDate.now())
    ))
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedSNV", result, "src/test/scala/")
  }

  "occurrences transform" should "work with an empty input VCF Dataframe" in {
    val results = job.transform(data ++ Map(raw_variant_calling.id -> spark.emptyDataFrame))
    val result = results("normalized_snv").as[NormalizedSNV].collect()
    result.length shouldBe 0
  }

  "occurrences transform" should "ignore invalid contigName" in {
    val results = job.transform(data ++ Map(raw_variant_calling.id -> Seq(
      VCF_SNV_Input(`contigName` = "chr2"),
      VCF_SNV_Input(`contigName` = "chrY"),
      VCF_SNV_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_snv").as[NormalizedSNV].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

  "addRareVariantColumn" should "add a column that indicate if variant is rare or not" in {
    val occurrences = Seq(
      RareVariantOccurence(chromosome = "1", start = 1000, reference = "A", alternate = "T"),
      RareVariantOccurence(chromosome = "1", start = 2000, reference = "C", alternate = "G"),
      RareVariantOccurence(chromosome = "1", start = 3000, reference = "C", alternate = "G")
    ).toDF()

    val rare = Seq(
      RareVariant(chromosome = "1", start = 1000, reference = "A", alternate = "T", is_rare = true),
      RareVariant(chromosome = "1", start = 2000, reference = "C", alternate = "G", is_rare = false)
    ).toDF()

    val result = SNV.addRareVariantColumn(occurrences, rare).as[RareVariantOutput]
    result.collect() should contain theSameElementsAs Seq(
      RareVariantOutput(chromosome = "1", start = 1000, reference = "A", alternate = "T", is_rare = true),
      RareVariantOutput(chromosome = "1", start = 2000, reference = "C", alternate = "G", is_rare = false),
      RareVariantOutput(chromosome = "1", start = 3000, reference = "C", alternate = "G", is_rare = true)
    )
  }

}
