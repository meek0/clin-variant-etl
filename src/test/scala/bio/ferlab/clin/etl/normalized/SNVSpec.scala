package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.{SNV_GENOTYPES, VCF_SNV_Input}
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.normalized.NormalizedSNV
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}
import org.apache.spark.sql.DataFrame

import java.sql.Date
import java.time.LocalDate

class SNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val family: DatasetConf = conf.getDataset("normalized_family")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val rare_variants: DatasetConf = conf.getDataset("enriched_rare_variant")

  val job = SNV(DeprecatedTestETLContext(), "BAT1")

  val patientDf: DataFrame = Seq(
    PatientOutput(
      `id` = "PA0001",
      `gender` = "male",
      `practitioner_role_id` = "PPR00101",
      `organization_id` = Some("OR00201")
    ),
    PatientOutput(
      `id` = "PA0002",
      `practitioner_role_id` = "PPR00101",
      `gender` = "male"
    ),
    PatientOutput(
      `id` = "PA0003",
      `practitioner_role_id` = "PPR00101",
      `gender` = "female"
    )
  ).toDF()

  val taskDf: DataFrame = Seq(
    TaskOutput(
      batch_id = "BAT1",
      `id` = "73254",
      `patient_id` = "PA0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0001"
    ),
    TaskOutput(
      batch_id = "BAT1",
      `id` = "73256",
      `patient_id` = "PA0002",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "22222"),
      `service_request_id` = "SRS0002"
    ),
    TaskOutput(
      batch_id = "BAT1",
      `id` = "73257",
      `patient_id` = "PA0003",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "33333"),
      `service_request_id` = "SRS0003"
    ),
    TaskOutput(
      batch_id = "BAT1",
      `id` = "73255",
      `patient_id` = "PA00095",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0099"
    )
  ).toDF

  val clinicalImpressionsDf: DataFrame = Seq(
    ClinicalImpressionOutput(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB0099")),
    ClinicalImpressionOutput(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    ClinicalImpressionOutput(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003"))
  ).toDF()

  val observationsDf: DataFrame = Seq(
    ObservationOutput(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0099", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    ObservationOutput(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "affected"),
  ).toDF()
  val serviceRequestDf: DataFrame = Seq(
    ServiceRequestOutput(service_request_type = "analysis", `id` = "SRA0001", `patient_id` = "PA0001",
      family = Some(FAMILY(mother = Some("PA0003"), father = Some("PA0002"))),
      family_id = Some("FM00001"),
      `clinical_impressions` = Some(Seq("CI0001", "CI0002", "CI0003")),
      `service_request_description` = Some("Maladies musculaires (Panel global)")
    ),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0001", `patient_id` = "PA0001", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0002", `patient_id` = "PA0002", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0003", `patient_id` = "PA0003", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)"))
  ).toDF()
  val familyDf: DataFrame = Seq(
    FamilyOutput(analysis_service_request_id = "SRA0001", patient_id = "PA0001", family = Some(FAMILY(mother = Some("PA0003"), father = Some("PA0002"))), family_id = Some("FM00001")),
    FamilyOutput(analysis_service_request_id = "SRA0001", patient_id = "PA0002", family = None, family_id = Some("FM00001")),
    FamilyOutput(analysis_service_request_id = "SRA0001", patient_id = "PA0003", family = None, family_id = Some("FM00001"))

  ).toDF()

  val specimenDf: DataFrame = Seq(
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = Some("SA_001"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = None, `specimen_id` = Some("SP_001")),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = Some("SA_002"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = None, `specimen_id` = Some("SP_002")),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = Some("SA_003"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = None, `specimen_id` = Some("SP_003")),
  ).toDF

  val data: Map[String, DataFrame] = Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Input(
      `genotypes` = List(
        SNV_GENOTYPES(), //proband
        SNV_GENOTYPES(`sampleId` = "22222", `calls` = List(0, 0), `alleleDepths` = List(30, 0)), //father
        SNV_GENOTYPES(`sampleId` = "33333")) //mother
    )).toDF(),
    patient.id -> patientDf,
    clinical_impression.id -> clinicalImpressionsDf,
    observation.id -> observationsDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf,
    specimen.id -> specimenDf,
    family.id -> familyDf,
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
    result.length shouldBe > (0)
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
