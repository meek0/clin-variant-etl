package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class SNVSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")

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
      `id` = "73254",
      `patient_id` = "PA0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0001"
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0002",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "22222"),
      `service_request_id` = "SRS0002"
    ),
    TaskOutput(
      `id` = "73257",
      `patient_id` = "PA0003",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "33333"),
      `service_request_id` = "SRS0003"
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA00095",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0099"
    )
  ).toDF

  val clinicalImpressionsDf = Seq(
    ClinicalImpressionOutput(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB0099")),
    ClinicalImpressionOutput(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    ClinicalImpressionOutput(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003"))
  ).toDF()

  val observationsDf = Seq(
    ObservationOutput(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "POS"),
    ObservationOutput(id = "OB0099", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "POS"),
    ObservationOutput(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "NEG"),
    ObservationOutput(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "POS"),
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

  val specimenDf: DataFrame = Seq(
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = Some("SA_001"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = None, `specimen_id` = Some("SP_001")),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = Some("SA_002"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = None, `specimen_id` = Some("SP_002")),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = Some("SA_003"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = None, `specimen_id` = Some("SP_003")),
  ).toDF

  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput(
      `genotypes` = List(
        GENOTYPES(), //proband
        GENOTYPES(`sampleId` = "22222", `calls` = List(0, 0)), //father
        GENOTYPES(`sampleId` = "33333")) //mother
    )).toDF(),
    patient.id -> patientDf,
    clinical_impression.id -> clinicalImpressionsDf,
    observation.id -> observationsDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf,
    specimen.id -> specimenDf
  )

  "occurrences transform" should "transform data in expected format" in {
    val results = new SNV("BAT1").transform(data)
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

  "getCompoundHet" should "return compound het for one patient and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), "father")
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )
  }
  it should "return compound het for one patient and multiple genes" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2"), "father"),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1", "BRAF2"), null),
      CompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2"), "father")
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1030-C-G", "1-1070-C-G")), HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")), HCComplement("BRAF2", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA001", "1", 1070, "C", "G", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1000-A-T"))))
    )

  }
  it should "return compound het for two patients and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), "father"),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1"), null),
      CompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA002", "1", 1050, "C", "G", Seq("BRAF1"), "father"),
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA002", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1050-C-G")))),
      CompoundHetOutput("PA002", "1", 1050, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )

  }

  "getPossiblyCompoundHet" should "return possibly compound het for many patients" in {
    val input = Seq(
      PossiblyCompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1090, "C", "G", Seq("BRAF3")),
      PossiblyCompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA002", "1", 1030, "C", "G", Seq("BRAF1"))
    ).toDF()

    val result = SNV.getPossiblyCompoundHet(input).as[PossiblyCompoundHetOutput]
    result.collect() should contain theSameElementsAs Seq(
      PossiblyCompoundHetOutput("PA001", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2), PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2), PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1070, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA002", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      PossiblyCompoundHetOutput("PA002", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
    )


  }


}
