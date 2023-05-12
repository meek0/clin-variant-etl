package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")

  val clinicalImpressionsDf: DataFrame = Seq(
    ClinicalImpressionOutput(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB0099")),
    ClinicalImpressionOutput(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    ClinicalImpressionOutput(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003")),
    ClinicalImpressionOutput(id = "CI0004", `patient_id` = "PA0004", observations = List("OB0004"))
  ).toDF()

  val observationsDf: DataFrame = Seq(
    ObservationOutput(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0099", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    ObservationOutput(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    ObservationOutput(id = "OB0004", patient_id = "PA0004", `observation_code` = "DSTA", `interpretation_code` = "affected"),
  ).toDF()
  val serviceRequestDf: DataFrame = Seq(
    ServiceRequestOutput(service_request_type = "analysis", `id` = "SRA0001", `patient_id` = "PA0001",
      family = Some(FAMILY(mother = Some("PA0003"), father = Some("PA0002"))),
      family_id = Some("FM00001"),
      `clinical_impressions` = Some(Seq("CI0001", "CI0002", "CI0003")),
      `service_request_description` = Some("Maladies musculaires (Panel global)")
    ),
    ServiceRequestOutput(service_request_type = "analysis", `id` = "SRA0002", `patient_id` = "PA0004",
      family = None,
      family_id = Some("FM00002"),
      `clinical_impressions` = Some(Seq("CI0004")),
      `service_request_description` = Some("Maladies musculaires (Panel global)")
    ),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0001", `patient_id` = "PA0001", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0002", `patient_id` = "PA0002", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0003", `patient_id` = "PA0003", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0004", `patient_id` = "PA0004", analysis_service_request_id = Some("SRA0002"), `service_request_description` = Some("Maladies musculaires (Panel global)"))
  ).toDF()

  val taskDf: DataFrame = Seq(
    TaskOutput(
      `id` = "73254",
      `patient_id` = "PA0001",
      `service_request_id` = "SRS0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `aliquot_id` = "1")
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA0002",
      `service_request_id` = "SRS0002",
      `experiment` = EXPERIMENT(`name` = "BAT1", `aliquot_id` = "2")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0003",
      `service_request_id` = "SRS0003",
      `experiment` = EXPERIMENT(`name` = "BAT1", `aliquot_id` = "3")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0004",
      `service_request_id` = "SRS0004",
      `experiment` = EXPERIMENT(`name` = "BAT1", `aliquot_id` = "4")
    )
  ).toDF


  val job1 = new Variants("BAT1")
  val job2 = new Variants("BAT2")

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_variant_calling.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.mainDestination.location)
  }


  val data: Map[String, DataFrame] = Map(
    raw_variant_calling.id -> Seq(
      VCF_SNV_Input(
        `genotypes` = List(
          SNV_GENOTYPES(`sampleId` = "1", `calls` = List(1, 1)),
          SNV_GENOTYPES(`sampleId` = "2", `calls` = List(1, 0)),
          SNV_GENOTYPES(`sampleId` = "3", `calls` = List(0, 0)),
          SNV_GENOTYPES(`sampleId` = "4", `calls` = List(-1, -1)),

        )),
      VCF_SNV_Input(
        referenceAllele = "G",
        `genotypes` = List(
          SNV_GENOTYPES(`sampleId` = "1", `calls` = List(1, 1), `alleleDepths` = List(10, 0)), //Should not be included in frequencies
          SNV_GENOTYPES(`sampleId` = "1", `calls` = List(1, 1), `conditionalQuality` = 10) //Should not be included in frequencies
        )),
      VCF_SNV_Input(
        referenceAllele = "A",
        INFO_FILTERS = List("DRAGENHardQUAL;LowDepth"), //Should not be included in frequencies
        `genotypes` = List(
          SNV_GENOTYPES(`sampleId` = "1", `calls` = List(1, 1), `alleleDepths` = List(0, 30)),
        ))
    ).toDF(),
    clinical_impression.id -> clinicalImpressionsDf,
    observation.id -> observationsDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf
  )

  "variants job" should "transform data in expected format" in {
    val results = job1.transform(data)
    val resultDf = results("normalized_variants")
    val result = resultDf.as[NormalizedVariants].collect()
    resultDf.columns.length shouldBe resultDf.as[NormalizedVariants].columns.length
    val variantWithFreq = result.find(_.`reference` == "T")
    variantWithFreq.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)", Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1)),
      `created_on` = null)
    )

    val variantWithoutFreqG = result.find(_.`reference` == "G")
    variantWithoutFreqG.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      reference= "G",
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)",Frequency(0,4,0.0,0,2,0.0,0),Frequency(0,0,0.0,0,0,0.0,0),Frequency(0,4,0.0,0,2,0.0,0))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(0,4,0.0,0,2,0.0,0), Frequency(0, 0, 0, 0, 0, 0, 0), Frequency(0,4,0.0,0,2,0.0,0)),
      `created_on` = null)
    )

    val variantWithoutFreqA = result.find(_.`reference` == "A")
    variantWithoutFreqA.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      reference= "A",
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG","Maladies musculaires (Panel global)",Frequency(0,2,0.0,0,1,0.0,0),Frequency(0,0,0.0,0,0,0.0,0),Frequency(0,2,0.0,0,1,0.0,0))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(0,2,0.0,0,1,0.0,0),Frequency(0,0,0.0,0,0,0.0,0),Frequency(0,2,0.0,0,1,0.0,0)),
      `created_on` = null)
    )



  }
}
