package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    ))

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")

  val clinicalImpressionsDf = Seq(
    ClinicalImpressionOutput(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB0099")),
    ClinicalImpressionOutput(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    ClinicalImpressionOutput(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003")),
    ClinicalImpressionOutput(id = "CI0004", `patient_id` = "PA0004", observations = List("OB0004"))
  ).toDF()

  val observationsDf = Seq(
    ObservationOutput(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "POS"),
    ObservationOutput(id = "OB0099", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "POS"),
    ObservationOutput(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "NEG"),
    ObservationOutput(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "NEG"),
    ObservationOutput(id = "OB0004", patient_id = "PA0004", `observation_code` = "DSTA", `interpretation_code` = "POS"),
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
      `experiment` = EXPERIMENT(`aliquot_id` = "1")
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA0002",
      `service_request_id` = "SRS0002",
      `experiment` = EXPERIMENT(`aliquot_id` = "2")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0003",
      `service_request_id` = "SRS0003",
      `experiment` = EXPERIMENT(`aliquot_id` = "3")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0004",
      `service_request_id` = "SRS0004",
      `experiment` = EXPERIMENT(`aliquot_id` = "4")
    )
  ).toDF


  val job1 = new Variants("BAT1")
  val job2 = new Variants("BAT2")

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_variant_calling.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.destination.location)
  }


  val data = Map(
    raw_variant_calling.id -> Seq(
      VCFInput(
        `genotypes` = List(
          GENOTYPES(`sampleId` = "1", `calls` = List(1, 1)),
          GENOTYPES(`sampleId` = "2", `calls` = List(1, 0)),
          GENOTYPES(`sampleId` = "3", `calls` = List(0, 0)),
          GENOTYPES(`sampleId` = "4", `calls` = List(-1, -1)),

        )),
      VCFInput(
        referenceAllele = "G",
        `genotypes` = List(
          GENOTYPES(`sampleId` = "1", `calls` = List(1, 1), `alleleDepths` = List(10, 0)), //Should not be included in frequencies
          GENOTYPES(`sampleId` = "1", `calls` = List(1, 1), `conditionalQuality` = 10) //Should not be included in frequencies
        )),
      VCFInput(
        referenceAllele = "A",
        INFO_FILTERS = List("DRAGENHardQUAL;LowDepth"), //Should not be included in frequencies
        `genotypes` = List(
          GENOTYPES(`sampleId` = "1", `calls` = List(1, 1)),
        ))
    ).toDF(),
    clinical_impression.id -> clinicalImpressionsDf,
    observation.id -> observationsDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf
  )

  "variants job" should "transform data in expected format" in {

    val resultDf = job1.transform(data)
    val result = resultDf.as[NormalizedVariants].collect()
    resultDf.columns.length shouldBe resultDf.as[NormalizedVariants].columns.length
    val variantWithFreq = result.find(_.`reference` == "T")
    variantWithFreq.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)", Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1)),
      `created_on` = null)
    )
    val emptyFrequency = Frequency(0, 0, 0, 0, 0, 0, 0)
    val emptyFrequencies = AnalysisFrequencies(emptyFrequency, emptyFrequency, emptyFrequency)

    val variantWithoutFreqG = result.find(_.`reference` == "G")
    variantWithoutFreqG.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      reference= "G",
      `frequencies_by_analysis` = List.empty[AnalysisCodeFrequencies],
      `frequency_RQDM` = emptyFrequencies,
      `created_on` = null)
    )

    val variantWithoutFreqA = result.find(_.`reference` == "A")
    variantWithoutFreqA.map(_.copy(`created_on` = null)) shouldBe Some(NormalizedVariants(
      reference= "A",
      `frequencies_by_analysis` = List.empty[AnalysisCodeFrequencies],
      `frequency_RQDM` = emptyFrequencies,
      `created_on` = null)
    )



  }
}
