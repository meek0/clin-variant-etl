package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.{SNV_GENOTYPES, SNV_SOMATIC_GENOTYPES, VCF_SNV_Input, VCF_SNV_Somatic_Input}
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedVariants
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, DeprecatedTestETLContext, SparkSpec}
import org.apache.spark.sql.DataFrame

class VariantsSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`patient_id` = "PA0001", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0001", `batch_id` = "BAT1", `bioinfo_analysis_code` = "GEAN", `aliquot_id` = "1", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = true, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "1", `specimen_id` = "1", `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002")),
    EnrichedClinical(`patient_id` = "PA0002", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0002", `batch_id` = "BAT1", `bioinfo_analysis_code` = "GEAN", `aliquot_id` = "2", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "2", `specimen_id` = "2", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None),
    EnrichedClinical(`patient_id` = "PA0003", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0003", `batch_id` = "BAT1", `bioinfo_analysis_code` = "GEAN", `aliquot_id` = "3", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false, `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "3", `specimen_id` = "3", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None),

    EnrichedClinical(`patient_id` = "PA0004", `analysis_service_request_id` = "SRA0002", `service_request_id` = "SRS0004", `batch_id` = "BAT1", `bioinfo_analysis_code` = "GEAN", `aliquot_id` = "4", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = true, `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "4", `specimen_id` = "4", `family_id` = Some("FM00002"), `mother_id` = None, `father_id` = None),
    EnrichedClinical(`patient_id` = "PA0004", `analysis_service_request_id` = "SRA0002", `service_request_id` = "SRS0004", `batch_id` = "BAT1", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "5", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = true, `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "5", `specimen_id` = "5", `family_id` = Some("FM00002"), `mother_id` = None, `father_id` = None),
  ).toDF()

  val job1 = Variants(DeprecatedTestETLContext(), "BAT1")
  val job2 = Variants(DeprecatedTestETLContext(), "BAT2")

  override val dbToCreate: List[String] = List(raw_variant_calling.table.map(_.database).getOrElse("clin"))
  override val dsToClean: List[DatasetConf] = List(job1.mainDestination)

  val data: Map[String, DataFrame] = Map(
    raw_variant_calling.id -> Seq(
      VCF_SNV_Input(
        referenceAllele = "T",
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
    enriched_clinical.id -> clinicalDf
  )

  val dataSomatic: Map[String, DataFrame] = data ++ Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Somatic_Input(
      `genotypes` = List(
        SNV_SOMATIC_GENOTYPES(`sampleId` = "5", `calls` = List(1, 1)),
      ))).toDF(),
  )

  val dataSomaticWithDuplicates: Map[String, DataFrame] = data ++ Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Somatic_Input(), VCF_SNV_Somatic_Input(), VCF_SNV_Somatic_Input()).toDF(),
  )

  "variants job" should "transform data in expected format" in {
    val results = job1.transform(data)
    val resultDf = results("normalized_variants")
    val result = resultDf.as[NormalizedVariants].collect()
    result.length shouldBe 3
    resultDf.columns.length shouldBe resultDf.as[NormalizedVariants].columns.length
    val variantWithFreq = result.find(_.`reference` == "T")
    variantWithFreq.map(_.copy(`created_on` = null)) shouldBe Some(normalized.NormalizedVariants(
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)", Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(2, 4, 0.5, 1, 2, 0.5, 1), Frequency(1, 4, 0.25, 1, 2, 0.5, 0), Frequency(3, 8, 0.375, 2, 4, 0.5, 1)),
      `created_on` = null)
    )

    val variantWithoutFreqG = result.find(_.`reference` == "G")
    variantWithoutFreqG.map(_.copy(`created_on` = null)) shouldBe Some(normalized.NormalizedVariants(
      reference = "G",
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)", Frequency(0, 4, 0.0, 0, 2, 0.0, 0), Frequency(0, 0, 0.0, 0, 0, 0.0, 0), Frequency(0, 4, 0.0, 0, 2, 0.0, 0))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(0, 4, 0.0, 0, 2, 0.0, 0), Frequency(0, 0, 0, 0, 0, 0, 0), Frequency(0, 4, 0.0, 0, 2, 0.0, 0)),
      `created_on` = null)
    )

    val variantWithoutFreqA = result.find(_.`reference` == "A")
    variantWithoutFreqA.map(_.copy(`created_on` = null)) shouldBe Some(normalized.NormalizedVariants(
      reference = "A",
      `frequencies_by_analysis` = List(AnalysisCodeFrequencies("MMG", "Maladies musculaires (Panel global)", Frequency(0, 2, 0.0, 0, 1, 0.0, 0), Frequency(0, 0, 0.0, 0, 0, 0.0, 0), Frequency(0, 2, 0.0, 0, 1, 0.0, 0))),
      `frequency_RQDM` = AnalysisFrequencies(Frequency(0, 2, 0.0, 0, 1, 0.0, 0), Frequency(0, 0, 0.0, 0, 0, 0.0, 0), Frequency(0, 2, 0.0, 0, 1, 0.0, 0)),
      `created_on` = null)
    )
  }

  "variants job" should "transform somatic data to expected format" in {
    val results = job1.transform(dataSomatic)
    val resultDf = results("normalized_variants")
    val result = resultDf.as[NormalizedVariants].collect()
    result.length shouldBe 1
  }

  "variants job" should "not create duplicated variants freqs" in {
    val results = job1.transform(dataSomaticWithDuplicates)
    val resultDf = results("normalized_variants")
    val result = resultDf.as[NormalizedVariants].collect()

    result.length shouldBe 1
    resultDf.columns.length shouldBe resultDf.as[NormalizedVariants].columns.length

    result(0).`frequencies_by_analysis`.size shouldBe 1
    result(0).`frequency_RQDM`.total shouldBe Frequency(0, 0, 0.0, 0, 0, 0.0, 0)
    result(0).`frequency_RQDM`.affected shouldBe Frequency(0, 0, 0.0, 0, 0, 0.0, 0)
    result(0).`frequency_RQDM`.non_affected shouldBe Frequency(0, 0, 0.0, 0, 0, 0.0, 0)
  }

  "variants job" should "throw exception if no valid VCF" in {
    val exception = intercept[Exception] {
      job1.transform(data ++ Map(raw_variant_calling.id -> spark.emptyDataFrame))
    }
    exception.getMessage shouldBe "No valid raw VCF available"
  }

  "variants job" should "ignore invalid contigName in VCF Germnline" in {
    val results = job1.transform(data ++ Map(raw_variant_calling.id -> Seq(
      VCF_SNV_Input(`contigName` = "chr2"),
      VCF_SNV_Input(`contigName` = "chrY"),
      VCF_SNV_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_variants").as[NormalizedVariants].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

  "variants job" should "ignore invalid contigName in VCF Somatic" in {
    val results = job1.transform(data ++ Map(
      raw_variant_calling.id -> Seq(
        VCF_SNV_Somatic_Input(`contigName` = "chr2"),
        VCF_SNV_Somatic_Input(`contigName` = "chrY"),
        VCF_SNV_Somatic_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_variants").as[NormalizedVariants].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }
}
