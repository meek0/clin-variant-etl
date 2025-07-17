package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.VCF_CNV_Somatic_Input
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedCNVSomaticTumorOnly
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class CNVSomaticTumorOnlySpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val raw_cnv: DatasetConf = conf.getDataset("raw_cnv_somatic_tumor_only")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job = CNVSomaticTumorOnly(TestETLContext(), "BAT1")

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`patient_id` = "PA0001", `analysis_id` = "SRA0001", `bioinfo_analysis_code` = "TEBA", `sequencing_id` = "SRS0001", `batch_id` = "BAT1", `aliquot_id` = "11111", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = true, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_001", `specimen_id` = "SP_001", `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"), `mother_aliquot_id` = Some("33333"), `father_aliquot_id` = Some("22222")),
    EnrichedClinical(`patient_id` = "PA0002", `analysis_id` = "SRA0001", `bioinfo_analysis_code` = "TEBA", `sequencing_id` = "SRS0002", `batch_id` = "BAT1", `aliquot_id` = "22222", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false, `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "SA_002", `specimen_id` = "SP_002", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
    EnrichedClinical(`patient_id` = "PA0003", `analysis_id` = "SRA0001", `bioinfo_analysis_code` = "TEBA", `sequencing_id` = "SRS0003", `batch_id` = "BAT1", `aliquot_id` = "33333", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false, `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"), `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_003", `specimen_id` = "SP_003", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None, `father_aliquot_id` = None),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    raw_cnv.id -> Seq(VCF_CNV_Somatic_Input()).toDF(),
    enriched_clinical.id -> clinicalDf
  )

  /*"occurrences transform" should "create the VCF_CNV_Input model" in {
    val cnv = spark.read.format("vcf").load("src/test/resources/test_json/cnv.vcf");
    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "VCF_CNV_Input", cnv, "src/test/scala/")
  }*/

  "cnv transform" should "transform data in expected format" in {
    val results = job.transform(data)
    val result = results("normalized_cnv_somatic_tumor_only").as[NormalizedCNVSomaticTumorOnly].collect()

    result should contain theSameElementsAs Seq(
      NormalizedCNVSomaticTumorOnly(),
    )

    // ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedCNV", result, "src/test/scala/")
  }

  "cnv transform" should "ignore invalid contigName" in {
    val results = job.transform(data ++ Map(raw_cnv.id -> Seq(
      VCF_CNV_Somatic_Input(`contigName` = "chr2"),
      VCF_CNV_Somatic_Input(`contigName` = "chrY"),
      VCF_CNV_Somatic_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_cnv_somatic_tumor_only").as[NormalizedCNVSomaticTumorOnly].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

  "load" should "save the data correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val normalizedCNVSomaticTumorOnlyData = Seq(
        NormalizedCNVSomaticTumorOnly()
      )

      // Load
      val context = TestETLContext()(updatedConf, spark)
      val job = CNVSomaticTumorOnly(context, "BAT1")
      job.load(Map("normalized_cnv_somatic_tumor_only" -> normalizedCNVSomaticTumorOnlyData.toDF))

      // Check the output data
      val result = updatedConf.getDataset("normalized_cnv_somatic_tumor_only").read(updatedConf, spark)
      result.as[NormalizedCNVSomaticTumorOnly].collect() should contain theSameElementsAs normalizedCNVSomaticTumorOnlyData
    }
  }
}
