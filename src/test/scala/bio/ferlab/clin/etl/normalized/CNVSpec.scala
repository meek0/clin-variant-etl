package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.VCF_CNV_Input
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedCNV
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class CNVSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  override val dsToClean: List[DatasetConf] = conf.sources.filter(_.table.isDefined)

  val raw_cnv: DatasetConf = conf.getDataset("raw_cnv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job = CNV(TestETLContext(), "BAT1")

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(
      `patient_id` = "PA0001", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001", `batch_id` = "BAT1",
      `aliquot_id` = "11111", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = true,
      `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"),
      `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_001", `specimen_id` = "SP_001",
      `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
      `mother_aliquot_id` = Some("33333"), `father_aliquot_id` = Some("22222")
    ),
    EnrichedClinical(
      `patient_id` = "PA0002", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0002", `batch_id` = "BAT1",
      `aliquot_id` = "22222", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false,
      `gender` = "Male", `analysis_display_name` = Some("Maladies musculaires (Panel global)"),
      `affected_status` = false, `affected_status_code` = "not_affected", `sample_id` = "SA_002",
      `specimen_id` = "SP_002", `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None,
      `mother_aliquot_id` = None, `father_aliquot_id` = None
    ),
    EnrichedClinical(
      `patient_id` = "PA0003", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0003", `batch_id` = "BAT1",
      `aliquot_id` = "33333", `practitioner_role_id` = "PPR00101", `organization_id` = "OR00201", `is_proband` = false,
      `gender` = "Female", `analysis_display_name` = Some("Maladies musculaires (Panel global)"),
      `affected_status` = true, `affected_status_code` = "affected", `sample_id` = "SA_003", `specimen_id` = "SP_003",
      `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None, `mother_aliquot_id` = None,
      `father_aliquot_id` = None
    ),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    raw_cnv.id -> Seq(VCF_CNV_Input()).toDF(),
    enriched_clinical.id -> clinicalDf
  )

  /*"occurrences transform" should "create the VCF_CNV_Input model" in {
    val cnv = spark.read.format("vcf").load("src/test/resources/test_json/cnv.vcf");
    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "VCF_CNV_Input", cnv, "src/test/scala/")
  }*/

  "cnv transform" should "transform data in expected format" in {
    val results = job.transformSingle(data)
    val result = results.as[NormalizedCNV].collect()

    result should contain theSameElementsAs Seq(
      NormalizedCNV(),
    )

    // ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedCNV", result, "src/test/scala/")
  }

  "cnv transform" should "ignore invalid contigName" in {
    val results = job.transform(data ++ Map(raw_cnv.id -> Seq(
      VCF_CNV_Input(`contigName` = "chr2"),
      VCF_CNV_Input(`contigName` = "chrY"),
      VCF_CNV_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_cnv").as[NormalizedCNV].collect()
    result.length shouldBe >(0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

  "load" should "save the data correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val normalizedCNVData = Seq(
        NormalizedCNV(
          `chromosome` = "chr2",
          `start` = 1000,
          `end` = 2000,
          `aliquot_id` = "11111",
          `batch_id` = "BAT1"
        ),
        NormalizedCNV(
          `chromosome` = "chr3",
          `start` = 100,
          `end` = 240,
          `aliquot_id` = "22222",
          `batch_id` = "BAT1"
        )
      )

      // Load
      val context = TestETLContext()(updatedConf, spark)
      val job = CNV(context, "BAT1")
      job.load(Map("normalized_cnv" -> normalizedCNVData.toDF))

      // Check the output data
      val result = updatedConf.getDataset("normalized_cnv").read(updatedConf, spark)
      result.as[NormalizedCNV].collect() should contain theSameElementsAs normalizedCNVData
    }
  }
}
