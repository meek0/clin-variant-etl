package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.RawCoverageByGene
import bio.ferlab.clin.etl.utils.FileInfo
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedCoverageByGene
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterEach

class CoverageByGeneSpec extends SparkSpec with WithTestConfig with BeforeAndAfterEach with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_coverage_by_gene")
  val raw_coverage_by_gene: DatasetConf = conf.getDataset("raw_coverage_by_gene")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job1 = CoverageByGene(TestETLContext(), "BAT1")
  val job2 = CoverageByGene(TestETLContext(), "BAT2")

  val resourcePath: String = this.getClass.getClassLoader.getResource(".").getFile

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `aliquot_id` = "aliquot1", `covgene_urls` = Some(Set(s"file://${resourcePath}BAT1/aliquot1.coverage_by_gene.GENCODE_CODING_CANONICAL.csv")), `patient_id` = "438787", `sequencing_id` = "SR0095"),
    EnrichedClinical(`batch_id` = "BAT2", `analysis_id` = "SRA0002", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "aliquot2", `covgene_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot2.coverage_by_gene.GENCODE_CODING_CANONICAL.csv")), `patient_id` = "PATIENT1", `sequencing_id` = "SR0001", `specimen_id` = "SPECIMEN1"),
    EnrichedClinical(`batch_id` = "BAT2", `analysis_id` = "SRA0003", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "aliquot3", `covgene_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot3.coverage_by_gene.GENCODE_CODING_CANONICAL.csv")), `patient_id` = "PATIENT2", `sequencing_id` = "SR0002", `specimen_id` = "SPECIMEN2"),
    EnrichedClinical(`batch_id` = "BAT3", `analysis_id` = "SRA0002", `bioinfo_analysis_code` = "TNEBA", `aliquot_id` = "aliquot2", `covgene_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot3.coverage_by_gene.GENCODE_CODING_CANONICAL.csv")), `patient_id` = "PATIENT1", `sequencing_id` = "SR0001", `specimen_id` = "SPECIMEN1"),
  ).toDF()

  override val dbToCreate: List[String] = List(enriched_clinical.table.get.database)
  override val dsToClean: List[DatasetConf] = List(enriched_clinical, mainDestination)

  override def beforeEach(): Unit = {
    super.beforeEach()
    LoadResolver
      .write(spark, conf)(enriched_clinical.format -> enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)
  }

  it should "extract all files from the batch with its info" in {
    val result = job2.extract()

    result(raw_coverage_by_gene.id)
      .as[RawCoverageByGene]
      .withColumn("input_file_name", input_file_name())
      .groupBy("input_file_name")
      .count()
      .count() shouldBe 2

    result("file_info")
      .as[FileInfo]
      .collect() should contain theSameElementsAs Seq(
      FileInfo(
        `batch_id` = "BAT2", `analysis_id` = "SRA0002", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "aliquot2",
        `patient_id` = "PATIENT1", `specimen_id` = "SPECIMEN1", `sequencing_id` = "SR0001",
        `is_proband` = true, `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
        url = s"file://${resourcePath}BAT2/aliquot2.coverage_by_gene.GENCODE_CODING_CANONICAL.csv"
      ),
      FileInfo(
        `batch_id` = "BAT2", `analysis_id` = "SRA0003", `bioinfo_analysis_code` = "TEBA", `aliquot_id` = "aliquot3",
        `patient_id` = "PATIENT2", `specimen_id` = "SPECIMEN2", `sequencing_id` = "SR0002",
        `is_proband` = true, `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
        url = s"file://${resourcePath}BAT2/aliquot3.coverage_by_gene.GENCODE_CODING_CANONICAL.csv"
      )
    )
  }

  it should "normalize coverage by gene data" in {
    val data = job1.extract()
    val result = job1.transformSingle(data)

    val all = result.as[NormalizedCoverageByGene]

    all.select("aliquot_id")
      .distinct()
      .as[String]
      .collect() should contain theSameElementsAs Seq("aliquot1")

    all.head() shouldBe NormalizedCoverageByGene()
  }

  it should "not fail when there is no coverage data in batch" in {
    val job = CoverageByGene(TestETLContext(default_load), "NODATA")
    noException should be thrownBy job.run()
  }


  "load" should "overwrite the right partitions" in {
    withOutputFolder("root") { root =>

      val updatedConf = updateConfStorages(conf, root)

      val context = TestETLContext()(updatedConf, spark)

      // Using data with same aliquot id and analysis id, but different batches
      val normalizedCoverageByGeneData1 = NormalizedCoverageByGene(`batch_id` = "BAT3", `analysis_id` = "SRA0004", `aliquot_id` = "aliquot1", `bioinfo_analysis_code` = "TEBA")
      val normalizedCoverageByGeneData2 = NormalizedCoverageByGene(`batch_id` = "BAT4", `analysis_id` = "SRA0004", `aliquot_id` = "aliquot1", `bioinfo_analysis_code` = "TNEBA")


      CoverageByGene(context, "BAT3").load(
        Map(mainDestination.id -> Seq(normalizedCoverageByGeneData1).toDF())
      )
      mainDestination.read(updatedConf, spark).as[NormalizedCoverageByGene].collect() should contain theSameElementsAs Seq(
        normalizedCoverageByGeneData1
      )

      // When saving the data for BATCH 4, it should not overwrite the data for BATCH3 even if the analysis id is the same
      CoverageByGene(context, "BAT4").load(
        Map(mainDestination.id -> Seq(normalizedCoverageByGeneData2).toDF())
      )
      mainDestination.read(updatedConf, spark).as[NormalizedCoverageByGene].collect() should contain theSameElementsAs Seq(
        normalizedCoverageByGeneData1,
        normalizedCoverageByGeneData2
      )
    }
  }


}
