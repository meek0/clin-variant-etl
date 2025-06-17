package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.RawExomiser
import bio.ferlab.clin.etl.utils.FileInfo
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedExomiser
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll

class ExomiserSpec extends SparkSpec with WithTestConfig with BeforeAndAfterAll with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job1 = Exomiser(TestETLContext(), "BAT1")
  val job2 = Exomiser(TestETLContext(), "BAT2")

  val resourcePath: String = this.getClass.getClassLoader.getResource(".").getFile

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`batch_id` = "BAT1", `aliquot_id` = "aliquot1", `analysis_id` = "SRA0001", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT1/aliquot1.exomiser.variants.tsv")), `patient_id` = "438787", `sequencing_id` = "SR0095"),
    EnrichedClinical(`batch_id` = "BAT2", `aliquot_id` = "aliquot2", `analysis_id` = "SRA0002", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot2.exomiser.variants.tsv")), `patient_id` = "PATIENT1", `sequencing_id` = "SR0001", `specimen_id` = "SPECIMEN1"),
    EnrichedClinical(`batch_id` = "BAT2", `aliquot_id` = "aliquot3", `analysis_id` = "SRA0003", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot3.exomiser.variants.tsv")), `patient_id` = "PATIENT2", `sequencing_id` = "SR0002", `specimen_id` = "SPECIMEN2"),
  ).toDF()

  override val dbToCreate: List[String] = List(enriched_clinical.table.get.database)
  override val dsToClean: List[DatasetConf] = List(enriched_clinical)

  override def beforeAll(): Unit = {
    super.beforeAll()
    LoadResolver
      .write(spark, conf)(enriched_clinical.format -> enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)
  }

  it should "extract all files from the batch with its info" in {
    val result = job2.extract()

    result(raw_exomiser.id)
      .as[RawExomiser]
      .withColumn("input_file_name", input_file_name())
      .groupBy("input_file_name")
      .count()
      .count() shouldBe 2

    result("file_info")
      .as[FileInfo]
      .collect() should contain theSameElementsAs Seq(
      FileInfo(
        `batch_id` = "BAT2", `analysis_id` = "SRA0002", `aliquot_id` = "aliquot2",
        `patient_id` = "PATIENT1", `specimen_id` = "SPECIMEN1", `sequencing_id` = "SR0001",
        `is_proband` = true, `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
        url = s"file://${resourcePath}BAT2/aliquot2.exomiser.variants.tsv"
      ),
      FileInfo(
        `batch_id` = "BAT2", `analysis_id` = "SRA0003", `aliquot_id` = "aliquot3",
        `patient_id` = "PATIENT2", `specimen_id` = "SPECIMEN2", `sequencing_id` = "SR0002",
        `is_proband` = true, `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
        url = s"file://${resourcePath}BAT2/aliquot3.exomiser.variants.tsv"
      )
    )
  }

  it should "normalize exomiser data" in {
    val data = job1.extract()
    val result = job1.transformSingle(data)

    result
      .as[NormalizedExomiser]
      .collect() should contain theSameElementsAs Seq(
      NormalizedExomiser(`chromosome` = "5", `start` = 133097215, `end` = 133097215, `reference` = "G", `alternate` = "T", `id` = "5-133097215-G-T_AD", `aliquot_id` = "aliquot1", `analysis_id` = "SRA0001", `rank` = 1, `gene_symbol` = "HSPA4", `entrez_gene_id` = 3308, `exomiser_variant_score` = 0.9948f, `gene_combined_score` = 0.9583f, `contributing_variant` = true, `moi` = "AD", `acmg_classification` = "UNCERTAIN_SIGNIFICANCE", `acmg_evidence` = null, `batch_id` = "BAT1"),
      NormalizedExomiser(`chromosome` = "19", `start` = 17267523, `end` = 17267523, `reference` = "C", `alternate` = "A", `id` = "19-17267523-C-A_AR", `aliquot_id` = "aliquot1", `analysis_id` = "SRA0001", `rank` = 2, `gene_symbol` = "BABAM1", `entrez_gene_id` = 29086, `exomiser_variant_score` = 0.8780f, `gene_combined_score` = 0.9153f, `contributing_variant` = true, `moi` = "AR", `acmg_classification` = "UNCERTAIN_SIGNIFICANCE", `acmg_evidence` = null, `batch_id` = "BAT1"),
      NormalizedExomiser(`chromosome` = "X", `start` = 111736800, `end` = 111736800, `reference` = "A", `alternate` = "G", `id` = "X-111736800-A-G_XR", `aliquot_id` = "aliquot1", `analysis_id` = "SRA0001", `rank` = 3, `gene_symbol` = "ALG13", `entrez_gene_id` = 79868, `exomiser_variant_score` = 0.6581f, `gene_combined_score` = 0.9008f, `contributing_variant` = true, `moi` = "XR", `acmg_classification` = "UNCERTAIN_SIGNIFICANCE", `acmg_evidence` = Seq("PP4", "BP6"), `batch_id` = "BAT1"),
      NormalizedExomiser(`chromosome` = "4", `start` = 16595819, `end` = 16595819, `reference` = "T", `alternate` = "C", `id` = "4-16595819-T-C_AD", `aliquot_id` = "aliquot1", `analysis_id` = "SRA0001", `rank` = 4, `gene_symbol` = "LDB2", `entrez_gene_id` = 9079, `exomiser_variant_score` = 0.9962f, `gene_combined_score` = 0.7781f, `contributing_variant` = true, `moi` = "AD", `acmg_classification` = "UNCERTAIN_SIGNIFICANCE", `acmg_evidence` = null, `batch_id` = "BAT1")
    )
  }

  it should "not fail when there is no exomiser data in batch" in {
    val job = Exomiser(TestETLContext(default_load), "NODATA")
    noException should be thrownBy job.run()
  }
}
