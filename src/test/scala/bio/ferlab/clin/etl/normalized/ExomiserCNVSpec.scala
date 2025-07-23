package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.RawExomiserCNV
import bio.ferlab.clin.etl.utils.FileInfo
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedExomiserCNV
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll

class ExomiserCNVSpec extends SparkSpec with WithTestConfig with BeforeAndAfterAll with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser_cnv")
  val raw_exomiser_cnv: DatasetConf = conf.getDataset("raw_exomiser_cnv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job1 = ExomiserCNV(TestETLContext(), Seq("SRA0001", "SRA0002"))
  val job2 = ExomiserCNV(TestETLContext(), Seq("SRA0003"))

  val resourcePath: String = this.getClass.getClassLoader.getResource(".").getFile

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `aliquot_id` = "aliquot1", `exomiser_cnv_urls` = Some(Set(s"file://${resourcePath}BAT1/SRA0001.exomiser.cnv.variants.tsv")), `patient_id` = "438787", `sequencing_id` = "SR0095"),
    EnrichedClinical(`batch_id` = "BAT2", `analysis_id` = "SRA0002", `aliquot_id` = "aliquot2", `exomiser_cnv_urls` = Some(Set(s"file://${resourcePath}BAT2/SRA0002.exomiser.cnv.variants.tsv"))),
  ).toDF()

  override val dbToCreate: List[String] = List(enriched_clinical.table.get.database)
  override val dsToClean: List[DatasetConf] = List(enriched_clinical)

  override def beforeAll(): Unit = {
    super.beforeAll()
    LoadResolver
      .write(spark, conf)(enriched_clinical.format -> enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)
  }

  it should "extract all files matching the analysis ids with their info" in {
    val result = job1.extract()

    result(raw_exomiser_cnv.id)
      .as[RawExomiserCNV]
      .withColumn("input_file_name", input_file_name())
      .groupBy("input_file_name")
      .count()
      .count() shouldBe 2

    result("file_info")
      .as[FileInfo]
      .count() shouldBe 2
  }

it should "not fail when there is no exomiser data for the given analysis ids" in {
    val result = job2.extract()

    result(raw_exomiser_cnv.id)
      .as[RawExomiserCNV]
      .withColumn("input_file_name", input_file_name())
      .groupBy("input_file_name")
      .count()
      .count() shouldBe 0
  }

  it should "normalize exomiser CNV data" in {
    val data = job1.extract()
    val result = job1.transformSingle(data)

    result
      .as[NormalizedExomiserCNV]
      .select("batch_id")
      .distinct()
      .as[String]
      .collect() should contain theSameElementsAs Seq("BAT1", "BAT2")

    result
      .as[NormalizedExomiserCNV]
      .select("analysis_id")
      .distinct()
      .as[String]
      .collect() should contain theSameElementsAs Seq("SRA0001", "SRA0002")
  }

}
