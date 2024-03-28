package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.RawExomiser
import bio.ferlab.clin.etl.utils.FileInfo
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedExomiser
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, DeprecatedTestETLContext, SparkSpec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll

class ExomiserSpec extends SparkSpec with WithTestConfig with BeforeAndAfterAll with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val job1 = Exomiser(DeprecatedTestETLContext(), "BAT1")
  val job2 = Exomiser(DeprecatedTestETLContext(), "BAT2")

  val resourcePath: String = this.getClass.getClassLoader.getResource(".").getFile

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`batch_id` = "BAT1", `aliquot_id` = "aliquot1", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT1/aliquot1.exomiser.variants.tsv")), `patient_id` = "438787", `service_request_id` = "SR0095"),
    EnrichedClinical(`batch_id` = "BAT2", `aliquot_id` = "aliquot2", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot2.exomiser.variants.tsv"))),
    EnrichedClinical(`batch_id` = "BAT2", `aliquot_id` = "aliquot3", `exomiser_urls` = Some(Set(s"file://${resourcePath}BAT2/aliquot3.exomiser.variants.tsv"))),
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
      .count() shouldBe 2
  }

  it should "normalize exomiser data" in {
    val data = job1.extract()
    val result = job1.transformSingle(data)

    result
      .as[NormalizedExomiser]
      .select("aliquot_id")
      .distinct()
      .as[String]
      .collect() should contain theSameElementsAs Seq("aliquot1")
  }

  it should "not fail when there is no exomiser data in batch" in {
    val job = Exomiser(DeprecatedTestETLContext(), "NODATA")
    noException should be thrownBy job.run()
  }
}
