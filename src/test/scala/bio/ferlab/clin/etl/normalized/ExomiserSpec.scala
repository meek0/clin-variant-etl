package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw.RawExomiser
import bio.ferlab.clin.etl.utils.FileInfo
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.normalized.NormalizedExomiser
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ExomiserSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")
  val normalized_task: DatasetConf = conf.getDataset("normalized_task")
  val normalized_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")

  val job1 = new Exomiser("BAT1")
  val job2 = new Exomiser("BAT2")

  val resourcePath: String = this.getClass.getClassLoader.getResource(".").getFile

  val taskDf: DataFrame = Seq(
    TaskOutput(
      experiment = EXPERIMENT(name = "BAT1", aliquot_id = "aliquot1"),
      documents = List(
        DOCUMENTS(id = "exo1", document_type = "EXOMISER")
      )
    ),
    TaskOutput(
      experiment = EXPERIMENT(name = "BAT2", aliquot_id = "aliquot2"),
      documents = List(
        DOCUMENTS(id = "exo2", document_type = "EXOMISER")
      )
    ),
    TaskOutput(
      experiment = EXPERIMENT(name = "BAT2", aliquot_id = "aliquot3"),
      documents = List(
        DOCUMENTS(id = "exo3", document_type = "EXOMISER")
      )
    )
  ).toDF()

  val documentDf: DataFrame = Seq(
    DocumentReferenceOutput(id = "exo1", contents = List(
      Content(s3_url = s"file://${resourcePath}BAT1/aliquot1.exomiser.variants.tsv", format = "TSV")
    )),
    DocumentReferenceOutput(id = "exo2", contents = List(
      Content(s3_url = s"file://${resourcePath}BAT2/aliquot2.exomiser.variants.tsv", format = "TSV")
    )),
    DocumentReferenceOutput(id = "exo3", contents = List(
      Content(s3_url = s"file://${resourcePath}BAT2/aliquot3.exomiser.variants.tsv", format = "TSV")
    )),
  ).toDF()

  override def beforeAll(): Unit = {
    normalized_task.table.foreach(table => spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}"))
    LoadResolver
      .write(spark, conf)(normalized_task.format -> normalized_task.loadtype)
      .apply(normalized_task, taskDf)

    normalized_document_reference.table.foreach(table => spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}"))
    LoadResolver
      .write(spark, conf)(normalized_document_reference.format -> normalized_document_reference.loadtype)
      .apply(normalized_document_reference, documentDf)
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS ${normalized_task.table.get.name}")
    Try(HadoopFileSystem.remove(normalized_task.location))

    spark.sql(s"DROP TABLE IF EXISTS ${normalized_document_reference.table.get.name}")
    Try(HadoopFileSystem.remove(normalized_document_reference.location))
  }

  it should "extract all files from the batch with its info" in {
    val result = job2.extract()(spark)

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
    val job = new Exomiser("NODATA")
    noException should be thrownBy job.run()
  }
}
