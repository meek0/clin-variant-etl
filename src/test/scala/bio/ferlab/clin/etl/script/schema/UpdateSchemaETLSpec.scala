package bio.ferlab.clin.etl.script.schema

import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, Format, LoadType, RunStep, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.{DuplicateColumn, Transformation, UpperCase}
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.{DataFrame, Row}


import java.nio.file.{Files, Paths}

class UpdateSchemaETLSpec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  lazy implicit val conf: SimpleConfiguration = SimpleConfiguration(
    DatalakeConf(
      storages = List(
        StorageConf("storage1", this.getClass.getClassLoader.getResource(".").getFile, FileSystemType.LOCAL)
      ),
      sources = List(
        DatasetConf(id = "dataset1", storageid = "storage1", path = "schemaUtilsSpec/dataset1/test.json", format = Format.JSON, loadtype = LoadType.Read, table = Some(TableConf("test", "dataset1")))
     )
    )
  )

  override val dbToCreate: List[String] = conf.sources.map(_.table).flatten.map(_.database).toList
  override val dsToClean: List[DatasetConf] = conf.sources.filter(_.table.isDefined)

  "UpdateSchemaETL" should "apply transformations to the source dataset" in {
    val dataset1 = conf.getDataset("dataset1")

    val inputDf = Seq(
      ("a1", "b1", "c1"),
      ("a2", "b2", "c2")
    ).toDF("a", "b", "c")
    val data = Map(dataset1.id -> inputDf)

    val job = new UpdateSchemaETL(
      TestETLContext(),
      dataset1,
      List(
        DuplicateColumn("b", "b_duplicate"),
        UpperCase("a")
      )
    )
    val resultDf = job.transformSingle(data)

    // Verify that the transformations has been applied correctly
    resultDf.columns shouldBe Seq("a", "b", "c", "b_duplicate")
    resultDf.collect() should contain theSameElementsAs Array(
      Row("A1", "b1", "c1", "b1"),
      Row("A2", "b2", "c2", "b2")
    )
  }

  "UpdateSchemaETL" should "overwrite the loadtype and write options in destination dataset" in {
    val dataset1 = conf.getDataset("dataset1")

    val job = new UpdateSchemaETL(
      TestETLContext(),
      dataset1,
      List()
    )

    job.mainDestination.loadtype shouldBe LoadType.OverWrite
    job.mainDestination.writeoptions shouldBe Map("overwriteSchema" -> "true")
  }

  "UpdateSchemaETL" should "skip the reset" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset1
      write(updatedConf, "dataset1", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))

      val context = TestETLContext(RunStep.initial_load)(updatedConf, spark)
      val dataset = context.config.getDataset("dataset1")
      val job = new UpdateSchemaETL(
        context,
        dataset,
        List()
      )
      job.reset()

      // Verify that the reset is skipped
      val df = dataset.read(updatedConf, spark)
      df.collect() should contain theSameElementsAs Array(Row("a1", "b1", "c1"))
      df.columns shouldBe Seq("a", "b", "c")
     }
  }

  "UpdateSchemaETL" should "load the dataset correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val dataset = context.config.getDataset("dataset1")
      val job = new UpdateSchemaETL(
        context,
        dataset,
        List()
      )
      val data = Map(dataset.id -> Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))
      job.load(data)

      // Verify that the dataset is loaded correctly
      val df = dataset.read(updatedConf, spark)
      df.collect() should contain theSameElementsAs Array(Row("a1", "b1", "c1"))
      df.columns shouldBe Seq("a", "b", "c")
    }
  }

  private def write(conf: Configuration, datasetId: String, df: DataFrame): Unit = {
    val dataset = conf.getDataset(datasetId)
    LoadResolver.write(spark, conf)(dataset.format, LoadType.OverWrite).apply(dataset, df)
  }
}
