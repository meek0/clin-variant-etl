package bio.ferlab.clin.etl.script.schema

import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, Format, LoadType, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.{DuplicateColumn, Transformation, UpperCase}
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.{DataFrame, Row}

import java.nio.file.{Files, Paths}

class UpdateSchemaETLSpec extends SparkSpec {

  lazy implicit val conf: SimpleConfiguration = SimpleConfiguration(
    DatalakeConf(
      storages = List(
        StorageConf("database1", this.getClass.getClassLoader.getResource(".").getFile, FileSystemType.LOCAL)
      ),
      sources = List(
        DatasetConf(
          id = "dataset1",
          storageid = "database1",
          path = "/tables/table1",
          format = Format.DELTA,
          loadtype = LoadType.Upsert,
          table = Some(TableConf("database1", "table1"))
        )
      )
    )
  )

  "UpdateSchemaETL" should "apply transformations to the source dataset" in {
    val dataset1 = conf.getDataset("dataset1")

    val inputDf = spark.createDataFrame(Seq(
      ("a1", "b1", "c1"),
      ("a2", "b2", "c2")
    )).toDF("a", "b", "c")
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

}
