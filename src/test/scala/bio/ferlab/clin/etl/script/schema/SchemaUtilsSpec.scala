package bio.ferlab.clin.etl.script.schema

import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, Format, LoadType, RunStep, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.{Rename, Transformation, UpperCase}
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec}
import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

class SchemaUtilsSpec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  lazy implicit val conf: SimpleConfiguration = SimpleConfiguration(
    DatalakeConf(
      storages = List(
        StorageConf("storage1", this.getClass.getClassLoader.getResource(".").getFile, FileSystemType.LOCAL)
      ),
      sources = List(
        DatasetConf(
          id = "dataset1",
          storageid = "storage1",
          path = "schemaUtilsSpec/dataset1/test.json",
          format = Format.JSON,
          loadtype = LoadType.Read,
          table = Some(TableConf("test", "dataset1"))
        ),
        DatasetConf(
          id = "dataset2",
          storageid = "storage1",
          path = "schemaUtilsSpec/dataset2/test.json",
          format = Format.JSON,
          loadtype = LoadType.Read,
          table = Some(TableConf("test", "dataset2"))
        ),
        DatasetConf(
          id = "dataset3",
          storageid = "storage1",
          path = "schemaUtilsSpec/dataset3",
          format = Format.DELTA,
          loadtype = LoadType.Read,
          table = Some(TableConf("test", "dataset3"))
        ),
        DatasetConf(
          id = "dataset4",
          storageid = "storage1",
          path = "schemaUtilsSpec/dataset4",
          format = Format.DELTA,
          loadtype = LoadType.Read,
          table = Some(TableConf("test", "dataset4"))
        )
      )
    )
  )

  override val dbToCreate: List[String] = conf.sources.map(_.table).flatten.map(_.database).toList
  override val dsToClean: List[DatasetConf] = conf.sources.filter(_.table.isDefined)

  "runUpdateSchemaFor" should "apply dataset transformations correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset1 and dataset2
      write(updatedConf, "dataset1", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))
      write(updatedConf, "dataset2", Seq(("x1", "y1", "z1")).toDF("x", "y", "z"))

      // Call runUpdateSchemaFor
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val filter: String => Boolean = _ => true // Apply transformations to all datasets
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset1" -> List(Rename(Map("a" -> "z")), UpperCase("b")),
          "dataset2" -> List(Rename(Map("x" -> "a")))
        )
      }
      SchemaUtils.runUpdateSchemaFor(context, filter, mappings)

      // Verify that the transformations have been applied correctly on dataset1
      val dataset1 = context.config.getDataset("dataset1")
      val df1 = dataset1.read(updatedConf, spark)
      df1.collect() should contain theSameElementsAs Array(Row("a1", "B1", "c1"))
      df1.columns shouldBe Seq("z", "b", "c")

      // Verify that dataset2 remains unchanged
      val dataset2 = context.config.getDataset("dataset2")
      val df2 = dataset2.read(updatedConf, spark)
      df2.columns shouldBe Seq("a", "y", "z")
    }
  }

  "runUpdateSchemaFor" should "not apply transformations if the filter does not match" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset1
      write(updatedConf, "dataset1", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))

      // Call runUpdateSchemaFor with a filter that does not match dataset1
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val filter: String => Boolean = _ == "dataset2" // No matching dataset
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset1" -> List(Rename(Map("a" -> "z")), UpperCase("b"))
        )
      }
      SchemaUtils.runUpdateSchemaFor(context, filter, mappings)

      // Verify that dataset1 remains unchanged
      val dataset1 = context.config.getDataset("dataset1")
      val df1 = dataset1.read(updatedConf, spark)
      df1.collect() should contain theSameElementsAs Array(Row("a1", "b1", "c1"))
      df1.columns shouldBe Seq("a", "b", "c")
    }
  }

  "runUpdateSchemaFor" should "try to process all datasets even if some fail" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset1 and dataset2
      write(updatedConf, "dataset1", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))
      write(updatedConf, "dataset2", Seq(("x1", "y1", "z1")).toDF("x", "y", "z"))

      // Call runUpdateSchemaFor with a transformation that will fail
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val filter: String => Boolean = _ => true // Apply transformations to all datasets
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset1" -> List(UpperCase("foo")), // This will fail
          "dataset2" -> List(Rename(Map("x" -> "a")))
        )
      }
      an[AnalysisException] should be thrownBy {
        SchemaUtils.runUpdateSchemaFor(context, filter, mappings)
      }

      // Dataset2 should still be processed even if dataset1 fails
      val dataset2 = context.config.getDataset("dataset2")
      val df2 = dataset2.read(updatedConf, spark)
      df2.columns shouldBe Seq("a", "y", "z")
    }
  }

// It is difficult to fully test vacuuming behavior here because datalake-lib set a
// retention period of at least 2 weeks. If the retention restriction is lifted, consider adding
// tests for more code branches (with filter, etc.).
  "runVacuumFor" should "apply vacuum on specified datasets" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset3
      write(updatedConf, "dataset3", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))

      // Create and write a new version for dataset3
      write(updatedConf, "dataset3", Seq(("a2", "b2", "c2")).toDF("a", "b", "c"))

      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      SchemaUtils.runVacuumFor(Seq("dataset3"), numberOfVersions = 1)(spark, updatedConf)

      //  For now we cannot verify the vacuum operation as it only remove files older than 2 weeks.
      // However, we can check that the Delta table still exists and contain the expected data
      val dataset = context.config.getDataset("dataset3")
      val df = dataset.read(updatedConf, spark)
      df.collect() should contain theSameElementsAs Array(Row("a2", "b2", "c2"))
      df.columns shouldBe Seq("a", "b", "c")
    }
  }

  /*
    Full testing of vacuum behavior is limited by datalake-lib’s minimum retention period of 2 weeks.
    This test only verifies that the vacuum code runs without exceptions and covers a single code branch
    (vacuum=true, no schema update error). If the retention restriction is lifted, consider adding
    tests for other code branches.
   */
  "runSchemaUpdateAndVacuum" should "update schema and vacuum datasets" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset3 and dataset4
      write(updatedConf, "dataset3", Seq(("x1", "y1", "z1")).toDF("x", "y", "z"))
      write(updatedConf, "dataset4", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))

      // Call runUpdateSchemaAndVacuum
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val isUpdatedFilter: String => Boolean = _ == "dataset3" // Schema update should be skipped for dataset3
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset3" -> List(Rename(Map("x" -> "a"))),
          "dataset4" -> List(Rename(Map("a" -> "z")))
        )
      }
      SchemaUtils.runUpdateSchemaAndVacuum(context, isUpdatedFilter, mappings, vacuum = true)

      // Verify that dataset3 remains unchanged
      val dataset3 = context.config.getDataset("dataset3")
      val df3 = dataset3.read(updatedConf, spark)
      df3.collect() should contain theSameElementsAs Array(Row("x1", "y1", "z1"))
      df3.columns shouldBe Seq("x", "y", "z")

      // Verify that dataset4 has been updated
      val dataset4 = context.config.getDataset("dataset4")
      val df4 = dataset4.read(updatedConf, spark)
      df4.collect() should contain theSameElementsAs Array(Row("a1", "b1", "c1"))
      df4.columns shouldBe Seq("z", "b", "c")
    }
  }

  "runUpdateSchemaAndVacuum" should "only log a description of the changes if dryrun is true" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset3
      write(updatedConf, "dataset3", Seq(("x1", "y1", "z1")).toDF("x", "y", "z"))

      // Call runUpdateSchemaAndVacuum with dryrun=true
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val isUpdatedFilter: String => Boolean = _ => false // All datasets should be considered for updates
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset3" -> List(Rename(Map("x" -> "a")))
        )
      }
      SchemaUtils.runUpdateSchemaAndVacuum(context, isUpdatedFilter, mappings, vacuum = true, dryrun = true)

      // Verify that no changes were applied
      val dataset3 = context.config.getDataset("dataset3")
      val df3 = dataset3.read(updatedConf, spark)
      df3.collect() should contain theSameElementsAs Array(Row("x1", "y1", "z1"))
      df3.columns shouldBe Seq("x", "y", "z")
    }
  }

  "runSchemaUpdateAndVacuum" should "ignore non-delta or empty datasets" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Create and write test data for dataset1 (JSON) and dataset3 (DELTA)
      write(updatedConf, "dataset1", Seq(("a1", "b1", "c1")).toDF("a", "b", "c"))
      write(updatedConf, "dataset3", Seq(("x1", "y1", "z1")).toDF("x", "y", "z"))

      // Call runUpdateSchemaAndVacuum
      val context = TestETLContext(RunStep.default_load)(updatedConf, spark)
      val isUpdatedFilter: String => Boolean = _ => false
      val mappings = new DatasetTransformationMapping {
        override val mapping: Map[String, List[Transformation]] = Map(
          "dataset1" -> List(Rename(Map("a" -> "z"))), // This dataset is not a Delta table
          "dataset3" -> List(Rename(Map("x" -> "a"))),
          "dataset4" -> List(UpperCase("a")) // This dataset is empty
        )
      }
      SchemaUtils.runUpdateSchemaAndVacuum(context, isUpdatedFilter, mappings, vacuum = true)

      // Verify that dataset1 remains unchanged
      val dataset1 = context.config.getDataset("dataset1")
      val df1 = dataset1.read(updatedConf, spark)
      df1.collect() should contain theSameElementsAs Array(Row("a1", "b1", "c1"))
      df1.columns shouldBe Seq("a", "b", "c")

      // Verify that dataset3 had been updated
      val dataset3 = context.config.getDataset("dataset3")
      val df3 = dataset3.read(updatedConf, spark)
      df3.collect() should contain theSameElementsAs Array(Row("x1", "y1", "z1"))
      df3.columns shouldBe Seq("a", "y", "z")
    }
  }

  private def write(conf: Configuration, datasetId: String, df: DataFrame): Unit = {
    val dataset = conf.getDataset(datasetId)
    LoadResolver.write(spark, conf)(dataset.format, LoadType.OverWrite).apply(dataset, df)
  }
}
