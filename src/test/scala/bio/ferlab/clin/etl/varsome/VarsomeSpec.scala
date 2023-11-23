package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.model.normalized.{NormalizedPanels, NormalizedVariants}
import bio.ferlab.clin.model.{VarsomeExtractOutput, VarsomeOutput}
import bio.ferlab.clin.testutils.HttpServerUtils.{resourceHandler, withHttpServer}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

class VarsomeSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  private val current = LocalDateTime.now()
  private val ts = Timestamp.valueOf(current)
  private val sevenDaysAgo = Timestamp.valueOf(current.minusDays(8))
  private val yesterday = Timestamp.valueOf(current.minusDays(1))
  val normalized_variantsDF: DataFrame = Seq(
    NormalizedVariants(start = 1000, `reference` = "A", `alternate` = "T"),
    NormalizedVariants(start = 1001, `reference` = "A", `alternate` = "T", `batch_id` = "BAT2"),
    NormalizedVariants(start = 1002, `reference` = "A", `alternate` = "T"),
    NormalizedVariants(start = 1003, `reference` = "A", `alternate` = "T", `genes_symbol` = List("OR4F4")), // bad panel
    NormalizedVariants(start = 1004, `reference` = "A", `alternate` = "T")
  ).toDF()
  val normalized_varsomeDF: DataFrame = Seq(
    VarsomeOutput("1", 1000, "A", "T", "1234", sevenDaysAgo, None, None),
    VarsomeOutput("1", 1002, "A", "T", "5678", yesterday, None, None)
  ).toDF()
  val normalized_panelsDF: DataFrame = Seq(
    NormalizedPanels(), // available panel
  ).toDF()
  val normalized_varsome: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  private val defaultData = Map(
    normalized_variants.id -> normalized_variantsDF,
    normalized_varsome.id -> normalized_varsomeDF,
    normalized_panels.id -> normalized_panelsDF
  )

  def withVarsomeServer[T](resource: String = "varsome_minimal.json")(block: String => T): T = withHttpServer("/lookup/batch/hg38", resourceHandler(resource, "application/json"))(block)

  def withData[T](data: Map[String, DataFrame] = defaultData)(block: Map[String, DataFrame] => T): T = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(normalized_varsome.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_normalized")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)
      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
    block(data)
  }

  "extract" should "return a dataframe that contains only variants not in varsome table or older than 7 days in varsome table and in panels" in {
    withData() { _ =>
      val dataframes = Varsome(DeprecatedTestETLContext(), ForBatch("BAT1"), "url", "").extract(currentRunDateTime = current)
      dataframes(normalized_variants.id).as[VarsomeExtractOutput].collect() should contain theSameElementsAs Seq(
        VarsomeExtractOutput("1", 1000, "A", "T"),
        VarsomeExtractOutput("1", 1004, "A", "T")
      )
    }

  }

  "transform" should "return a dataframe representing Varsome response" in {
    withData() { data =>
      withVarsomeServer() { url =>
        val df = Varsome(DeprecatedTestETLContext(), ForBatch("BAT1"), url, "").transformSingle(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts, None, None),
          VarsomeOutput("1", 1004, "A", "T", "1234", ts, None, None)
        )
      }

    }
  }

  it should "return a dataframe representing complete Varsome response" in {
    withData() { data =>

      withVarsomeServer("varsome_full.json") { url =>
        val df = Varsome(DeprecatedTestETLContext(), ForBatch("BAT1"), url, "").transformSingle(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(VarsomeOutput(`updated_on` = ts))
      }

    }
  }

  "run" should "create varsome table" in {
    withData() { _ =>

      withVarsomeServer() { url =>
        Varsome(DeprecatedTestETLContext(runSteps = RunStep.default_load), ForBatch("BAT1"), url, "").run(currentRunDateTime = Some(current))
        val df = spark.table(normalized_varsome.table.get.fullName)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts, None, None), // updated because more than 7 days
          VarsomeOutput("1", 1002, "A", "T", "5678", yesterday, None, None),  // already here DELTA - UPSERT
          VarsomeOutput("1", 1004, "A", "T", "1234", ts, None, None), // new
        )
      }

    }
  }

}