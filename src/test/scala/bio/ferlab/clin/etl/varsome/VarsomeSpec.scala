package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.etl.vcf.ForBatch
import bio.ferlab.clin.model.{VariantRawOutput, VarsomeExtractOutput, VarsomeOutput}
import bio.ferlab.clin.testutils.HttpServerUtils.{resourceHandler, withHttpServer}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

class VarsomeSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {
  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  import spark.implicits._

  private val current = LocalDateTime.now()
  private val ts = Timestamp.valueOf(current)
  private val sevenDaysAgo = Timestamp.valueOf(current.minusDays(8))
  private val yesterday = Timestamp.valueOf(current.minusDays(1))
  val normalized_variantsDF: DataFrame = Seq(
    VariantRawOutput(start = 1000, `reference` = "A", `alternate` = "T"),
    VariantRawOutput(start = 1001, `reference` = "A", `alternate` = "T", `batch_id` = "BAT2"),
    VariantRawOutput(start = 1002, `reference` = "A", `alternate` = "T")
  ).toDF()
  val normalized_varsomeDF: DataFrame = Seq(
    VarsomeOutput("1", 1000, "A", "T", "1234", sevenDaysAgo, None, None),
    VarsomeOutput("1", 1002, "A", "T", "5678", yesterday, None, None)
  ).toDF()
  val normalized_varsome: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val defaultData = Map(
    normalized_variants.id -> normalized_variantsDF,
    normalized_varsome.id -> normalized_varsomeDF
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

  "extract" should "return a dataframe that contains only variants not in varsome table or older than 7 days in varsome table" in {
    withData() { _ =>
      val dataframes = new Varsome(ForBatch("BAT1"), "url", "").extract(currentRunDateTime = current)
      dataframes(normalized_variants.id).as[VarsomeExtractOutput].collect() should contain theSameElementsAs Seq(
        VarsomeExtractOutput("1", 1000, "A", "T")
      )
    }

  }

  "transform" should "return a dataframe representing Varsome response" in {
    withData() { data =>
      withVarsomeServer() { url =>
        val df = new Varsome(ForBatch("BAT1"), url, "").transform(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts, None, None)
        )
      }

    }
  }

  it should "return a dataframe representing complete Varsome response" in {
    withData() { data =>

      withVarsomeServer("varsome_full.json") { url =>
        val df = new Varsome(ForBatch("BAT1"), url, "").transform(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(VarsomeOutput(`updated_on` = ts))
      }

    }
  }

  "run" should "create varsome table" in {
    withData() { _ =>

      withVarsomeServer() { url =>
        new Varsome(ForBatch("BAT1"), url, "").run(currentRunDateTime = Some(current))
        val df = spark.table(normalized_varsome.table.get.fullName)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts, None, None),
          VarsomeOutput("1", 1002, "A", "T", "5678", yesterday, None, None)
        )
      }

    }
  }

}