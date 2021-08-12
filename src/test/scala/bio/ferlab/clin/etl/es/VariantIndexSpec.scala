package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.spark3.loader.{LoadResolver, LoadType}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class VariantIndexSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val enriched_variants = conf.getDataset("enriched_variants")
  val enriched_consequences = conf.getDataset("enriched_consequences")

  val data = Map(
    enriched_variants.id -> Seq(VariantEnrichedOutput("1"), VariantEnrichedOutput(
      "2",
      `batch_id` = "BAT2",
      `createdOn` = "BAT0",
      `updatedOn` = "BAT2")).toDF,
    enriched_consequences.id -> Seq(ConsequenceEnrichedOutput(), ConsequenceEnrichedOutput("2")).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File(enriched_consequences.location))
    FileUtils.deleteDirectory(new File(enriched_variants.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")
    spark.sql("USE clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .resolve(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "run" should "produce json files in the right format" in {

    val result = VariantIndex.getInsert("BAT1")
    result.count() shouldBe 1
    result.as[VariantIndexOutput].collect().head shouldBe VariantIndexOutput("1")

  }

  "run update" should "produce json files in the right format" in {

    val result = VariantIndex.getUpdate("BAT1")
    result.count() shouldBe 1
    result.as[VariantIndexUpdate].collect().head shouldBe VariantIndexUpdate("2")
  }

}
