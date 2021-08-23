package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.spark3.loader.{LoadResolver, LoadType}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

class VariantIndexSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val enriched_variants = conf.getDataset("enriched_variants")
  val enriched_consequences = conf.getDataset("enriched_consequences")
  val bat0 = Timestamp.valueOf("2021-01-26 14:50:08.108")
  val bat1 = Timestamp.valueOf("2021-02-26 14:50:08.108")
  val bat2 = Timestamp.valueOf("2021-03-26 14:50:08.108")

  val data = Map(
    enriched_variants.id -> Seq(VariantEnrichedOutput("1"), VariantEnrichedOutput(
      "2",
      `batch_id` = "BAT2",
      //`created_on` = "BAT0",
      //`updated_on` = "BAT2"
      `created_on` = bat0,
      `updated_on` = bat2
      )
    ).toDF,
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

    val result = VariantIndex.getInsert(bat1)
    result.count() shouldBe 1
    result.as[VariantIndexOutput].collect().head shouldBe VariantIndexOutput("1")

  }

  "run update" should "produce json files in the right format" in {

    val result = VariantIndex.getUpdate(bat1)
    result.count() shouldBe 1
    result.as[VariantIndexUpdate].collect().head shouldBe VariantIndexUpdate("2")
  }

}
