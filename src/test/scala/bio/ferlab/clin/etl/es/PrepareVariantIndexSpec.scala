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

class PrepareVariantIndexSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val enriched_variants = conf.getDataset("enriched_variants")
  val enriched_consequences = conf.getDataset("enriched_consequences")

  val data = Map(
    enriched_variants.id -> Seq(VariantEnrichedOutput()).toDF,
    enriched_consequences.id -> Seq(ConsequenceEnrichedOutput()).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
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

    val result = PrepareVariantIndex.run("spark-warehouse/output", "BAT0")
    result.as[VariantIndexOutput].collect().head shouldBe VariantIndexOutput()

  }

  "run update" should "produce json files in the right format" in {

    Seq(VariantEnrichedOutput(
      `batch_id` = "BAT2",
      `createdOn` = "BAT1",//Timestamp.valueOf("2020-01-01 12:00:00"),
      `updatedOn` = "BAT2"))//Timestamp.valueOf("2020-01-01 13:00:00")))
      .toDF
      .write.format("delta").mode(SaveMode.Overwrite)
      .saveAsTable("clin.variants")

    val resultUpdate = PrepareVariantIndex.runUpdate("spark-warehouse/output", "BAT1")

    resultUpdate.as[VariantIndexUpdate].collect().head shouldBe VariantIndexUpdate()
  }

}
