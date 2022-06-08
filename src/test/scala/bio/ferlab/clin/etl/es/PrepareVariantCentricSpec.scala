package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

class PrepareVariantCentricSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val bat0: Timestamp = Timestamp.valueOf("2021-01-26 14:50:08.108")
  val bat1: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108")
  val bat2: Timestamp = Timestamp.valueOf("2021-03-26 14:50:08.108")

  val data = Map(
    enriched_variants.id -> Seq(VariantEnrichedOutput("1"), VariantEnrichedOutput(
      "2",
      `created_on` = bat0,
      //`updated_on` = bat2
    )
    ).toDF,
    enriched_consequences.id -> Seq(EnrichedConsequences(), EnrichedConsequences("2")).toDF
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File(enriched_consequences.location))
    FileUtils.deleteDirectory(new File(enriched_variants.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")
    spark.sql("USE clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "run" should "produce parquet files in the right format" in {

    val result = new PrepareVariantCentric("re_000").transform(data)
    result.count() shouldBe 2
    result.as[VariantIndexOutput].collect() should contain allElementsOf Seq(VariantIndexOutput("1"))

    new PrepareVariantCentric("re_000").load(result)
    result.write.mode("overwrite").json(this.getClass.getClassLoader.getResource(".").getFile + "/es_index/variant_centric")
  }

}
