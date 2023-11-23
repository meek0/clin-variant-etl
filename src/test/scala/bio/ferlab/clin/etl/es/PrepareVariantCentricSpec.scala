package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.{EnrichedConsequences, EnrichedVariant}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, DeprecatedTestETLContext}
import org.scalatest.BeforeAndAfterAll

import java.sql.Timestamp

class PrepareVariantCentricSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll with BeforeAndAfterAll {

  import spark.implicits._

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val bat0: Timestamp = Timestamp.valueOf("2021-01-26 14:50:08.108")
  val bat1: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108")
  val bat2: Timestamp = Timestamp.valueOf("2021-03-26 14:50:08.108")

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(enriched_consequences, enriched_variants)

  val data = Map(
    enriched_variants.id -> Seq(EnrichedVariant("1"), EnrichedVariant(
      "2",
      `created_on` = bat0,
      //`updated_on` = bat2
    )
    ).toDF,
    enriched_consequences.id -> Seq(EnrichedConsequences(), EnrichedConsequences("2")).toDF
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("USE clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "run" should "produce parquet files in the right format" in {
    val job = PrepareVariantCentric(DeprecatedTestETLContext(), "re_000")

    val result = job.transformSingle(data)
    result.count() shouldBe 2
    result.as[VariantIndexOutput].collect() should contain allElementsOf Seq(VariantIndexOutput("1"))

    job.loadSingle(result)
    result.write.mode("overwrite").json(this.getClass.getClassLoader.getResource(".").getFile + "/es_index/variant_centric")
  }

}
