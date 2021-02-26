package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

class PrepareIndexSpecs extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  FileUtils.deleteDirectory(new File("spark-warehouse"))
  spark.sql("CREATE DATABASE IF NOT EXISTS clin")
  spark.sql("USE clin")

  Seq(VariantEnrichedOutput(
    `createdOn` = Timestamp.valueOf("2020-01-01 12:00:00"),
    `updatedOn` = Timestamp.valueOf("2020-01-01 12:00:00")))
    .toDF
    .write.format("delta").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/variants")
    .saveAsTable("clin.variants")

  Seq(ConsequenceEnrichedOutput()).toDF
    .write.format("delta").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/consequences")
    .saveAsTable("clin.consequences")

  "run" should "produce json files in the right format" in {

    val result = PrepareIndex.run("spark-warehouse/output", "2019-12-31 12:00:00")
    result.as[VariantIndexOutput].collect().head shouldBe VariantIndexOutput()

  }

  "run update" should "produce json files in the right format" in {

    Seq(VariantEnrichedOutput(
      `batch_id` = "BAT2",
      `createdOn` = Timestamp.valueOf("2020-01-01 12:00:00"),
      `updatedOn` = Timestamp.valueOf("2020-01-01 13:00:00")))
      .toDF
      .write.format("delta").mode(SaveMode.Overwrite)
      .saveAsTable("clin.variants")

    val resultUpdate = PrepareIndex.runUpdate("spark-warehouse/output", "2020-01-01 12:00:00")

    resultUpdate.as[VariantIndexUpdate].collect().head shouldBe VariantIndexUpdate()
  }

}
