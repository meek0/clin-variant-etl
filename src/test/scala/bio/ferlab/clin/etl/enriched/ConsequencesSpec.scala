package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.{ConsequenceEnrichedOutput, ConsequenceRawOutput, Dbnsfp_originalOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  FileUtils.deleteDirectory(new File("spark-warehouse"))
  spark.sql("CREATE DATABASE IF NOT EXISTS clin")

  Seq(Dbnsfp_originalOutput()).toDF
    .write.format("parquet").mode(SaveMode.Overwrite)
    .saveAsTable("clin.dbnsfp_original")

  "consequences job" should "transform data in expected format" in {

    val df = Seq(ConsequenceRawOutput()).toDF()

    val result = Consequences.build(df).as[ConsequenceEnrichedOutput].collect().head
    result shouldBe ConsequenceEnrichedOutput(
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)

    //val df = Consequences.build(df)
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ConsequenceEnrichedOutput2", df, "src/test/scala/")
  }
}


