package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns.{locus, locusColumnNames}
import bio.ferlab.clin.model.VariantOutput
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "insert" should "Add data and not replace" in {

    val tableName = UUID.randomUUID().toString.replace("-", "")
    val database = "default"

    val df = Seq(VariantOutput()).toDF()

    withOutputFolder("spark-warehouse"){ output =>
      SparkUtils.insert(df, Some(output), database, tableName, { df => df}, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      SparkUtils.insert(df, Some(output), database, tableName, { df => df}, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 2
    }

  }

  "upsert" should "Add data and not replace" in {

    val tableName = UUID.randomUUID().toString.replace("-", "")
    val database = "default"

    val variant1 = VariantOutput()
    val variant2 = VariantOutput() //different createdOn and updatedOn than variant1 but same locus

    val initialLoad = Seq(variant1).toDF()
    val update = Seq(variant2).toDF()

    withOutputFolder("spark-warehouse"){ output =>
      SparkUtils.upsert(initialLoad, Some(output), database, tableName, { df => df}, locusColumnNames, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      SparkUtils.upsert(update, Some(output), database, tableName, { df => df}, locusColumnNames, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      val result = spark.table(tableName).as[VariantOutput].collect().head
      result shouldBe variant1.copy(`updatedOn` = variant2.`updatedOn`)
    }

  }

}
