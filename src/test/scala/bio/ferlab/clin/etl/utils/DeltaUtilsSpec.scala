package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.clin.model.VariantRawOutput
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class DeltaUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "insert" should "Add data and not replace" in {

    val tableName = UUID.randomUUID().toString.replace("-", "")
    val database = "default"

    val df = Seq(VariantRawOutput()).toDF()

    withOutputFolder("spark-warehouse"){ output =>
      DeltaUtils.insert(df, Some(output), database, tableName, { df => df}, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      DeltaUtils.insert(df, Some(output), database, tableName, { df => df}, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 2
    }

  }

  "upsert" should "Add data and not replace" in {

    val tableName = UUID.randomUUID().toString.replace("-", "")
    val database = "default"

    val variant1 = VariantRawOutput()
    val variant2 = VariantRawOutput() //different createdOn and updatedOn than variant1 but same locus

    val initialLoad = Seq(variant1).toDF()
    val update = Seq(variant2).toDF()

    withOutputFolder("spark-warehouse"){ output =>
      DeltaUtils.upsert(initialLoad, Some(output), database, tableName, { df => df}, locusColumnNames, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      DeltaUtils.upsert(update, Some(output), database, tableName, { df => df}, locusColumnNames, Seq("chromosome"))

      spark.table(tableName).count() shouldBe 1

      val result = spark.table(tableName).as[VariantRawOutput].collect().head
      result shouldBe variant1.copy(`updatedOn` = variant2.`updatedOn`)
    }

  }

}
