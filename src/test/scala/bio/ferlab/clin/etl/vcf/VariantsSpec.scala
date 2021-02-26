package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{VCFInput, VariantRawOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "variants job" should "transform data in expected format" in {

    val df = Seq(VCFInput()).toDF()

    val result = Variants.build(df, "BAT1").as[VariantRawOutput].collect().head
    result shouldBe VariantRawOutput(`createdOn` = result.`createdOn`, `updatedOn` = result.`updatedOn`)
  }
}
