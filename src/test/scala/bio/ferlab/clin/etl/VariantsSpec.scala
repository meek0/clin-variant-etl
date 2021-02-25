package bio.ferlab.clin.etl

import bio.ferlab.clin.model.{VCFInput, VariantOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "variants job" should "transform data in expected format" in {

    val df = Seq(VCFInput()).toDF()

    Variants.build(df, "BAT1").as[VariantOutput].collect() should contain allElementsOf Seq(
      VariantOutput()
    )
  }
}
