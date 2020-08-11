package bio.ferlab.clin.etl

import bio.ferlab.clin.testutils.WithSparkSession
import columns._
import org.apache.spark.sql.functions.struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLFunctionsSpecs extends AnyFlatSpec with WithSparkSession with Matchers {

  "canonical" should "return expected values" in {
    import spark.implicits._
    val df = Seq("YES", "NO", "Other", null).toDF("CANONICAL").select(struct("CANONICAL") as "annotation")
    df.select(canonical).as[Boolean].collect() should contain theSameElementsAs Seq(true, false, false, false)
  }

}
