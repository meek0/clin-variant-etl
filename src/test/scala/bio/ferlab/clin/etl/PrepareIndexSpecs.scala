package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns._
import bio.ferlab.clin.testutils.WithSparkHiveSession
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrepareIndexSpecs extends AnyFlatSpec with WithSparkHiveSession with Matchers {

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0)).toDF("calls")
    occurrences
      .select(
        ac,
        columns.an
      ).collect() should contain theSameElementsAs Seq(Row(3, 6))

  }

}
