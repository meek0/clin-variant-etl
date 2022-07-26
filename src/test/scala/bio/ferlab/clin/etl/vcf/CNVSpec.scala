package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CNVSpec  extends AnyFlatSpec with WithSparkSession with Matchers{
  import spark.implicits._

  val occurrences: DataFrame = Seq(
    "1",
    "10",
    "X",
    "Y",
    "M"
  ).toDF("chromosome")

  "sortChromosome" should "return chromosome as integers" in {
    val frame: DataFrame = occurrences.select(CNV.sortChromosome)
    val res: Array[Int] = frame.as[Int].collect()
    res should contain theSameElementsAs Seq(1, 10, 100, 101, 102)
  }
}
