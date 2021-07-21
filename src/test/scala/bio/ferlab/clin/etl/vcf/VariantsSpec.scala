package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{VCFInput, VariantRawOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val localConf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  import spark.implicits._

  val data = Map(
    "complete_joint_calling" -> Seq(VCFInput()).toDF()
  )

  "variants job" should "transform data in expected format" in {

    val result = new Variants("BAT1").transform(data).as[VariantRawOutput].collect().head
    result shouldBe VariantRawOutput(`createdOn` = result.`createdOn`, `updatedOn` = result.`updatedOn`)
  }
}
