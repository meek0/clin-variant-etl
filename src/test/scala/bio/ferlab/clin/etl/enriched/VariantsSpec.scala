package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.VcfUtils.columns
import bio.ferlab.clin.etl.utils.VcfUtils.columns.ac
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val localConf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  val normalized_occurrences = Seq(OccurrenceRawOutput(), OccurrenceRawOutput(`organization_id` = "OR00202")).toDF

  val normalized_variants = Seq(VariantRawOutput()).toDF()

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_raw")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    normalized_variants
      .write.format("delta").mode(SaveMode.Overwrite)
      .saveAsTable("clin_raw.variants")

    normalized_occurrences
      .write.format("delta").mode(SaveMode.Overwrite)
      .saveAsTable("clin_raw.occurrences")

    Seq(GnomadExomes21Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.gnomad_exomes_2_1_1_liftover_grch38")

    Seq(GnomadGenomes21Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.gnomad_genomes_2_1_1_liftover_grch38")

    Seq(GnomadGenomes30Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.gnomad_genomes_3_0")

    Seq(OneKGenomesOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.1000_genomes")

    Seq(Topmed_bravoOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.topmed_bravo")

    Seq(ClinvarOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.clinvar")

    Seq(DbsnpOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.dbsnp")

    Seq(GenesOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .saveAsTable("clin.genes")
  }

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0)).toDF("calls")
    occurrences
      .select(
        ac,
        columns.an
      ).collect() should contain theSameElementsAs Seq(Row(3, 6))

  }

  "variants job" should "transform data in expected format" in {

    val data = Map(
      "normalized_variants" -> normalized_variants,
      "normalized_occurrences" -> normalized_occurrences
    )

    val result = new Variants("BAT0").transform(data)
      .as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = List(DONORS(), DONORS(`organization_id` = "OR00202")),
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }

  "variants job" should "run" in {

    new Variants("BAT0").run()

    val result = spark.table("clin.variants").as[VariantEnrichedOutput].collect().head

    result shouldBe VariantEnrichedOutput(
      `donors` = List(DONORS(), DONORS(`organization_id` = "OR00202")),
      `createdOn` = result.`createdOn`,
      `updatedOn` = result.`updatedOn`)
  }
}

