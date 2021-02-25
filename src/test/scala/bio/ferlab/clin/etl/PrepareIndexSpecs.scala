package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns._
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrepareIndexSpecs extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  spark.sql("CREATE DATABASE IF NOT EXISTS clin")
  spark.sql("USE clin")

  Seq(ClinvarOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/clinvar")
    .saveAsTable("clin.clinvar")

  Seq(Dbnsfp_originalOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/dbnsfp_original")
    .saveAsTable("clin.dbnsfp_original")

  Seq(DbsnpOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/dbsnp")
    .saveAsTable("clin.dbsnp")

  Seq(GenesOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/genes")
    .saveAsTable("clin.genes")

  Seq(GnomadExomes21Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/gnomad_exomes_2_1_1_liftover_grch38")
    .saveAsTable("clin.gnomad_exomes_2_1_1_liftover_grch38")

  Seq(GnomadGenomes21Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/gnomad_genomes_2_1_1_liftover_grch38")
    .saveAsTable("clin.gnomad_genomes_2_1_1_liftover_grch38")

  Seq(GnomadGenomes30Output()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/gnomad_genomes_3_0")
    .saveAsTable("clin.gnomad_genomes_3_0")

  Seq(OneKGenomesOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/1000_genomes")
    .saveAsTable("clin.1000_genomes")

  Seq(Topmed_bravoOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/topmed_bravo")
    .saveAsTable("clin.topmed_bravo")

  Seq(OccurrenceOutput(), OccurrenceOutput(`organization_id` = "OR00202")).toDF
    .write.format("delta").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/occurrences")
    .saveAsTable("clin.occurrences")

  Seq(VariantOutput(), VariantOutput(`batch_id` = "BAT2", `last_batch_id` = Some("BAT2"))).toDF
    .write.format("delta").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/variants")
    .saveAsTable("clin.variants")

  Seq(ConsequenceOutput()).toDF
    .write.format("delta").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/consequences")
    .saveAsTable("clin.consequences")

  "ac" should "return sum of allele count" in {
    import spark.implicits._
    val occurrences = Seq(Seq(0, 1), Seq(1, 1), Seq(0, 0)).toDF("calls")
    occurrences
      .select(
        ac,
        columns.an
      ).collect() should contain theSameElementsAs Seq(Row(3, 6))

  }

  "run" should "produce json files in the right format" in {

    val result = PrepareIndex.run("spark-warehouse/output", "BAT1")
    result.as[VariantIndexOutput].collect().head shouldBe VariantIndexOutput()

  }

  "run update" should "produce json files in the right format" in {

    val resultUpdate = PrepareIndex.runUpdate("spark-warehouse/output", "BAT2")
    resultUpdate.show(false)

    resultUpdate.as[VariantIndexUpdate].collect().head shouldBe VariantIndexUpdate()
  }

}
