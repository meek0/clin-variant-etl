package bio.ferlab.clin.etl.external

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateGenesTableSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "create genes table" should "return data in the right format" in {

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS clin")
    spark.sql("USE clin")

    Seq(HumanGenesOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/clin.db/human_genes")
      .saveAsTable("clin.human_genes")

    Seq(OrphanetGeneSetOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/clin.db/orphanet_gene_set")
      .saveAsTable("clin.orphanet_gene_set")

    Seq(HpoGeneSetOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/clin.db/hpo_gene_set")
      .saveAsTable("clin.hpo_gene_set")

    Seq(OmimGeneSetOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", "spark-warehouse/clin.db/omim_gene_set")
      .saveAsTable("clin.omim_gene_set")

    val result = CreateGenesTable.run("spark-warehouse/output")

    result.as[GenesOutput].collect().head shouldBe GenesOutput()

  }

}
