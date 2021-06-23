package bio.ferlab.clin.etl.external

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.spark3.public.ImportGenesTable
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateGenesTableSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  val root: String = "spark-warehouse/clin.db"

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("public_database_storage", root)))

  "create genes table" should "return data in the right format" in {

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS clin")
    spark.sql("USE clin")

    Seq(HumanGenesOutput(), HumanGenesOutput(`symbol` = "OR4F4")).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/human_genes")
      .saveAsTable("clin.human_genes")

    Seq(OrphanetGeneSetOutput(gene_symbol = "OR4F5")).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/orphanet_gene_set")
      .saveAsTable("clin.orphanet_gene_set")

    Seq(HpoGeneSetOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/hpo_gene_set")
      .saveAsTable("clin.hpo_gene_set")

    Seq(
      OmimGeneSetOutput(omim_gene_id = 601013),
      OmimGeneSetOutput(omim_gene_id = 601013, phenotype = PHENOTYPE(null, null, null, null))
    ).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/omim_gene_set")
      .saveAsTable("clin.omim_gene_set")

    Seq(DddGeneSetOutput(`symbol` = "OR4F5")).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/ddd_gene_set")
      .saveAsTable("clin.ddd_gene_set")

    Seq(CosmicGeneSetOutput(`symbol` = "OR4F5")).toDF.write.format("parquet").mode(SaveMode.Overwrite)
      .option("path", s"$root/public/cosmic_gene_set")
      .saveAsTable("clin.cosmic_gene_set")

    val result = new ImportGenesTable().run().where("symbol='OR4F5'")

    val expectedOrphanet = List(
      ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive"))
    )
    val expectedOmim =
      List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))

    result.as[GenesOutput].collect().head shouldBe GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)

  }

}
