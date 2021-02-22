package bio.ferlab.clin.etl

import org.apache.spark.sql.SparkSession

object PrepareGeneIndex extends App {

  val Array(output) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Prepare Gene Index").getOrCreate()

  run(output)

  def run(output: String)(implicit spark: SparkSession): Unit = {
    spark.sql("use clin")
    spark.table("genes")
      .select("chromosome", "symbol", "entrez_gene_id", "omim_gene_id", "hgnc",
        "ensembl_gene_id", "location", "name", "alias", "biotype")
      .write.mode("overwrite")
      .json(s"$output")
  }

}

