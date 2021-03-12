package bio.ferlab.clin.etl.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateGenesTable extends App {

  val Array(output) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Create Genes Tables").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("use clin")

  run(output)

  def run(output: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val humanGenes = spark.table("human_genes")
      .select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype")

    val orphanet = spark.table("orphanet_gene_set")
      .select($"gene_symbol" as "symbol", $"disorder_id", $"name" as "panel", $"type_of_inheritance" as "inheritance")

    val omim =  spark.table("omim_gene_set")
      .select($"omim_gene_id",
        $"phenotype.name" as "name",
        $"phenotype.omim_id" as "omim_id",
        $"phenotype.inheritance" as "inheritance")

    val hpo = spark.table("hpo_gene_set")
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val ddd_gene_set = spark.table("ddd_gene_set")
      .select("disease_name", "symbol")

    val cosmic_gene_set = spark.table("cosmic_gene_set")
      .select("symbol", "tumour_types_germline")

    humanGenes
      .joinAndMergeWith(orphanet, Seq("symbol"), "orphanet")
      .joinAndMergeWith(hpo, Seq("entrez_gene_id"), "hpo")
      .joinAndMergeWith(omim, Seq("omim_gene_id"), "omim")
      .joinAndMergeWith(ddd_gene_set, Seq("symbol"), "ddd")
      .joinAndMergeWith(cosmic_gene_set, Seq("symbol"), "cosmic")

  }

  implicit class DataFrameOps(df: DataFrame) {
    def joinAndMergeWith(gene_set: DataFrame, joinOn: Seq[String], asColumnName: String) = {
      df
        .join(gene_set, joinOn, "left")
        .groupBy("symbol")
        .agg(
          first(struct(df("*"))) as "hg",
          when(first(col(gene_set.columns.head)).isNotNull, collect_list(struct(gene_set.drop(joinOn:_*)("*")))).otherwise(lit(null)) as asColumnName
        )
        .select(col("hg.*"), col(asColumnName))
    }
  }

}
