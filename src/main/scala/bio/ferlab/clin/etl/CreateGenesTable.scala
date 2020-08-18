package bio.ferlab.clin.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, first, lit, regexp_replace, struct, when}

object CreateGenesTable extends App {

  val Array(output) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Create Genes Tables").getOrCreate()
  spark.sql("use clin")

  import spark.implicits._

  val humanGenes = spark.table("human_genes").select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id", $"external_references.hgnc" as "hgnc", $"ensembl_gene_id",
    $"map_location" as "location", $"description" as "name", $"synonyms" as "alias", regexp_replace($"type_of_gene", "-", "_") as "biotype")

  val orphanet = spark.table("orphanet_gene_set").select($"ensembl_gene_id", $"disorder_id", $"name" as "panel")

  val hpo = spark.table("hpo_gene_set").select($"ensembl_gene_id", $"hpo_term_id", $"hpo_term_name").distinct()

  val withOrphanet = humanGenes
    .join(orphanet, humanGenes("ensembl_gene_id") === orphanet("ensembl_gene_id"), "left")
    .groupBy(humanGenes("ensembl_gene_id"))
    .agg(
      first(struct(humanGenes("*"))) as "hg",
      when(first(orphanet("ensembl_gene_id")).isNotNull, collect_list(struct($"disorder_id", $"panel"))).otherwise(lit(null)) as "orphanet",
    )
    .select($"hg.*", $"orphanet")

  val genes = withOrphanet
    .join(hpo, withOrphanet("ensembl_gene_id") === hpo("ensembl_gene_id"), "left")
    .groupBy(withOrphanet("ensembl_gene_id"))
    .agg(
      first(struct(withOrphanet("*"))) as "hg",
      when(first(hpo("ensembl_gene_id")).isNotNull, collect_list(struct($"hpo_term_id", $"hpo_term_name"))).otherwise(lit(null)) as "hpo"
    )
    .select($"hg.*", $"hpo")

  genes.repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", s"$output/genes")
    .saveAsTable("genes")


}

