package bio.ferlab.clin.etl

import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{collect_list, concat, concat_ws, explode, first, lit, regexp_replace, struct, when}

object CreateGenesTable extends App {

  val Array(output) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Create Genes Tables").getOrCreate()
  spark.sql("use clin")

  import spark.implicits._

  val humanGenes = spark.table("human_genes").select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id", $"external_references.hgnc" as "hgnc", $"ensembl_gene_id",
    $"map_location" as "location", $"description" as "name", $"synonyms" as "alias", regexp_replace($"type_of_gene", "-", "_") as "biotype")

  val orphanet = spark.table("orphanet_gene_set").select($"gene_symbol", $"disorder_id", $"name" as "panel")

  val withOrphanet = humanGenes
    .join(orphanet, humanGenes("symbol") === orphanet("gene_symbol"), "left")
    .groupBy(humanGenes("symbol"))
    .agg(
      first(struct(humanGenes("*"))) as "hg",
      when(first(orphanet("gene_symbol")).isNotNull, collect_list(struct($"disorder_id", $"panel"))).otherwise(lit(null)) as "orphanet",
    )
    .select($"hg.*", $"orphanet")

  val hpo = spark.table("hpo_gene_set").select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
    .distinct()
    .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("),$"hpo_term_id", lit(")") ))
  val withHpo = withOrphanet
    .join(hpo, withOrphanet("entrez_gene_id") === hpo("entrez_gene_id"), "left")
    .groupBy(withOrphanet("symbol"))
    .agg(
      first(struct(withOrphanet("*"))) as "hg",
      when(first(hpo("entrez_gene_id")).isNotNull, collect_list(struct($"hpo_term_id", $"hpo_term_name", $"hpo_term_label"))).otherwise(lit(null)) as "hpo"
    )
    .select($"hg.*", $"hpo")

  val omim = spark.table("omim_gene_set").select($"omim_gene_id", $"phenotype")

  val genes = withHpo
    .join(omim, withHpo("omim_gene_id") === omim("omim_gene_id"), "left")
    .groupBy(withHpo("symbol"))
    .agg(
      first(struct(withHpo("*"))) as "hg",
      when(first(omim("omim_gene_id")).isNotNull, collect_list($"phenotype")).otherwise(lit(null)) as "omim"
    )
    .select($"hg.*", $"omim")

  genes.repartition(1)
    .write
    .mode("overwrite")
    .partitionBy("chromosome")
    .format("parquet")
    .option("path", s"$output/genes")
    .saveAsTable("genes")


}

