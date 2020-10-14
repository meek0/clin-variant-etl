package bio.ferlab.clin.etl

import org.apache.spark.sql.SparkSession

object CreatePublicTables extends App {

  val Array(input) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Create Public Tables").getOrCreate()
  spark.sql("use clin")

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `clinvar` (
      |   `chromosome` string,
      |   `start` bigint,
      |   `end` bigint,
      |   `name` string,
      |   `reference` string,
      |   `alternate` string,
      |   `clin_sig_original` array<string>,
      |   `clin_sig_conflict` array<string>,
      |   `clin_sig` array<string>
      |)
      |USING parquet
      |LOCATION '$input/clinvar'
      |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `dbsnp` (
      |   `chromosome` string,
      |   `start` bigint,
      |   `reference` string,
      |   `alternate` string,
      |   `end` bigint,
      |   `name` string,
      |   `original_contig_name` string
      |)
      |USING parquet
      |LOCATION '$input/dbsnp'
      |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `topmed_bravo` (
       |   `chromosome` string,
       |   `start` bigint,
       |   `end` bigint,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/topmed_bravo'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `gnomad_genomes_3_0` (
       |   `chromosome` string,
       |   `start` int,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad/gnomad_genomes_3.0'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `gnomad_exomes_2_1_1_liftover_grch38` (
       |   `chromosome` string,
       |   `start` int,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad/gnomad_exomes_2.1.1_liftover_grch38'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `gnomad_genomes_2_1_1_liftover_grch38` (
       |   `chromosome` string,
       |   `start` int,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad/gnomad_genomes_2.1.1_liftover_grch38'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `1000_genomes` (
       |   `chromosome` string,
       |   `start` bigint,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/1000_genomes'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `dbnsfp_scores` (
       |`start` int,
       |`reference` string,
       |`alternate` string,
       |`aaref` string,
       |`symbol` string,
       |`ensembl_gene_id` string,
       |`ensembl_protein_id` string,
       |`ensembl_transcript_id` string,
       |`cds_strand` string,
       |`sift_score` double,
       |`sift_pred` string,
       |`sift_converted_rank_score` double,
       |`polyphen2_hdiv_score` double,
       |`polyphen2_hdiv_pred` string,
       |`polyphen2_hdiv_rank_score` double,
       |`polyphen2_hvar_score` double,
       |`polyphen2_hvar_pred` string,
       |`polyphen2_hvar_rank_score` double,
       |`fathmm_score` double,
       |`fathmm_pred` string,
       |`fathmm_converted_rank_score` double,
       |`cadd_score` double,
       |`cadd_rankscore` double,
       |`cadd_phred` double,
       |`dann_score` double,
       |`dann_rank_score` double,
       |`revel_rankscore` double,
       |`lrt_converted_rankscore` double,
       |`lrt_pred` string,
       |`phylo_p100way_vertebrate` double,
       |`phylo_p100way_vertebrate_rankscore` double,
       |`phylop30way_mammalian` double,
       |`phylo_p30way_mammalian_rankscore` double,
       |`phylop17way_primate` double,
       |`phylo_p17way_primate_rankscore` double,
       |`phast_cons100way_vertebrate` double,
       |`phast_cons100way_vertebrate_rankscore` double,
       |`phastcons30way_mammalian` double,
       |`phast_cons30way_mammalian_rankscore` double,
       |`phast_cons17way_primate` double,
       |`phast_cons17way_primate_rankscore` double,
       |`gerp_nr` double,
       |`gerp_rs` double,
       |`gerp_rs_rankscore` double,
       |`chromosome` string
       |)
       |USING parquet
       |LOCATION '$input/dbnsfp/scores'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `human_genes` (
       |  `tax_id` int,
       |  `entrez_gene_id` int,
       |  `symbol` string,
       |  `locus_tag` string,
       |  `synonyms` array<string>,
       |  `external_references` map<string,string>,
       |  `chromosome` string,
       |  `map_location` string,
       |  `description` string,
       |  `type_of_gene` string,
       |  `symbol_from_nomenclature_authority` string,
       |  `full_name_from_nomenclature_authority` string,
       |  `nomenclature_status` string,
       |  `other_designations` array<string>,
       |  `feature_types` map<string,string>,
       |  `ensembl_gene_id` string,
       |  `omim_gene_id` string
       |)
       |USING parquet
       |LOCATION '$input/human_genes'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `orphanet_gene_set` (
       |  `disorder_id` int,
       |  `orpha_number` int,
       |  `expert_link` string,
       |  `name` string,
       |  `disorder_type` string,
       |  `disorder_group` string,
       |  `gene_symbol` string,
       |  `ensembl_gene_id` string,
       |  `association_type` string,
       |  `association_type_id` int,
       |  `status` string
       |)
       |USING parquet
       |LOCATION '$input/orphanet_gene_set'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `hpo_gene_set` (
       |  `entrez_gene_id` int,
       |  `symbol` string,
       |  `hpo_term_id` string,
       |  `hpo_term_name` string,
       |  `frequency_raw` string,
       |  `frequency_hpo` string,
       |  `source_info` string,
       |  `source` string,
       |  `source_id` string,
       |  `ensembl_gene_id` string
       |)
       |USING parquet
       |LOCATION '$input/hpo_gene_set'
       |""".stripMargin)
  spark.sql(
    s"""CREATE TABLE IF NOT EXISTS `omim_gene_set` (
       |  `chromosome` STRING,
       |  `start` INT,
       |  `end` INT,
       |  `cypto_location` STRING,
       |  `computed_cypto_location` STRING,
       |  `omim_gene_id` INT,
       |  `symbols` ARRAY<STRING>,
       |  `name` STRING,
       |  `approved_symbol` STRING,
       |  `entrez_gene_id` INT,
       |  `ensembl_gene_id` STRING,
       |  `comments` STRING,
       |  `phenotype` STRUCT<`name`: STRING, `omim_id`: STRING, `inheritance`: ARRAY<STRING>>
       |)
       |USING parquet
       |LOCATION '$input/omim_gene_set'
       |""".stripMargin)

}

