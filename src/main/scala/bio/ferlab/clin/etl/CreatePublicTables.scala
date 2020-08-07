package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.ByLocus._
import bio.ferlab.clin.etl.columns.{ac, an}
import org.apache.spark.sql.functions.{col, collect_list, first, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreatePublicTables extends App {

  val Array(input) = args

  implicit val spark: SparkSession = SparkSession.builder
    .enableHiveSupport()
    .appName(s"Create Public Tables").getOrCreate()
  spark.sql("use clin")

  spark.sql(
    s"""CREATE TABLE `clinvar` (
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
    s"""CREATE TABLE `dbsnp` (
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
    s"""CREATE TABLE `topmed_bravo` (
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
    s"""CREATE TABLE `gnomad_genomes_3_0` (
       |   `chromosome` string,
       |   `start` bigint,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad_genomes_3_0'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE `gnomad_exomes_2_1_1_liftover_grch38` (
       |   `chromosome` string,
       |   `start` int,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad_exomes_2_1_1_liftover_grch38'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE `gnomad_genomes_2_1_1_liftover_grch38` (
       |   `chromosome` string,
       |   `start` int,
       |   `name` string,
       |   `reference` string,
       |   `alternate` string,
       |   `ac` int,
       |   `af` double
       |)
       |USING parquet
       |LOCATION '$input/gnomad_genomes_2_1_1_liftover_grch38'
       |""".stripMargin)

  spark.sql(
    s"""CREATE TABLE `1000_genomes` (
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
    s"""CREATE TABLE `dbnsfp_scores` (
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
       |LOCATION '$input/dbnsfp_scores'
       |""".stripMargin)


}

