package bio.ferlab.clin.etl.external

import bio.ferlab.clin.etl.external.PublicTablesCreateStatements._
import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import bio.ferlab.datalake.spark3.public.SparkApp

object CreatePublicTables extends SparkApp {

  val Array(_, _) = args

  implicit val (conf, _, spark) = init()

  spark.sparkContext.setLogLevel("ERROR")

  val createStatementMap = Map(
    "cancer_hotspots" -> cancer_hotspots,
    "clinvar" -> clinvar,
    "cosmic_gene_set" -> cosmic_gene_set,
    "dbsnp" -> dbsnp,
    "ddd_gene_set" -> ddd_gene_set,
    "topmed_bravo" -> topmed_bravo,
    "gnomad_genomes_3_0" -> gnomad_genomes_3_0,
    "gnomad_genomes_3_1_1" -> gnomad_genomes_3_1_1,
    "gnomad_exomes_2_1_1" -> gnomad_exomes_2_1_1,
    "gnomad_genomes_2_1_1" -> gnomad_genomes_2_1_1,
    "1000_genomes" -> `1000_genomes`,
    "dbnsfp_annovar" -> dbnsfp_annovar,
    "dbnsfp_original" -> dbnsfp_original,
    "dbnsfp_scores" -> dbnsfp_scores,
    "human_genes" -> human_genes,
    "ensembl_mapping" -> ensembl_mapping,
    "orphanet_gene_set" -> orphanet_gene_set,
    "hpo_gene_set" -> hpo_gene_set,
    "omim_gene_set" -> omim_gene_set,
    "genes" -> genes
  )

  conf
    .sources
    .collect { case ds: DatasetConf if ds.table.isDefined && ds.path.contains("/public") => ds}
    .foreach { case ds @ DatasetConf(_, _, _, _, _, Some(TableConf(database, name)), _, _, _, _, _, _) =>
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
      spark.sql(s"DROP TABLE IF EXISTS $database.$name")
      spark.sql(s"USE $database")
      createStatementMap
        .get(name)
        .foreach(statement => spark.sql(statement(ds.location)))
    }



}
