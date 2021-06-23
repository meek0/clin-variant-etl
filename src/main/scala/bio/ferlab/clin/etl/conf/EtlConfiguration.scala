package bio.ferlab.clin.etl.conf

import bio.ferlab.datalake.spark3.config._
import bio.ferlab.datalake.spark3.loader.Format.{PARQUET, VCF}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

object EtlConfiguration extends App {

  val alias = "public_database_storage"

  val clin_storage = List(
    StorageConf(alias, "s3a://clin")
  )

  val clin_spark_conf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true"
  )

  val clin_conf =
    Configuration(
      storages = clin_storage,
      sources = List(
        //raw
        DatasetConf("clinvar_vcf", alias, "/raw/clinvar/clinvar.vcf.gz", VCF    , OverWrite),

        //public
        DatasetConf("1000_genomes"        , alias, "/public/1000_genomes"                        , PARQUET, OverWrite, TableConf("clin", "1000_genomes")),
        DatasetConf("cancer_hotspots"     , alias, "/public/cancer_hotspots"                     , PARQUET, OverWrite, TableConf("clin", "cancer_hotspots")),
        DatasetConf("clinvar"             , alias, "/public/clinvar"                             , PARQUET, OverWrite, TableConf("clin", "clinvar")),
        DatasetConf("cosmic_gene_set"     , alias, "/public/cosmic_gene_set"                     , PARQUET, OverWrite, TableConf("clin", "cosmic_gene_set")),
        DatasetConf("dbnsfp"              , alias, "/public/dbnsfp/clin"                         , PARQUET, OverWrite, TableConf("clin", "dbnsfp")),
        DatasetConf("dbnsfp_annovar"      , alias, "/public/annovar/dbnsfp"                      , PARQUET, OverWrite, TableConf("clin", "dbnsfp_annovar")),
        DatasetConf("dbnsfp_original"     , alias, "/public/dbnsfp/scores"                       , PARQUET, OverWrite, TableConf("clin", "dbnsfp_original")),
        DatasetConf("dbsnp"               , alias, "/public/dbsnp"                               , PARQUET, OverWrite, TableConf("clin", "dbsnp")),
        DatasetConf("ddd_gene_set"        , alias, "/public/ddd_gene_set"                        , PARQUET, OverWrite, TableConf("clin", "ddd_gene_set")),
        DatasetConf("ensembl_mapping"     , alias, "/public/ensembl_mapping"                     , PARQUET, OverWrite, TableConf("clin", "ensembl_mapping")),
        DatasetConf("genes"               , alias, "/public/genes"                               , PARQUET, OverWrite, TableConf("clin", "genes")),
        DatasetConf("gnomad_genomes_2_1_1", alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_2_1_1")),
        DatasetConf("gnomad_exomes_2_1_1" , alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , PARQUET, OverWrite, TableConf("clin", "gnomad_exomes_2_1_1")),
        DatasetConf("gnomad_genomes_3_0"  , alias, "/public/gnomad_genomes_3_0"                  , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_0")),
        DatasetConf("human_genes"         , alias, "/public/human_genes"                         , PARQUET, OverWrite, TableConf("clin", "human_genes")),
        DatasetConf("hpo_gene_set"        , alias, "/public/hpo_gene_set"                        , PARQUET, OverWrite, TableConf("clin", "hpo_gene_set")),
        DatasetConf("omim_gene_set"       , alias, "/public/omim_gene_set"                       , PARQUET, OverWrite, TableConf("clin", "omim_gene_set")),
        DatasetConf("orphanet_gene_set"   , alias, "/public/orphanet_gene_set"                   , PARQUET, OverWrite, TableConf("clin", "orphanet_gene_set")),
        DatasetConf("topmed_bravo"        , alias, "/public/topmed_bravo"                        , PARQUET, OverWrite, TableConf("clin", "topmed_bravo"))
      ),
      sparkconf = clin_spark_conf
    )

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", clin_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/production.conf", clin_conf)

  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", clin_conf)

}

