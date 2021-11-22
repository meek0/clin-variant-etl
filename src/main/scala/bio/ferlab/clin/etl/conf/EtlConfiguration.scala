package bio.ferlab.clin.etl.conf

import bio.ferlab.datalake.commons.config.Format.{DELTA, JSON, PARQUET, VCF}
import bio.ferlab.datalake.commons.config.LoadType.{Insert, OverWrite, Scd1}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object EtlConfiguration extends App {

  val clin_datalake = "clin_datalake"
  val clin_import = "clin_import"

  val clin_qa_database = "clin_qa"
  val clin_prd_database = "clin"

  val clin_qa_storage = List(
    StorageConf(clin_import, "s3a://clin-qa-app-files-import", S3),
    StorageConf(clin_datalake, "s3a://clin-qa-app-datalake", S3)
  )

  val clin_prd_storage = List(
    StorageConf(clin_import, "s3a://clin-prd-app-files-import", S3),
    StorageConf(clin_datalake, "s3a://clin-prd-app-datalake", S3)
  )

  val clin_spark_conf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.legacy.timeParserPolicy"-> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN"
  )

  val sources =
    List(
      //s3://clin-{env}-app-files-import/201106_A00516_0169_AHFM3HDSXY/201106_A00516_0169_AHFM3HDSXY.hard-filtered.formatted.norm.VEP.vcf.gz
      DatasetConf("raw_variant_calling"            , clin_import  , "/{{BATCH_ID}}/*.hard-filtered.formatted.norm.VEP.vcf.gz", VCF    , OverWrite),
      DatasetConf("raw_clinical_impression"        , clin_datalake, "/raw/landing/fhir/ClinicalImpression"                   , JSON   , OverWrite),
      DatasetConf("raw_group"                      , clin_datalake, "/raw/landing/fhir/Group"                                , JSON   , OverWrite),
      DatasetConf("raw_observation"                , clin_datalake, "/raw/landing/fhir/Observation"                          , JSON   , OverWrite),
      DatasetConf("raw_organization"               , clin_datalake, "/raw/landing/fhir/Organization"                         , JSON   , OverWrite),
      DatasetConf("raw_patient"                    , clin_datalake, "/raw/landing/fhir/Patient"                              , JSON   , OverWrite),
      DatasetConf("raw_practitioner"               , clin_datalake, "/raw/landing/fhir/Practitioner"                         , JSON   , OverWrite),
      DatasetConf("raw_practitioner_role"          , clin_datalake, "/raw/landing/fhir/PractitionerRole"                     , JSON   , OverWrite),
      DatasetConf("raw_service_request"            , clin_datalake, "/raw/landing/fhir/ServiceRequest"                       , JSON   , OverWrite),
      DatasetConf("raw_specimen"                   , clin_datalake, "/raw/landing/fhir/Specimen"                             , JSON   , OverWrite),
      DatasetConf("raw_task"                       , clin_datalake, "/raw/landing/fhir/Task"                                 , JSON   , OverWrite),

      //public
      DatasetConf("normalized_1000_genomes"        , clin_datalake, "/public/1000_genomes"                               , PARQUET, OverWrite, TableConf("clin", "1000_genomes")),
      DatasetConf("normalized_cancer_hotspots"     , clin_datalake, "/public/cancer_hotspots"                            , PARQUET, OverWrite, TableConf("clin", "cancer_hotspots")),
      DatasetConf("normalized_clinvar"             , clin_datalake, "/public/clinvar"                                    , PARQUET, OverWrite, TableConf("clin", "clinvar")),
      DatasetConf("normalized_cosmic_gene_set"     , clin_datalake, "/public/cosmic_gene_set"                            , PARQUET, OverWrite, TableConf("clin", "cosmic_gene_set")),
      DatasetConf("normalized_dbnsfp_scores"       , clin_datalake, "/public/dbnsfp/parquet/scores"                      , PARQUET, OverWrite, TableConf("clin", "dbnsfp_scores")),
      DatasetConf("normalized_dbnsfp_annovar"      , clin_datalake, "/public/annovar/dbnsfp"                             , PARQUET, OverWrite, TableConf("clin", "dbnsfp_annovar")),
      DatasetConf("normalized_dbnsfp_original"     , clin_datalake, "/public/dbnsfp/scores"                              , PARQUET, OverWrite, TableConf("clin", "dbnsfp_original")),
      DatasetConf("normalized_dbsnp"               , clin_datalake, "/public/dbsnp"                                      , PARQUET, OverWrite, TableConf("clin", "dbsnp")),
      DatasetConf("normalized_ddd_gene_set"        , clin_datalake, "/public/ddd_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "ddd_gene_set")),
      DatasetConf("normalized_ensembl_mapping"     , clin_datalake, "/public/ensembl_mapping"                            , PARQUET, OverWrite, TableConf("clin", "ensembl_mapping")),
      DatasetConf("enriched_genes"                 , clin_datalake, "/public/genes"                                      , PARQUET, OverWrite, TableConf("clin", "genes")),
      DatasetConf("normalized_gnomad_genomes_2_1_1", clin_datalake, "/public/gnomad/gnomad_genomes_2.1.1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_2_1_1")),
      DatasetConf("normalized_gnomad_exomes_2_1_1" , clin_datalake, "/public/gnomad/gnomad_exomes_2.1.1_liftover_grch38" , PARQUET, OverWrite, TableConf("clin", "gnomad_exomes_2_1_1")),
      DatasetConf("normalized_gnomad_genomes_3_0"  , clin_datalake, "/public/gnomad/gnomad_genomes_3.0"                  , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_0")),
      DatasetConf("normalized_gnomad_genomes_3_1_1", clin_datalake, "/public/gnomad/gnomad_genomes_3_1_1"                , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_1_1")),
      DatasetConf("normalized_human_genes"         , clin_datalake, "/public/human_genes"                                , PARQUET, OverWrite, TableConf("clin", "human_genes")),
      DatasetConf("normalized_hpo_gene_set"        , clin_datalake, "/public/hpo_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "hpo_gene_set")),
      DatasetConf("normalized_omim_gene_set"       , clin_datalake, "/public/omim_gene_set"                              , PARQUET, OverWrite, TableConf("clin", "omim_gene_set")),
      DatasetConf("normalized_orphanet_gene_set"   , clin_datalake, "/public/orphanet_gene_set"                          , PARQUET, OverWrite, TableConf("clin", "orphanet_gene_set")),
      DatasetConf("normalized_topmed_bravo"        , clin_datalake, "/public/topmed_bravo"                               , PARQUET, OverWrite, TableConf("clin", "topmed_bravo")),

      //fhir
      DatasetConf("normalized_clinical_impression" , clin_datalake, "/normalized/fhir/ClinicalImpression", DELTA  , OverWrite   , TableConf("clin", "fhir_clinical_impression")),
      DatasetConf("normalized_group"               , clin_datalake, "/normalized/fhir/Group"             , DELTA  , OverWrite   , TableConf("clin", "fhir_group")),
      DatasetConf("normalized_observation"         , clin_datalake, "/normalized/fhir/Observation"       , DELTA  , OverWrite   , TableConf("clin", "fhir_observation")),
      DatasetConf("normalized_organization"        , clin_datalake, "/normalized/fhir/Organization"      , DELTA  , OverWrite   , TableConf("clin", "fhir_organization")),
      DatasetConf("normalized_patient"             , clin_datalake, "/normalized/fhir/Patient"           , DELTA  , OverWrite   , TableConf("clin", "fhir_patient")),
      DatasetConf("normalized_practitioner"        , clin_datalake, "/normalized/fhir/Practitioner"      , DELTA  , OverWrite   , TableConf("clin", "fhir_practitioner")),
      DatasetConf("normalized_practitioner_role"   , clin_datalake, "/normalized/fhir/PractitionerRole"  , DELTA  , OverWrite   , TableConf("clin", "fhir_practitioner_role")),
      DatasetConf("normalized_service_request"     , clin_datalake, "/normalized/fhir/ServiceRequest"    , DELTA  , OverWrite   , TableConf("clin", "fhir_service_request")),
      DatasetConf("normalized_specimen"            , clin_datalake, "/normalized/fhir/specimen"          , DELTA  , OverWrite   , TableConf("clin", "fhir_specimen")),
      DatasetConf("normalized_task"                , clin_datalake, "/normalized/fhir/task"              , DELTA  , OverWrite   , TableConf("clin", "fhir_task")),

      //clinical normalized
      DatasetConf("normalized_occurrences"         , clin_datalake, "/normalized/occurrences"            , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "normalized_occurrences"))),
      DatasetConf("normalized_variants"            , clin_datalake, "/normalized/variants"               , DELTA  , Scd1     , partitionby = List("chromosome"), table = Some(TableConf("clin", "normalized_variants")), keys = List("locus")),
      DatasetConf("normalized_consequences"        , clin_datalake, "/normalized/consequences"           , DELTA  , Scd1     , partitionby = List("chromosome"), table = Some(TableConf("clin", "normalized_consequences")), keys = List("ensembl_transcript_id")),

      //clinical enriched
      DatasetConf("enriched_occurrences"           , clin_datalake, "/enriched/occurrences"              , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "occurrences"))),
      DatasetConf("enriched_variants"              , clin_datalake, "/enriched/variants"                 , DELTA  , Scd1     , partitionby = List("chromosome"), table = Some(TableConf("clin", "variants")), keys = List("locus")),
      DatasetConf("enriched_consequences"          , clin_datalake, "/enriched/consequences"             , DELTA  , Scd1     , partitionby = List("chromosome"), table = Some(TableConf("clin", "consequences")), keys = List("ensembl_transcript_id")),

      //es index
      DatasetConf("es_index_gene_centric"          , clin_datalake, "/es_index/gene_centric"             , PARQUET, OverWrite, partitionby = List()            , table = Some(TableConf("clin", "gene_centric"))),
      DatasetConf("es_index_gene_suggestions"      , clin_datalake, "/es_index/gene_suggestions"         , PARQUET, OverWrite, partitionby = List()            , table = Some(TableConf("clin", "gene_suggestions"))),
      DatasetConf("es_index_variant_centric"       , clin_datalake, "/es_index/variant_centric"          , PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "variant_centric"))),
      DatasetConf("es_index_variant_suggestions"   , clin_datalake, "/es_index/variant_suggestions"      , PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "variant_suggestions"))),

    )

  val qa_conf = Configuration(
    storages = clin_qa_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(clin_qa_database, t.name)))),
    sparkconf = clin_spark_conf
  )

  val prd_conf = Configuration(
    storages = clin_prd_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(clin_prd_database, t.name)))),
    sparkconf = clin_spark_conf
  )

  val test_conf = Configuration(
    storages = List(),
    sources = sources,
    sparkconf = clin_spark_conf
  )

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prod.conf", prd_conf)

  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", test_conf)

}

