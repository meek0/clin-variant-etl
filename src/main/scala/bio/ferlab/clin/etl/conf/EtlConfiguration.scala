package bio.ferlab.clin.etl.conf

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA, GFF, JSON, PARQUET, VCF}
import bio.ferlab.datalake.commons.config.LoadType.{Insert, OverWrite, OverWritePartition, Scd1, Upsert}
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.datalake.spark3.publictables.PublicDatasets
import pureconfig.generic.auto._
object EtlConfiguration extends App {

  val clin_datalake = "clin_datalake"
  val clin_import = "clin_import"
  val clin_download = "clin_download"

  val clin_qa_database = "clin_qa"
  val clin_staging_database = "clin_staging"
  val clin_prd_database = "clin"

  val clin_qa_storage = List(
    StorageConf(clin_import, "s3a://cqgc-qa-app-files-import", S3),
    StorageConf(clin_datalake, "s3a://cqgc-qa-app-datalake", S3),
    StorageConf(clin_download, "s3a://cqgc-qa-app-download", S3)
  )

  val clin_staging_storage = List(
    StorageConf(clin_import, "s3a://cqgc-staging-app-files-import", S3),
    StorageConf(clin_datalake, "s3a://cqgc-staging-app-datalake", S3),
    StorageConf(clin_download, "s3a://cqgc-staging-app-download", S3)

  )

  val clin_prd_storage = List(
    StorageConf(clin_import, "s3a://cqgc-prod-app-files-import", S3),
    StorageConf(clin_datalake, "s3a://cqgc-prod-app-datalake", S3),
    StorageConf(clin_download, "s3a://cqgc-prod-app-download", S3)
  )

  val clin_spark_conf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.legacy.timeParserPolicy"-> "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.sql.mapKeyDedupPolicy" -> "LAST_WIN",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> "true",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true"
  )

  val tsv_with_headers = Map("sep" -> "\t", "header" -> "true")
  val csv_with_headers = Map("sep" -> ",", "header" -> "true")

  val sources =
    List(
      //s3://clin-{env}-app-files-import/201106_A00516_0169_AHFM3HDSXY/201106_A00516_0169_AHFM3HDSXY.hard-filtered.formatted.norm.VEP.vcf.gz
      DatasetConf("raw_snv"                   , clin_import  , "/{{BATCH_ID}}/*.norm.VEP.vcf.gz"                              , VCF , OverWrite),
      DatasetConf("raw_cnv"                   , clin_import  , "/{{BATCH_ID}}/*[0-9].cnv.vcf.gz"                              , VCF , OverWrite),
      DatasetConf("raw_cnv_somatic_tumor_only", clin_import  , "/{{BATCH_ID}}/*.dragen.WES_somatic-tumor_only.cnv.vcf.gz"     , VCF , OverWrite),
      DatasetConf("raw_exomiser"              , clin_import  , "/{{BATCH_ID}}/*.exomiser.variants.tsv"                        , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("raw_coverage_by_gene"      , clin_import  , "/{{BATCH_ID}}/*.coverage_by_gene.GENCODE_CODING_CANONICAL.csv", CSV , OverWrite, readoptions = csv_with_headers),
      DatasetConf("raw_clinical_impression"   , clin_datalake, "/raw/landing/fhir/ClinicalImpression"                         , JSON, OverWrite),
      DatasetConf("raw_observation"           , clin_datalake, "/raw/landing/fhir/Observation"                                , JSON, OverWrite),
      DatasetConf("raw_organization"          , clin_datalake, "/raw/landing/fhir/Organization"                               , JSON, OverWrite),
      DatasetConf("raw_patient"               , clin_datalake, "/raw/landing/fhir/Patient"                                    , JSON, OverWrite),
      DatasetConf("raw_practitioner"          , clin_datalake, "/raw/landing/fhir/Practitioner"                               , JSON, OverWrite),
      DatasetConf("raw_practitioner_role"     , clin_datalake, "/raw/landing/fhir/PractitionerRole"                           , JSON, OverWrite),
      DatasetConf("raw_service_request"       , clin_datalake, "/raw/landing/fhir/ServiceRequest"                             , JSON, OverWrite),
      DatasetConf("raw_specimen"              , clin_datalake, "/raw/landing/fhir/Specimen"                                   , JSON, OverWrite),
      DatasetConf("raw_task"                  , clin_datalake, "/raw/landing/fhir/Task"                                       , JSON, OverWrite),
      DatasetConf("raw_document_reference"    , clin_datalake, "/raw/landing/fhir/DocumentReference"                          , JSON, OverWrite),
      DatasetConf("raw_panels"                , clin_datalake, "/raw/landing/panels/panels.tsv"                               , CSV , OverWrite, readoptions = tsv_with_headers),
      DatasetConf("raw_franklin"              , clin_datalake, "/raw/landing/franklin/batch_id={{BATCH_ID}}"                  , JSON, OverWrite, partitionby = List("family_id","aliquot_id","analysis_id")),

      //old version of gnomad, should be removed
      //DatasetConf("normalized_gnomad_genomes_2_1_1", clin_datalake, "/public/gnomad/gnomad_genomes_2.1.1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_2_1_1")),
      //DatasetConf("normalized_gnomad_exomes_2_1_1", clin_datalake, "/public/gnomad/gnomad_exomes_2.1.1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_exomes_2_1_1")),
      DatasetConf("normalized_gnomad_genomes_3_0", clin_datalake, "/public/gnomad/gnomad_genomes_3.0", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_0")),
      //DatasetConf("normalized_gnomad_genomes_3_1_1", clin_datalake, "/public/gnomad/gnomad_genomes_3_1_1", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_1_1")),
      //old version of dbnsfp, should be removed after migration
      //DatasetConf("deprecated_normalized_dbnsfp_scores"       , clin_datalake, "/public/dbnsfp/parquet/scores"                      , PARQUET, OverWrite),
      DatasetConf("deprecated_normalized_dbnsfp_original"     , clin_datalake, "/public/dbnsfp/scores"                              , PARQUET, OverWrite),
      //varsome
      DatasetConf("normalized_varsome"             , clin_datalake, "/public/varsome"                                    , DELTA  , Upsert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "varsome")), keys = List("chromosome", "start", "reference", "alternate")),
      //fhir
      DatasetConf("normalized_clinical_impression" , clin_datalake, "/normalized/fhir/ClinicalImpression", DELTA  , OverWrite   , TableConf("clin", "fhir_clinical_impression")),
      DatasetConf("normalized_observation"         , clin_datalake, "/normalized/fhir/Observation"       , DELTA  , OverWrite   , TableConf("clin", "fhir_observation")),
      DatasetConf("normalized_organization"        , clin_datalake, "/normalized/fhir/Organization"      , DELTA  , OverWrite   , TableConf("clin", "fhir_organization")),
      DatasetConf("normalized_patient"             , clin_datalake, "/normalized/fhir/Patient"           , DELTA  , OverWrite   , TableConf("clin", "fhir_patient")),
      DatasetConf("normalized_practitioner"        , clin_datalake, "/normalized/fhir/Practitioner"      , DELTA  , OverWrite   , TableConf("clin", "fhir_practitioner")),
      DatasetConf("normalized_practitioner_role"   , clin_datalake, "/normalized/fhir/PractitionerRole"  , DELTA  , OverWrite   , TableConf("clin", "fhir_practitioner_role")),
      DatasetConf("normalized_service_request"     , clin_datalake, "/normalized/fhir/ServiceRequest"    , DELTA  , OverWrite   , TableConf("clin", "fhir_service_request")),
      DatasetConf("normalized_family"              , clin_datalake, "/normalized/fhir/family"            , DELTA  , OverWrite   , TableConf("clin", "fhir_family")),
      DatasetConf("normalized_specimen"            , clin_datalake, "/normalized/fhir/specimen"          , DELTA  , OverWrite   , TableConf("clin", "fhir_specimen")),
      DatasetConf("normalized_task"                , clin_datalake, "/normalized/fhir/task"              , DELTA  , OverWrite   , TableConf("clin", "fhir_task")),
      DatasetConf("normalized_document_reference"  , clin_datalake, "/normalized/fhir/DocumentReference" , DELTA  , OverWrite   , TableConf("clin", "fhir_document_reference")),
      DatasetConf("enriched_clinical"              , clin_datalake, "/enriched/clinical"                 , DELTA  , OverWrite   , TableConf("clin", "clinical")),

      //clinical normalized
      DatasetConf("normalized_snv"                            , clin_datalake, "/normalized/snv"                    , DELTA  , OverWritePartition, partitionby = List("batch_id", "chromosome"), table = Some(TableConf("clin", "normalized_snv"))),
      DatasetConf("normalized_snv_somatic"                    , clin_datalake, "/normalized/snv_somatic"            , DELTA  , OverWritePartition, partitionby = List("batch_id", "chromosome"), table = Some(TableConf("clin", "normalized_snv_somatic"))),
      DatasetConf("normalized_cnv"                            , clin_datalake, "/normalized/cnv"                    , DELTA  , OverWritePartition, partitionby = List("batch_id", "patient_id"), table = Some(TableConf("clin", "normalized_cnv"))),
      DatasetConf("normalized_cnv_somatic_tumor_only"         , clin_datalake, "/normalized/cnv_somatic_tumor_only" , DELTA  , OverWritePartition, partitionby = List("batch_id", "patient_id"), table = Some(TableConf("clin", "normalized_cnv_somatic_tumor_only"))),
      DatasetConf("normalized_variants"            , clin_datalake, "/normalized/variants"               , DELTA  , OverWritePartition, partitionby = List("batch_id", "chromosome"), table = Some(TableConf("clin", "normalized_variants"))),
      DatasetConf("normalized_consequences"        , clin_datalake, "/normalized/consequences"           , DELTA  , Scd1              , partitionby = List("chromosome")            , table = Some(TableConf("clin", "normalized_consequences")), keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")),
      DatasetConf("normalized_panels"              , clin_datalake, "/normalized/panels"                 , PARQUET, OverWrite         , partitionby = List()                        , table = Some(TableConf("clin", "normalized_panels"))),
      DatasetConf("normalized_exomiser"            , clin_datalake, "/normalized/exomiser"               , DELTA  , OverWritePartition, partitionby = List("batch_id")              , table = Some(TableConf("clin", "normalized_exomiser"))),
      DatasetConf("normalized_coverage_by_gene"    , clin_datalake, "/normalized/coverage_by_gene"       , DELTA  , OverWritePartition, partitionby = List("batch_id")              , table = Some(TableConf("clin", "normalized_coverage_by_gene"))),
      DatasetConf("normalized_franklin"            , clin_datalake, "/normalized/franklin"               , DELTA  , OverWritePartition, partitionby = List("batch_id")              , table = Some(TableConf("clin", "normalized_franklin"))),

      //clinical enriched
      DatasetConf("enriched_snv"                   , clin_datalake, "/enriched/snv"                      , DELTA  , OverWrite         , partitionby = List("chromosome")                               , table = Some(TableConf("clin", "snv"))              , keys = List("chromosome", "start", "reference", "alternate", "aliquot_id")),
      DatasetConf("enriched_snv_somatic"           , clin_datalake, "/enriched/snv_somatic"              , DELTA  , OverWritePartition, partitionby = List("analysis_service_request_id", "chromosome"), table = Some(TableConf("clin", "snv_somatic"))      , keys = List("chromosome", "start", "reference", "alternate", "aliquot_id", "bioinfo_analysis_code")),
      DatasetConf("enriched_cnv"                   , clin_datalake, "/enriched/cnv"                      , DELTA  , OverWrite         , partitionby = List("chromosome")                               , table = Some(TableConf("clin", "cnv"))              , keys = List("chromosome", "start", "reference", "alternate", "aliquot_id")),
      DatasetConf("enriched_variants"              , clin_datalake, "/enriched/variants"                 , DELTA  , OverWrite         , partitionby = List("chromosome")                               , table = Some(TableConf("clin", "variants"))),
      DatasetConf("enriched_consequences"          , clin_datalake, "/enriched/consequences"             , DELTA  , Scd1              , partitionby = List("chromosome")                               , table = Some(TableConf("clin", "consequences"))     , keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")),
      DatasetConf("enriched_coverage_by_gene"      , clin_datalake, "/enriched/coverage_by_gene"         , DELTA  , OverWrite         , partitionby = List("chromosome")                               , table = Some(TableConf("clin", "coverage_by_gene"))),

      //es index
      DatasetConf("es_index_gene_centric"            , clin_datalake, "/es_index/gene_centric"            , PARQUET, OverWrite, partitionby = List()            , table = Some(TableConf("clin", "gene_centric"))),
      DatasetConf("es_index_gene_suggestions"        , clin_datalake, "/es_index/gene_suggestions"        , PARQUET, OverWrite, partitionby = List()            , table = Some(TableConf("clin", "gene_suggestions"))),
      DatasetConf("es_index_variant_centric"         , clin_datalake, "/es_index/variant_centric"         , PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "variant_centric"))),
      DatasetConf("es_index_cnv_centric"             , clin_datalake, "/es_index/cnv_centric"             , PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "cnv_centric"))),
      DatasetConf("es_index_variant_suggestions"     , clin_datalake, "/es_index/variant_suggestions"     , PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "variant_suggestions"))),
      DatasetConf("es_index_coverage_by_gene_centric", clin_datalake, "/es_index/coverage_by_gene_centric", PARQUET, OverWrite, partitionby = List("chromosome"), table = Some(TableConf("clin", "coverage_by_gene_centric"))),

    ) ++ PublicDatasets(clin_datalake, tableDatabase = Some("clin"), viewDatabase = None).sources

  val qa_conf = SimpleConfiguration(DatalakeConf(
    storages = clin_qa_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(clin_qa_database, t.name)))),
    sparkconf = clin_spark_conf
  ))

  val staging_conf = SimpleConfiguration(DatalakeConf(
    storages = clin_staging_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(clin_staging_database, t.name)))),
    sparkconf = clin_spark_conf
  ))

  val prd_conf = SimpleConfiguration(DatalakeConf(
    storages = clin_prd_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(clin_prd_database, t.name)))),
    sparkconf = clin_spark_conf
  ))

  val test_conf = SimpleConfiguration(DatalakeConf(
    storages = List(),
    sources = sources,
    sparkconf = clin_spark_conf
  ))

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/staging.conf", staging_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prod.conf", prd_conf)

  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", test_conf)

}

