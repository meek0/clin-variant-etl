package bio.ferlab.clin.etl.conf

import bio.ferlab.datalake.spark3.config._
import bio.ferlab.datalake.spark3.loader.Format.{DELTA, JSON, PARQUET, VCF}
import bio.ferlab.datalake.spark3.loader.LoadType.{Insert, OverWrite}

object EtlConfiguration extends App {

  val clin_datalake = "clin_storage"
  val clin_import = "clin-import"

  val clin_qa_storage = List(
    StorageConf(clin_import, "s3a://clin-qa-app-files-import"),
    StorageConf(clin_datalake, "s3a://clin-qa-app-datalake")
  )

  val clin_prd_storage = List(
    StorageConf(clin_import, "s3a://clin-prd-app-files-import"),
    StorageConf(clin_datalake, "s3a://clin-prd-app-datalake")
  )

  val clin_spark_conf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true",
    "spark.sql.legacy.timeParserPolicy"-> "CORRECTED"
  )

  val clin_conf =
    Configuration(
      sources = List(
        //raw

        //s3://clin-{env}-app-files-import/201106_A00516_0169_AHFM3HDSXY/201106_A00516_0169_AHFM3HDSXY.hard-filtered.formatted.norm.VEP.vcf.gz
        DatasetConf("raw_variant_calling"    , clin_import  , "/{{BATCH_ID}}/*.hard-filtered.formatted.norm.VEP.vcf.gz", VCF    , OverWrite),
        DatasetConf("raw_clinical_impression", clin_datalake, "/raw/landing/fhir/ClinicalImpression"        , JSON   , OverWrite),
        DatasetConf("raw_group"              , clin_datalake, "/raw/landing/fhir/Group"                     , JSON   , OverWrite),
        DatasetConf("raw_observation"        , clin_datalake, "/raw/landing/fhir/Observation"               , JSON   , OverWrite),
        DatasetConf("raw_organization"       , clin_datalake, "/raw/landing/fhir/Organization"              , JSON   , OverWrite),
        DatasetConf("raw_patient"            , clin_datalake, "/raw/landing/fhir/Patient"                   , JSON   , OverWrite),
        DatasetConf("raw_practitioner"       , clin_datalake, "/raw/landing/fhir/Practitioner"              , JSON   , OverWrite),
        DatasetConf("raw_practitioner_role"  , clin_datalake, "/raw/landing/fhir/PractitionerRole"          , JSON   , OverWrite),
        DatasetConf("raw_service_request"    , clin_datalake, "/raw/landing/fhir/ServiceRequest"            , JSON   , OverWrite),
        DatasetConf("raw_specimen"           , clin_datalake, "/raw/landing/fhir/Specimen"                  , JSON   , OverWrite),
        DatasetConf("raw_task"               , clin_datalake, "/raw/landing/fhir/Task"                      , JSON   , OverWrite),

        //public
        DatasetConf("1000_genomes"           , clin_datalake, "/public/1000_genomes"                               , PARQUET, OverWrite, TableConf("clin", "1000_genomes")),
        DatasetConf("cancer_hotspots"        , clin_datalake, "/public/cancer_hotspots"                            , PARQUET, OverWrite, TableConf("clin", "cancer_hotspots")),
        DatasetConf("clinvar"                , clin_datalake, "/public/clinvar"                                    , PARQUET, OverWrite, TableConf("clin", "clinvar")),
        DatasetConf("cosmic_gene_set"        , clin_datalake, "/public/cosmic_gene_set"                            , PARQUET, OverWrite, TableConf("clin", "cosmic_gene_set")),
        DatasetConf("dbnsfp"                 , clin_datalake, "/public/dbnsfp/clin"                                , PARQUET, OverWrite, TableConf("clin", "dbnsfp")),
        DatasetConf("dbnsfp_annovar"         , clin_datalake, "/public/annovar/dbnsfp"                             , PARQUET, OverWrite, TableConf("clin", "dbnsfp_annovar")),
        DatasetConf("dbnsfp_original"        , clin_datalake, "/public/dbnsfp/scores"                              , PARQUET, OverWrite, TableConf("clin", "dbnsfp_original")),
        DatasetConf("dbsnp"                  , clin_datalake, "/public/dbsnp"                                      , PARQUET, OverWrite, TableConf("clin", "dbsnp")),
        DatasetConf("ddd_gene_set"           , clin_datalake, "/public/ddd_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "ddd_gene_set")),
        DatasetConf("ensembl_mapping"        , clin_datalake, "/public/ensembl_mapping"                            , PARQUET, OverWrite, TableConf("clin", "ensembl_mapping")),
        DatasetConf("genes"                  , clin_datalake, "/public/genes"                                      , PARQUET, OverWrite, TableConf("clin", "genes")),
        DatasetConf("gnomad_genomes_2_1_1"   , clin_datalake, "/public/gnomad/gnomad_genomes_2.1.1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_2_1_1")),
        DatasetConf("gnomad_exomes_2_1_1"    , clin_datalake, "/public/gnomad/gnomad_exomes_2.1.1_liftover_grch38" , PARQUET, OverWrite, TableConf("clin", "gnomad_exomes_2_1_1")),
        DatasetConf("gnomad_genomes_3_0"     , clin_datalake, "/public/gnomad/gnomad_genomes_3.0"                  , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_0")),
        DatasetConf("gnomad_genomes_3_1_1"   , clin_datalake, "/public/gnomad/gnomad_genomes_3_1_1_full"           , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_1_1")),
        DatasetConf("human_genes"            , clin_datalake, "/public/human_genes"                                , PARQUET, OverWrite, TableConf("clin", "human_genes")),
        DatasetConf("hpo_gene_set"           , clin_datalake, "/public/hpo_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "hpo_gene_set")),
        DatasetConf("omim_gene_set"          , clin_datalake, "/public/omim_gene_set"                              , PARQUET, OverWrite, TableConf("clin", "omim_gene_set")),
        DatasetConf("orphanet_gene_set"      , clin_datalake, "/public/orphanet_gene_set"                          , PARQUET, OverWrite, TableConf("clin", "orphanet_gene_set")),
        DatasetConf("topmed_bravo"           , clin_datalake, "/public/topmed_bravo"                               , PARQUET, OverWrite, TableConf("clin", "topmed_bravo")),

        //fhir
        DatasetConf("normalized_clinical_impression"    , clin_datalake, "/normalized/fhir/ClinicalImpression", DELTA  , OverWrite   , TableConf("clin", "clinical_impression")),
        DatasetConf("normalized_group"                  , clin_datalake, "/normalized/fhir/Group"             , DELTA  , OverWrite   , TableConf("clin", "group")),
        DatasetConf("normalized_observation"            , clin_datalake, "/normalized/fhir/Observation"       , DELTA  , OverWrite   , TableConf("clin", "observation")),
        DatasetConf("normalized_organization"           , clin_datalake, "/normalized/fhir/Organization"      , DELTA  , OverWrite   , TableConf("clin", "organization")),
        DatasetConf("normalized_patient"                , clin_datalake, "/normalized/fhir/Patient"           , DELTA  , OverWrite   , TableConf("clin", "patient")),
        DatasetConf("normalized_practitioner"           , clin_datalake, "/normalized/fhir/Practitioner"      , DELTA  , OverWrite   , TableConf("clin", "practitioner")),
        DatasetConf("normalized_practitioner_role"      , clin_datalake, "/normalized/fhir/PractitionerRole"  , DELTA  , OverWrite   , TableConf("clin", "practitioner_role")),
        DatasetConf("normalized_service_request"        , clin_datalake, "/normalized/fhir/ServiceRequest"    , DELTA  , OverWrite   , TableConf("clin", "service_request")),
        DatasetConf("normalized_specimen"               , clin_datalake, "/normalized/fhir/specimen"          , DELTA  , OverWrite   , TableConf("clin", "specimen")),
        DatasetConf("normalized_task"                   , clin_datalake, "/normalized/fhir/task"              , DELTA  , OverWrite   , TableConf("clin", "task")),

        //clinical normalized
        DatasetConf("normalized_occurrences" , clin_datalake, "/normalized/occurrences"                     , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "occurrences"))),
        DatasetConf("normalized_variants"    , clin_datalake, "/normalized/variants"                        , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "variants"))),
        DatasetConf("normalized_consequences", clin_datalake, "/normalized/consequences"                    , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "consequences"))),

        //clinical enriched
        DatasetConf("enriched_occurrences"   , clin_datalake, "/enriched/occurrences"                       , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "occurrences"))),
        DatasetConf("enriched_variants"      , clin_datalake, "/enriched/variants"                          , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "variants"))),
        DatasetConf("enriched_consequences"  , clin_datalake, "/enriched/consequences"                      , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "consequences"))),
      ),
      sparkconf = clin_spark_conf
    )

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", clin_conf.copy(storages = clin_qa_storage))
  ConfigurationWriter.writeTo("src/main/resources/config/production.conf", clin_conf.copy(storages = clin_prd_storage))

  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", clin_conf)

}

