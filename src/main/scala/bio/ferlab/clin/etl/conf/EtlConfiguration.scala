package bio.ferlab.clin.etl.conf

import bio.ferlab.datalake.spark3.config._
import bio.ferlab.datalake.spark3.loader.Format.{DELTA, JSON, PARQUET, VCF}
import bio.ferlab.datalake.spark3.loader.LoadType.{Insert, OverWrite, Upsert}

object EtlConfiguration extends App {

  val alias = "clin_storage"

  val clin_storage = List(
    StorageConf(alias, "s3a://clin")
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
      storages = clin_storage,
      sources = List(
        //raw
        // /vcf/complete_joint_calling.vqsr.bcftools_norm_ref.vep_20191218.SeqCap_EZ_Exome_v3_capture.vcf.gz
        DatasetConf("raw_variant_calling"    , alias, "/vcf/complete_joint_calling.*.vcf.gz"        , VCF    , OverWrite),
        DatasetConf("raw_clinical_impression", alias, "/raw/landing/fhir/ClinicalImpression"        , JSON   , OverWrite),
        DatasetConf("raw_group"              , alias, "/raw/landing/fhir/Group"                     , JSON   , OverWrite),
        DatasetConf("raw_observation"        , alias, "/raw/landing/fhir/Observation"               , JSON   , OverWrite),
        DatasetConf("raw_organization"       , alias, "/raw/landing/fhir/Organization"              , JSON   , OverWrite),
        DatasetConf("raw_patient"            , alias, "/raw/landing/fhir/Patient"                   , JSON   , OverWrite),
        DatasetConf("raw_practitioner"       , alias, "/raw/landing/fhir/Practitioner"              , JSON   , OverWrite),
        DatasetConf("raw_practitioner_role"  , alias, "/raw/landing/fhir/PractitionerRole"          , JSON   , OverWrite),
        DatasetConf("raw_service_request"    , alias, "/raw/landing/fhir/ServiceRequest"            , JSON   , OverWrite),
        DatasetConf("raw_specimen"           , alias, "/raw/landing/fhir/Specimen"                  , JSON   , OverWrite),
        DatasetConf("raw_task"               , alias, "/raw/landing/fhir/Task"                      , JSON   , OverWrite),

        //public
        DatasetConf("1000_genomes"           , alias, "/public/1000_genomes"                               , PARQUET, OverWrite, TableConf("clin", "1000_genomes")),
        DatasetConf("cancer_hotspots"        , alias, "/public/cancer_hotspots"                            , PARQUET, OverWrite, TableConf("clin", "cancer_hotspots")),
        DatasetConf("clinvar"                , alias, "/public/clinvar"                                    , PARQUET, OverWrite, TableConf("clin", "clinvar")),
        DatasetConf("cosmic_gene_set"        , alias, "/public/cosmic_gene_set"                            , PARQUET, OverWrite, TableConf("clin", "cosmic_gene_set")),
        DatasetConf("dbnsfp"                 , alias, "/public/dbnsfp/clin"                                , PARQUET, OverWrite, TableConf("clin", "dbnsfp")),
        DatasetConf("dbnsfp_annovar"         , alias, "/public/annovar/dbnsfp"                             , PARQUET, OverWrite, TableConf("clin", "dbnsfp_annovar")),
        DatasetConf("dbnsfp_original"        , alias, "/public/dbnsfp/scores"                              , PARQUET, OverWrite, TableConf("clin", "dbnsfp_original")),
        DatasetConf("dbsnp"                  , alias, "/public/dbsnp"                                      , PARQUET, OverWrite, TableConf("clin", "dbsnp")),
        DatasetConf("ddd_gene_set"           , alias, "/public/ddd_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "ddd_gene_set")),
        DatasetConf("ensembl_mapping"        , alias, "/public/ensembl_mapping"                            , PARQUET, OverWrite, TableConf("clin", "ensembl_mapping")),
        DatasetConf("genes"                  , alias, "/public/genes"                                      , PARQUET, OverWrite, TableConf("clin", "genes")),
        DatasetConf("gnomad_genomes_2_1_1"   , alias, "/public/gnomad/gnomad_genomes_2.1.1_liftover_grch38", PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_2_1_1")),
        DatasetConf("gnomad_exomes_2_1_1"    , alias, "/public/gnomad/gnomad_exomes_2.1.1_liftover_grch38" , PARQUET, OverWrite, TableConf("clin", "gnomad_exomes_2_1_1")),
        DatasetConf("gnomad_genomes_3_0"     , alias, "/public/gnomad/gnomad_genomes_3.0"                  , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_0")),
        DatasetConf("gnomad_genomes_3_1_1"   , alias, "/public/gnomad/gnomad_genomes_3_1_1"                , PARQUET, OverWrite, TableConf("clin", "gnomad_genomes_3_1_1")),
        DatasetConf("human_genes"            , alias, "/public/human_genes"                                , PARQUET, OverWrite, TableConf("clin", "human_genes")),
        DatasetConf("hpo_gene_set"           , alias, "/public/hpo_gene_set"                               , PARQUET, OverWrite, TableConf("clin", "hpo_gene_set")),
        DatasetConf("omim_gene_set"          , alias, "/public/omim_gene_set"                              , PARQUET, OverWrite, TableConf("clin", "omim_gene_set")),
        DatasetConf("orphanet_gene_set"      , alias, "/public/orphanet_gene_set"                          , PARQUET, OverWrite, TableConf("clin", "orphanet_gene_set")),
        DatasetConf("topmed_bravo"           , alias, "/public/topmed_bravo"                               , PARQUET, OverWrite, TableConf("clin", "topmed_bravo")),

        //fhir
        DatasetConf("normalized_clinical_impression"    , alias, "/normalized/fhir/ClinicalImpression", DELTA  , Upsert   , TableConf("clin", "clinical_impression")),
        DatasetConf("normalized_group"                  , alias, "/normalized/fhir/Group"             , DELTA  , Upsert   , TableConf("clin", "group")),
        DatasetConf("normalized_observation"            , alias, "/normalized/fhir/Observation"       , DELTA  , Upsert   , TableConf("clin", "observation")),
        DatasetConf("normalized_organization"           , alias, "/normalized/fhir/Organization"      , DELTA  , Upsert   , TableConf("clin", "organization")),
        DatasetConf("normalized_patient"                , alias, "/normalized/fhir/Patient"           , DELTA  , Upsert   , TableConf("clin", "patient")),
        DatasetConf("normalized_practitioner"           , alias, "/normalized/fhir/Practitioner"      , DELTA  , Upsert   , TableConf("clin", "practitioner")),
        DatasetConf("normalized_practitioner_role"      , alias, "/normalized/fhir/PractitionerRole"  , DELTA  , Upsert   , TableConf("clin", "practitioner_role")),
        DatasetConf("normalized_service_request"        , alias, "/normalized/fhir/ServiceRequest"    , DELTA  , Upsert   , TableConf("clin", "service_request")),
        DatasetConf("normalized_specimen"               , alias, "/normalized/fhir/specimen"          , DELTA  , Upsert   , TableConf("clin", "specimen")),
        DatasetConf("normalized_task"                   , alias, "/normalized/fhir/task"              , DELTA  , Upsert   , TableConf("clin", "task")),

        //clinical normalized
        DatasetConf("normalized_occurrences" , alias, "/normalized/occurrences"                     , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "occurrences"))),
        DatasetConf("normalized_variants"    , alias, "/normalized/variants"                        , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "variants"))),
        DatasetConf("normalized_consequences", alias, "/normalized/consequences"                    , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin_normalized", "consequences"))),

        //clinical enriched
        DatasetConf("enriched_occurrences"   , alias, "/enriched/occurrences"                       , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "occurrences"))),
        DatasetConf("enriched_variants"      , alias, "/enriched/variants"                          , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "variants"))),
        DatasetConf("enriched_consequences"  , alias, "/enriched/consequences"                      , DELTA  , Insert   , partitionby = List("chromosome"), table = Some(TableConf("clin", "consequences"))),
      ),
      sparkconf = clin_spark_conf
    )

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", clin_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/production.conf", clin_conf)

  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", clin_conf)

}

