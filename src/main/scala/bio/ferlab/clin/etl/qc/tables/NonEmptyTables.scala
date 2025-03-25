package bio.ferlab.clin.etl.qc.tables

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonEmptyTables extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotBeEmpty(fhir_clinical_impression, "fhir_clinical_impression"),
      shouldNotBeEmpty(fhir_code_system, "fhir_code_system"),
      shouldNotBeEmpty(fhir_observation, "fhir_observation"),
      shouldNotBeEmpty(fhir_organization, "fhir_organization"),
      shouldNotBeEmpty(fhir_patient, "fhir_patient"),
      shouldNotBeEmpty(fhir_practitioner, "fhir_practitioner"),
      shouldNotBeEmpty(fhir_practitioner_role, "fhir_practitioner_role"),
      shouldNotBeEmpty(fhir_service_request, "fhir_service_request"),
      shouldNotBeEmpty(fhir_specimen, "fhir_specimen"),
      shouldNotBeEmpty(fhir_task, "fhir_task"),
      shouldNotBeEmpty(fhir_family, "fhir_family"),
      shouldNotBeEmpty(fhir_document_reference, "fhir_document_reference"),
      shouldNotBeEmpty(clinical, "clinical"),
      shouldNotBeEmpty(normalized_snv, "normalized_snv"),
      shouldNotBeEmpty(normalized_cnv, "normalized_cnv"),
      shouldNotBeEmpty(normalized_variants, "normalized_variants"),
      shouldNotBeEmpty(normalized_consequences, "normalized_consequences"),
      shouldNotBeEmpty(normalized_panels, "normalized_panels"),
      shouldNotBeEmpty(normalized_exomiser, "normalized_exomiser"),
      shouldNotBeEmpty(normalized_snv_somatic, "normalized_snv_somatic"),
      shouldNotBeEmpty(normalized_cnv_somatic_tumor_only, "normalized_cnv_somatic_tumor_only"),
      shouldNotBeEmpty(normalized_coverage_by_gene, "normalized_coverage_by_gene"),
      shouldNotBeEmpty(snv_somatic, "snv_somatic"),
      shouldNotBeEmpty(snv, "snv"),
      shouldNotBeEmpty(cnv, "cnv"),
      shouldNotBeEmpty(variants, "variants"),
      shouldNotBeEmpty(consequences, "consequences"),
      shouldNotBeEmpty(coverage_by_gene, "coverage_by_gene"),
      shouldNotBeEmpty(gene_centric, "gene_centric"),
      shouldNotBeEmpty(gene_suggestions, "gene_suggestions"),
      shouldNotBeEmpty(variant_centric, "variant_centric"),
      shouldNotBeEmpty(cnv_centric, "cnv_centric"),
      shouldNotBeEmpty(coverage_by_gene_centric, "coverage_by_gene_centric"),
      shouldNotBeEmpty(variant_suggestions, "variant_suggestions"),
      shouldNotBeEmpty(thousand_genomes, "1000_genomes"),
      shouldNotBeEmpty(clinvar, "clinvar"),
      shouldNotBeEmpty(cosmic_gene_set, "cosmic_gene_set"),
      shouldNotBeEmpty(dbsnp, "dbsnp"),
      shouldNotBeEmpty(ddd_gene_set, "ddd_gene_set"),
      shouldNotBeEmpty(ensembl_mapping, "ensembl_mapping"),
      shouldNotBeEmpty(human_genes, "human_genes"),
      shouldNotBeEmpty(hpo_gene_set, "hpo_gene_set"),
      shouldNotBeEmpty(omim_gene_set, "omim_gene_set"),
      shouldNotBeEmpty(orphanet_gene_set, "orphanet_gene_set"),
      shouldNotBeEmpty(topmed_bravo, "topmed_bravo"),
      shouldNotBeEmpty(refseq_annotation, "refseq_annotation"),
      shouldNotBeEmpty(genes, "genes"),
      shouldNotBeEmpty(dbnsfp_original, "dbnsfp_original"),
      shouldNotBeEmpty(dbnsfp_annovar, "dbnsfp_annovar"),
      shouldNotBeEmpty(dbnsfp, "dbnsfp"),
      shouldNotBeEmpty(spliceai_indel, "spliceai_indel"),
      shouldNotBeEmpty(spliceai_snv, "spliceai_snv"),
      shouldNotBeEmpty(rare_variant_enriched, "rare_variant_enriched"),
      shouldNotBeEmpty(spliceai_enriched, "spliceai_enriched"),
      shouldNotBeEmpty(nextflow_svclustering, "nextflow_svclustering"),
      shouldNotBeEmpty(nextflow_svclustering_parental_origin, "nextflow_svclustering_parental_origin")
    )
  }
}
