package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*"),
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*").columns.filterNot(List("analysis_display_name"/*CLIN-1358*/).contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.non_affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.total.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"frequency_RQDM.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"frequency_RQDM.affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"frequency_RQDM.non_affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"frequency_RQDM.total.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select("col.*"),
        variant_centric.select(explode($"donors")).select("col.*").columns.filterNot(List("analysis_display_name"/*CLIN-1358*/, "practitioner_role_id").contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select(explode($"col.hc_complement")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select(explode($"col.possibly_hc_complement")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.thousand_genomes.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.topmed_bravo.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.gnomad_genomes_2_1_1.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.gnomad_exomes_2_1_1.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_0.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_1_1.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"clinvar.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.orphanet")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.hpo")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.omim")).select("col.*")
      ),/*
      shouldNotContainOnlyNull(
        variant_centric.select($"varsome.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"varsome.publications")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"varsome.acmg.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"varsome.acmg.verdict.*"),
        variant_centric.select($"varsome.acmg.verdict.*").columns.filterNot(List("approx_score"/*CLIN-1510*/).contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"varsome.acmg.classifications")).select("col.*")
      ),*/
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"consequences")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"consequences")).select("col.predictions.*")
      ),
    )
  }
}
