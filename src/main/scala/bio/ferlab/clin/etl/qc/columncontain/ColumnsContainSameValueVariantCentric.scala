package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric,
        variant_centric.columns.filterNot(List("variant_type", "assembly_version", "last_annotation_update").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.affected.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.non_affected.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.total.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.affected.*"),
        variant_centric.select($"frequency_RQDM.affected.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.non_affected.*"),
        variant_centric.select($"frequency_RQDM.non_affected.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.total.*"),
        variant_centric.select($"frequency_RQDM.total.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select("col.*"),
        variant_centric.select(explode($"donors")).select("col.*").columns.filterNot(List("has_alt", "last_update", "variant_type", "sequencing_strategy", "genome_build").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select(explode($"col.hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select(explode($"col.possibly_hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select($"col.exomiser.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select($"col.exomiser_other_moi.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.thousand_genomes.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.topmed_bravo.*"),
        variant_centric.select($"external_frequencies.topmed_bravo.*").columns.filterNot(List("an").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_2_1_1.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_exomes_2_1_1.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_0.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_1_1.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"clinvar.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.orphanet")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.hpo")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.omim")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select($"col.gnomad.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select($"col.spliceai.*")
      ),/*
      shouldNotContainSameValue(
        variant_centric.select($"varsome.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"varsome.publications")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"varsome.acmg.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"varsome.acmg.verdict.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"varsome.acmg.classifications")).select("col.*"),
        variant_centric.select(explode($"varsome.acmg.classifications")).select("col.*").columns.filterNot(List("met_criteria").contains(_)): _*
      ),*/
      shouldNotContainSameValue(
        variant_centric.select($"cmc.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"consequences")).select("col.*"),
        variant_centric.select(explode($"consequences")).select("col.*").columns.filterNot(List("feature_type", "picked").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"consequences")).select("col.predictions.*")
      ),
    )
  }
}
