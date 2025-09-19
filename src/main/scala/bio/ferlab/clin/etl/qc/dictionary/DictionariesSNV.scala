package bio.ferlab.clin.etl.qc.dictionary

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions.explode

object DictionariesSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldValuesContainedInDictionary(variant_centric.select($"chromosome"), DicChromosome: _*)("chromosome"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"clinvar.clin_sig")), DicClinvar: _*)("clinvar"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"gene_external_reference")), DicGeneExternalReference: _*)("gene_external_reference"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"panels")), DicPanels: _*)("panels"),
      shouldValuesContainedInDictionary(variant_centric.select($"variant_class"), DicVariantClass: _*)("variant_class"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"variant_external_reference")), DicVariantExternalReference: _*)("variant_external_reference"),
      shouldValuesContainedInDictionary(variant_centric.select($"cmc.tier"), DicCmcTier: _*)("cmc.tier"),
      shouldValuesContainedInDictionary(variant_centric.select($"hotspot"), DicHotspot: _*)("hotspot"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"genes")).select(explode($"col.omim.inheritance_code")).select(explode($"col")), DicOMIM: _*)("genes.omim.inheritance_code"),
      shouldValuesContainedInDictionary(variant_centric.select($"franklin_max.acmg_classification"), DicFraACMGClassMax: _*)("franklin_max.acmg_classification"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"franklin_max.acmg_evidence")), DicFraACMGEviMax: _*)("franklin_max.acmg_evidence")
    )
  }
}
