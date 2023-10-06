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
      shouldValuesContainedInDictionary(variant_centric.select(explode($"varsome.acmg.classifications")).select($"col.name"), DicVarsomeAcmgClassificationName: _*)("name"),
      shouldValuesContainedInDictionary(variant_centric.select($"varsome.acmg.verdict.verdict"), DicVarsomeAcmgVerdict: _*)("verdict"),
      shouldValuesContainedInDictionary(variant_centric.select($"cmc.tier"), DicCmcTier: _*)("cmc.tier"),
      shouldValuesContainedInDictionary(variant_centric.select($"hotspot"), DicHotspot: _*)("hotspot")
    )
  }
}
