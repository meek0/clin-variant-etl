package bio.ferlab.clin.etl.qc.dictionary

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_Consequences.variant_centric
import org.apache.spark.sql.functions.explode

object DictionariesConsequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldValuesContainedInDictionary(variants_consequences.select(explode($"col.consequences")), DicConsequences: _*)("consequences"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.predictions.fathmm_pred"), DicFathmmPred: _*)("fathmm_pred"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.biotype"), DicGenesBiotype: _*)("biotype"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.predictions.lrt_pred"), DicLrtPred: _*)("lrt_pred"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.predictions.polyphen2_hvar_pred"), DicPolyphen2HvarPred: _*)("polyphen2_hvar_pred"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.predictions.sift_pred"), DicSiftPred: _*)("sift_pred"),
      shouldValuesContainedInDictionary(variants_consequences.select($"col.vep_impact"), DicVepImpact: _*)("vep_impact")
    )
  }
}
