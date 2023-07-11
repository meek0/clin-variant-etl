package bio.ferlab.clin.etl.qc.dictionary

import Dictionaries._
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object DictionariesConsequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select(explode($"col.consequences")), DicConsequences: _*)("consequences"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.predictions.fathmm_pred"), DicFathmmPred: _*)("fathmm_pred"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.biotype"), DicGenesBiotype: _*)("biotype"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.predictions.lrt_pred"), DicLrtPred: _*)("lrt_pred"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.predictions.polyphen2_hvar_pred"), DicPolyphen2HvarPred: _*)("polyphen2_hvar_pred"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.predictions.sift_pred"), DicSiftPred: _*)("sift_pred"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"consequences")).select($"col.vep_impact"), DicVepImpact: _*)("vep_impact")
    )
  }
}
