package bio.ferlab.clin.etl.qc.dictionary

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions.explode

object DictionariesDonors extends TestingApp {
  run { spark =>
    import spark.implicits._
    
    handleErrors(
      shouldValuesContainedInDictionary(variants_donors.select($"col.affected_status_code"), DicAffectedStatusCode: _*)("affected_status_code"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.father_zygosity"), DicFatherZygosity: _*)("father_zygosity"),
      shouldValuesContainedInDictionary(variants_donors.select(explode($"col.filters")), DicFilters: _*)("filters"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.gender"), DicGender: _*)("gender"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.is_hc"), DicIsHc: _*)("is_hc"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.is_possibly_hc"), DicIsPossiblyHc: _*)("is_possibly_hc"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.mother_zygosity"), DicMotherZygosity: _*)("mother_zygosity"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.analysis_code"), DicPanels: _*)("analysis_code"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.parental_origin"), DicParentalOrigin: _*)("parental_origin"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.transmission"), DicTransmission: _*)("transmission"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.zygosity"), DicZygosity: _*)("zygosity"),
      shouldValuesContainedInDictionary(variants_donors.select($"col.exomiser.acmg_classification"), DicExoACMGClass: _*)("donors.exomiser.acmg_classification")
    )
  }
}
