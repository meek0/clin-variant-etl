package bio.ferlab.clin.etl.qc.dictionary

import Dictionaries._
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object DictionariesDonors extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.affected_status_code"), DicAffectedStatusCode: _*)("affected_status_code"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.father_zygosity"), DicFatherZygosity: _*)("father_zygosity"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select(explode($"col.filters")), DicFilters: _*)("filters"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.gender"), DicGender: _*)("gender"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.is_hc"), DicIsHc: _*)("is_hc"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.is_possibly_hc"), DicIsPossiblyHc: _*)("is_possibly_hc"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.mother_zygosity"), DicMotherZygosity: _*)("mother_zygosity"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.analysis_code"), DicPanels: _*)("analysis_code"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.parental_origin"), DicParentalOrigin: _*)("parental_origin"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.transmission"), DicTransmission: _*)("transmission"),
      shouldValuesContainedInDictionary(variant_centric.select(explode($"donors")).select($"col.zygosity"), DicZygosity: _*)("zygosity")
    )
  }
}
