package bio.ferlab.clin.etl.qc.dictionary

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions.explode

object DictionariesCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldValuesContainedInDictionary(cnv_centric.select($"chromosome"), DicChromosome: _*)("chromosome"),
      shouldValuesContainedInDictionary(cnv_centric.select(explode($"filters")), DicCNVFilters: _*)("filters"),
      shouldValuesContainedInDictionary(cnv_centric.select($"type"), DicCNVType: _*)("type"),
      shouldValuesContainedInDictionary(cnv_centric.select($"analysis_code"), DicPanels: _*)("analysis_code"),
      shouldValuesContainedInDictionary(cnv_centric.select(explode($"genes")).select(explode($"col.panels")), DicPanels: _*)("panels")
    )
  }
}
