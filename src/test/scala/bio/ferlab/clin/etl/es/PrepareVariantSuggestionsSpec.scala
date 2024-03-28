package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.CLINVAR
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}
import org.apache.spark.sql.DataFrame

class PrepareVariantSuggestionsSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("es_index_variant_suggestions")
  val es_index_variant_centric: DatasetConf = conf.getDataset("es_index_variant_centric")

  val data: Map[String, DataFrame] = Map(
    es_index_variant_centric.id -> Seq(VariantIndexOutput(`clinvar` = CLINVAR(`clinvar_id` = null), `consequences` = List(CONSEQUENCES(), CONSEQUENCES(`symbol` = null)))).toDF()
  )

  "transform PrepareVariantSuggestions" should "produce suggestions for variants" in {

    val result = PrepareVariantSuggestions(DeprecatedTestETLContext()).transformSingle(data)

    result.as[VariantSuggestionsOutput].collect().head shouldBe VariantSuggestionsOutput()
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "VariantSuggestionsOutput", result, "src/test/scala/")
  }

}
