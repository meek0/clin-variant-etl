package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class PrepareGeneSuggestionsSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("es_index_gene_suggestions")
  val es_index_gene_centric: DatasetConf = conf.getDataset("es_index_gene_centric")

  val data: Map[String, DataFrame] = Map(
    es_index_gene_centric.id -> Seq(
      GeneCentricOutput(symbol = "OR4F4", `entrez_gene_id` = 0, `omim_gene_id` = null, `hgnc` = null, `ensembl_gene_id` = null, `location` = null, name= null, `alias` = List(), `biotype` = null, `orphanet` = null,hpo=null,`omim` = null, chromosome=null, ddd=null, cosmic=null, `number_of_patients_snvs` = 2, `number_of_snvs_per_patient` = List(VARIANT_PER_PATIENT("PA0002", 1), VARIANT_PER_PATIENT("PA0001", 1)), hash="63592aea532cb1c022cbc13ea463513df18baf57"),
      GeneCentricOutput(symbol = "OR4F5"),
      GeneCentricOutput(symbol = "OR4F6", `number_of_patients_snvs` = 0, `number_of_snvs_per_patient` = List(), hash = "026aba5120030fcfbc29ebed8b2a1d78f90c07ad")
    ).toDF()
  )

  "transform PrepareGeneSuggestions" should "produce suggestions for genes" in {

    val result = PrepareGeneSuggestions(TestETLContext()).transformSingle(data)

    result.as[GeneSuggestionsOutput].collect() should contain allElementsOf Seq(
      GeneSuggestionsOutput(symbol = "OR4F4", `chromosome` = null, `ensembl_gene_id` = null, suggestion_id = "63592aea532cb1c022cbc13ea463513df18baf57", `suggest` = List(SUGGEST(4, List("OR4F4")), SUGGEST(2, List()))),
      GeneSuggestionsOutput(symbol = "OR4F5"),
      GeneSuggestionsOutput(symbol = "OR4F6", suggestion_id = "026aba5120030fcfbc29ebed8b2a1d78f90c07ad", `suggest` = List(SUGGEST(4, List("OR4F6")), SUGGEST(2, List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139", "ENSG00000198216")))))
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "GeneSuggestionsOutput", result, "src/test/scala/")
  }

}
