package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.ClassGenerator
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp

class PrepareGeneSuggestionsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val destination: DatasetConf = conf.getDataset("es_index_gene_suggestions")
  val es_index_gene_centric: DatasetConf = conf.getDataset("es_index_gene_centric")

  val data: Map[String, DataFrame] = Map(
    es_index_gene_centric.id -> Seq(GeneCentricOutput()).toDF()
  )

  "transform PrepareGeneSuggestions" should "produce suggestions for genes" in {

    val result = new PrepareGeneSuggestions("re_000").transform(data)

    result.show(false)
    result.as[GeneSuggestionsOutput].collect().head shouldBe GeneSuggestionsOutput()
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "GeneSuggestionsOutput", result, "src/test/scala/")
  }

}
