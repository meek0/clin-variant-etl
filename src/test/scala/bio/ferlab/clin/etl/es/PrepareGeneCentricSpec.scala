package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model.{DONORS, GeneCentricOutput, GenesOutput, VariantEnrichedOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrepareGeneCentricSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val genesDf: DataFrame = Seq(GenesOutput()).toDF()
  val variantsDf: DataFrame = Seq(
    VariantEnrichedOutput(
      `locus` = "1-10000-A-TAA",
      `genes_symbol` = List("OR4F5", "OR4F4"),
      `donors` = List(DONORS(`patient_id` = "PA0001"), DONORS(`patient_id` = "PA0002"))),
    VariantEnrichedOutput(
      `locus` = "1-10000-A-TA",
      `genes_symbol` = List("OR4F5"),
      `donors` = List(DONORS(`patient_id` = "PA0003"), DONORS(`patient_id` = "PA0002")))
  ).toDF()

  val destination: DatasetConf = conf.getDataset("es_index_gene_centric")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")

  val data = Map(
    enriched_genes.id -> genesDf,
    enriched_variants.id -> variantsDf
  )

  "Gene_centric transform" should "return data as GeneCentricOutput" in {

    val result = new PrepareGeneCentric("re_000").transform(data)
    result.columns should contain allElementsOf Seq("hash")
    result.as[GeneCentricOutput].collect() should contain allElementsOf Seq(GeneCentricOutput())
  }
}

