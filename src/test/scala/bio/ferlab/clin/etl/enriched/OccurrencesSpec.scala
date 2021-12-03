package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OccurrencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")

  "Enriched occurrences job" should "filter occurrences by zygosity" in {

    val occurrencesDf = Seq(
      OccurrenceRawOutput(chromosome = "1"  , zygosity = "HET" , affected_status = true),
      OccurrenceRawOutput(chromosome = "1"  , zygosity = "HOM" , affected_status = true),
      OccurrenceRawOutput(chromosome = "1"  , zygosity = "UNK" , affected_status = true),
      OccurrenceRawOutput(chromosome = "1"  , zygosity = "WT"  , affected_status = true),
      OccurrenceRawOutput(chromosome = "1"  , zygosity = null  , affected_status = true)
    ).toDF()

    val inputData = Map(normalized_occurrences.id -> occurrencesDf)
    val df = new Occurrences("1").transform(inputData)
    val result = df.as[OccurrenceRawOutput].collect()

    result.length shouldBe 2

  }

}

