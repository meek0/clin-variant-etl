package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.model.normalized.NormalizedConsequences
import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.EnrichedConsequences
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.scalatest.BeforeAndAfterAll

class ConsequencesSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll with BeforeAndAfterAll {

  import spark.implicits._

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  private val data = Map(
    normalized_consequences.id -> Seq(NormalizedConsequences(),NormalizedConsequences(`hgvsp` = "ENSP00000334393.3:p.Ser270=")).toDF(),
    dbnsfp_original.id -> Seq(Dbnsfp_originalOutput()).toDF,
    normalized_ensembl_mapping.id -> Seq(EnsemblMappingOutput()).toDF,
    enriched_genes.id -> Seq(EnrichedGenes()).toDF,
  )

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(enriched_consequences, enriched_genes)

  val etl = Consequences(TestETLContext(RunStep.default_load))

  override def beforeAll(): Unit = {
    super.beforeAll()

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val resultDf = etl.transformSingle(data)
    val result = resultDf.as[EnrichedConsequences].collect()

    //    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedConsequences", resultDf, "src/test/scala/")

    result(0) shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
    result(1) shouldBe EnrichedConsequences(
      `hgvsp` = "ENSP00000334393.3:p.Ser270=",
      `aa_change` = "p.Ser270=",
      `predictions` = null,
      `conservations` = null
    )
  }

  "consequences job" should "run" in {
    etl.run()

    val result = enriched_consequences.read.as[EnrichedConsequences].collect().head
    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }
}


