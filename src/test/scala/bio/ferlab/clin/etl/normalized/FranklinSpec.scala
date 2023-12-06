package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw._
import bio.ferlab.clin.etl.normalized.Franklin.parseNullString
import bio.ferlab.clin.model.normalized.NormalizedFranklin
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class FranklinSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")
  override val dsToClean: List[DatasetConf] = List(raw_franklin, normalized_franklin)

  val rawDf: DataFrame = Seq(
    RawFranklin(`batch_id` = "BAT1", `family_id` = "1", `aliquot_id` = "1", `analysis_id` = "1", `variants` = Seq(
      VARIANTS(`variant` = VARIANT(`chromosome` = "chr1"), `classification` = CLASSIFICATION(`acmg_rules` = Seq(
        ACMG_RULES(`name` = "PS1", `is_met` = true),
        ACMG_RULES(`name` = "PS2", `is_met` = false),
        ACMG_RULES(`name` = "PS3", `is_met` = true)))),
      VARIANTS(`variant` = VARIANT(`chromosome` = "chr2")))
    )).toDF()

  val batchId = "BAT1"
  val etl = Franklin(TestETLContext(runSteps = default_load), batchId = batchId)

  it should "normalize franklin data" in {
    val expected = Seq(
      NormalizedFranklin(),
      NormalizedFranklin(`chromosome` = "2", `franklin_acmg_evidence` = Set())
    )
    val result = etl.transformSingle(Map(raw_franklin.id -> rawDf))

    result
      .as[NormalizedFranklin]
      .collect() should contain theSameElementsAs expected
  }

  it should "not fail when there is no franklin data" in {
    etl.extract()(raw_franklin.id)
      .as[RawFranklin]
      .collect() shouldBe empty

    noException should be thrownBy etl.run()
  }

  it should "not fail when franklin analysis is not completed" in {
    val batchPath = raw_franklin.path.replace("{{BATCH_ID}}", batchId)
    val updatedRawDs = raw_franklin.copy(path = batchPath)
    LoadResolver
      .write(spark, conf)(raw_franklin.format, raw_franklin.loadtype)
      .apply(updatedRawDs, rawDf)
    Files.createFile(Paths.get(updatedRawDs.location).resolve("_FRANKLIN_IDS_.txt").toAbsolutePath)

    val fs = FileSystemResolver.resolve(conf.getStorage(raw_franklin.storageid).filesystem)
    val sourceFiles = fs.list(updatedRawDs.location, recursive = true)
    sourceFiles.count(_.path.endsWith(".txt")) shouldBe 1

    noException should be thrownBy etl.run()
    normalized_franklin.read shouldBe empty
  }

  "parseNullString" should "parse null strings as null values" in {
    val df = Seq(
      RawFranklin(`family_id` = null),
      RawFranklin(`aliquot_id` = null)
    ).toDF()

    val expected = Seq(
      (null, "12345"),
      ("1", null)
    )

    df.select(
      parseNullString("family_id"),
      parseNullString("aliquot_id")
    )
      .as[(String, String)]
      .collect() should contain theSameElementsAs expected
  }
}
