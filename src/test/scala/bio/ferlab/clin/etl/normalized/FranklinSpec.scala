package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.raw._
import bio.ferlab.clin.etl.normalized.Franklin.parseNullString
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.normalized.NormalizedFranklin
import bio.ferlab.clin.testutils.LoadResolverUtils.write
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.commons.config.{DatasetConf, RunStep, SimpleConfiguration}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}

class FranklinSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")
  val rawDf: DataFrame = Seq(
    RawFranklin(`batch_id` = "BAT1", `family_id` = "1", `aliquot_id` = "1", `analysis_id` = "1", `variants` = Seq(
      VARIANTS(
        `variant` = VARIANT(`chromosome` = "chr1"),
        `classification` = CLASSIFICATION(`acmg_rules` = Seq(
          ACMG_RULES(`name` = "PS1", `is_met` = true),
          ACMG_RULES(`name` = "PS2", `is_met` = false),
          ACMG_RULES(`name` = "PS3", `is_met` = true)))
      ),
      VARIANTS(`variant` = VARIANT(`chromosome` = "chr2")))
    )).toDF()

  val clinicalDf: DataFrame = Seq(
    EnrichedClinical(`analysis_id` = "SRA0001", `batch_id` = "BAT1", `aliquot_id` = "1", `family_id` = Some("1")),
    EnrichedClinical(`analysis_id` = "SRA0002", `batch_id` = "BAT2", `aliquot_id` = "2", `family_id` = Some("2"))
  ).toDF()

  override val dsToClean: List[DatasetConf] = List(raw_franklin, normalized_franklin, enriched_clinical)
  override val dbToCreate: List[String] = List("clin")

  val batchId = "BAT1"

  "transformSingle" should "normalize franklin data" in {
    val expected = Seq(
      NormalizedFranklin(`analysis_id` = "SRA0001", `batch_id` = "BAT1"),
      NormalizedFranklin(`analysis_id` = "SRA0001", `chromosome` = "2", `acmg_evidence` = Set())
    )

    val etl = franklinETL()

    val result = etl.transformSingle(Map(
      raw_franklin.id -> rawDf,
      enriched_clinical.id -> clinicalDf
    ))

    result
      .as[NormalizedFranklin]
      .collect() should contain theSameElementsAs expected
  }

  "transformSingle" should "should join clinical data using family_id or aliquot_id" in {
    val clinicalDf = Seq(
      EnrichedClinical(`analysis_id` = "SRA0001", `batch_id` = "BAT1", `aliquot_id` = "1", `family_id` = Some("1")),
      EnrichedClinical(`analysis_id` = "SRA0002", `batch_id` = "BAT1", `aliquot_id` = "2", `family_id` = Some("2"))
    ).toDF()

    val rawDf = Seq(
      // with family_id present and aliquot_id missing (family analysis)
      RawFranklin(`batch_id` = "BAT1", `family_id` = "1", `aliquot_id` = "null", `analysis_id` = "1", `variants` = Seq(
        VARIANTS(`variant` = VARIANT(`chromosome` = "chr1")),
        VARIANTS(`variant` = VARIANT(`chromosome` = "chr2"))
      )),
      // with family id missing and aliquot id present (solo analysis)
      RawFranklin(`batch_id` = "BAT1", `family_id` = "null", `aliquot_id` = "2", `analysis_id` = "1", `variants` = Seq(
        VARIANTS(`variant` = VARIANT(`chromosome` = "chr1"))
      ))
    ).toDF()

    val etl = franklinETL()
    val results = etl.transformSingle(Map(
      raw_franklin.id -> rawDf,
      enriched_clinical.id -> clinicalDf
    ))

    results
      .as[NormalizedFranklin]
      .collect() should contain theSameElementsAs Seq(
      NormalizedFranklin(`analysis_id` = "SRA0001", `batch_id` = "BAT1", `family_id` = Some("1"), `aliquot_id` = None, `acmg_evidence` = Set(), `chromosome` = "1"),
      NormalizedFranklin(`analysis_id` = "SRA0001", `batch_id` = "BAT1", `family_id` = Some("1"), `aliquot_id` = None, `acmg_evidence` = Set(), `chromosome` = "2"),
      NormalizedFranklin(`analysis_id` = "SRA0002", `batch_id` = "BAT1", `family_id` = None, `aliquot_id` = Some("2"), `acmg_evidence` = Set(), `chromosome` = "1")
    )
  }

  "extract" should "read clinical data and raw franklin data correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val clinicalDs = updatedConf.getDataset(enriched_clinical.id)
      write(clinicalDs, clinicalDf)(spark, updatedConf)

      val batchPath = raw_franklin.path.replace("{{BATCH_ID}}", batchId)
      val updatedRawDs = updatedConf.getDataset(raw_franklin.id).copy(path = batchPath)
      write(updatedRawDs, rawDf)(spark, updatedConf)

      val etl = franklinETL()(updatedConf, spark)
      val extracted = etl.extract()

      extracted(raw_franklin.id).as[RawFranklin].collect() should contain theSameElementsAs rawDf.as[RawFranklin].collect()
      extracted(enriched_clinical.id).as[EnrichedClinical].collect() should contain theSameElementsAs clinicalDf.as[EnrichedClinical].collect()
    }
  }

  "load" should "save the data correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // Prepare some normalized franklin data to save
      val normalizedFranklinData = Seq(
        NormalizedFranklin(`analysis_id` = "SRA0001"),
        NormalizedFranklin(`analysis_id` = "SRA0002")
      )

      // Load (save) the data
      val etl = franklinETL()(updatedConf, spark)
      etl.load(Map(normalized_franklin.id -> normalizedFranklinData.toDF()))

      // Read back and check
      val result = updatedConf.getDataset(normalized_franklin.id).read(updatedConf, spark)
      result.as[NormalizedFranklin].collect() should contain theSameElementsAs normalizedFranklinData
    }
  }

  it should "not fail when there is no franklin data" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)
      val clinicalDs = updatedConf.getDataset(enriched_clinical.id)
      write(clinicalDs, clinicalDf)(spark, updatedConf)

      val etl = franklinETL()(updatedConf, spark)
      etl.extract()(raw_franklin.id)
        .as[RawFranklin]
        .collect() shouldBe empty

      noException should be thrownBy etl.run()
    }
  }

  it should "not fail when franklin analysis is not completed" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // simulate clinical data
      val clinicalDs = updatedConf.getDataset(enriched_clinical.id)
      write(clinicalDs, clinicalDf)(spark, updatedConf)

      // simulate raw franklin data
      val batchPath = raw_franklin.path.replace("{{BATCH_ID}}", batchId)
      val updatedRawDs = updatedConf.getDataset(raw_franklin.id).copy(path = batchPath)
      write(updatedRawDs, rawDf)(spark, updatedConf)

      // simulate incomplete analysis by creating a .txt file in the raw franklin data location
      Files.createFile(Paths.get(updatedRawDs.location(updatedConf)).resolve("_FRANKLIN_IDS_.txt").toAbsolutePath)
      val fs = FileSystemResolver.resolve(updatedConf.getStorage(raw_franklin.storageid).filesystem)
      val sourceFiles = fs.list(updatedRawDs.location(updatedConf), recursive = true)
      sourceFiles.count(_.path.endsWith(".txt")) shouldBe 1

      // run the ETL and check that it does not fail
      val etl = franklinETL()(updatedConf, spark)
      noException should be thrownBy etl.run()
      normalized_franklin.read shouldBe empty
    }
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

  private def franklinETL(batchId: String = batchId, runSteps: Seq[RunStep] = default_load)(implicit
                                                                                            conf: SimpleConfiguration,
                                                                                            spark: SparkSession): Franklin = {
    Franklin(TestETLContext(runSteps = runSteps)(conf, spark), batchId = batchId)
  }
}
