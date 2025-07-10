package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.AnalysisIds
import bio.ferlab.clin.etl.model.raw._
import bio.ferlab.clin.etl.normalized.Franklin.parseNullString
import bio.ferlab.clin.model.normalized.NormalizedFranklin
import bio.ferlab.clin.testutils.LoadResolverUtils.write
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.commons.config.{DatasetConf, RunStep, SimpleConfiguration}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import java.nio.file.{Files, Paths}

class FranklinSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")
  val defaultRawFranklinData: Map[String, Seq[RawFranklin]] = Map(
    "SRA0001" -> Seq(
      RawFranklin(
        `analysis_id` = "SRA0001", `aliquot_id` = "1", `franklin_analysis_id` = "1",
        `variants` = Seq(
          VARIANTS(
            `variant` = VARIANT(`chromosome` = "chr1"),
            `classification` = CLASSIFICATION(`acmg_rules` = Seq(
              ACMG_RULES(`name` = "PS1", `is_met` = true),
              ACMG_RULES(`name` = "PS2", `is_met` = false),
              ACMG_RULES(`name` = "PS3", `is_met` = true)))
          ),
          VARIANTS(`variant` = VARIANT(`chromosome` = "chr2"))
        )
      )
    )
  )
  val defaultAnalysisIds: Seq[String] = defaultRawFranklinData.keys.toSeq


  override val dsToClean: List[DatasetConf] = List(raw_franklin, normalized_franklin)
  override val dbToCreate: List[String] = List("clin")

  "extract" should "read raw franklin data from all analysis ids that are ready" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // We simulate a case with several valid analysis. 
      val rawFranklinData: Map[String, Seq[RawFranklin]] = defaultRawFranklinData ++ Map(
        "SRA0002" -> Seq(
          RawFranklin(
            `analysis_id` = "SRA0002", `aliquot_id` = "2", `franklin_analysis_id` = "22",
            `variants` = Seq(VARIANTS(`variant` = VARIANT(`chromosome` = "chr3")))
          )
        )
      )
      writeRawFranklinData(rawFranklinData)(updatedConf, spark)

      // Running extract
      val etl = franklinETL(rawFranklinData.keys.toSeq)(updatedConf, spark)
      val extracted = etl.extract()

      // Data from all analysis should be included
      extracted.size shouldBe 1
      extracted(raw_franklin.id).as[RawFranklin].collect() should contain theSameElementsAs rawFranklinData.values.flatten
    }
  }

  it should "not include data from incomplete analysis" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      writeRawFranklinData()(updatedConf, spark)

      val extraRawFranklinData = Map(
        "SRA0002" -> Seq(
          RawFranklin(
            `analysis_id` = "SRA0002", `aliquot_id` = "2", `franklin_analysis_id` = "22",
            `variants` = Seq(VARIANTS(`variant` = VARIANT(`chromosome` = "chr3")))
          )
        ))
      writeRawFranklinData(extraRawFranklinData)(updatedConf, spark)

      // simulate incomplete analysis by writing a .txt file in the raw franklin data location
      val analysisPath = raw_franklin.path.replace("{{ANALYSIS_ID}}", "SRA0002")
      val analysisLocation = conf.getDataset(raw_franklin.id).copy(path = analysisPath).location(updatedConf)
      Files.createFile(Paths.get(analysisLocation).resolve("_FRANKLIN_IDS_.txt").toAbsolutePath)


      val etl = franklinETL(defaultAnalysisIds ++ extraRawFranklinData.keys.toSeq)(updatedConf, spark)
      val extracted = etl.extract()

      // We expect the extraction to return only the default raw franklin data as the analysis SRA0002 is incomplete
      extracted.size shouldBe 1
      extracted(raw_franklin.id).as[RawFranklin].collect() should contain theSameElementsAs defaultRawFranklinData.values.flatten
    }
  }

  "transformSingle" should "normalize franklin data" in {
    val expected = Seq(
      NormalizedFranklin(`analysis_id` = "SRA0001", `aliquot_id` = Some("1")),
      NormalizedFranklin(`analysis_id` = "SRA0001", `aliquot_id` = Some("1"), `chromosome` = "2", `acmg_evidence` = Set())
    )

    val etl = franklinETL()

    val result = etl.transformSingle(Map(
      raw_franklin.id -> defaultRawFranklinData.values.flatten.toSeq.toDF()
    ))

    result
      .as[NormalizedFranklin]
      .collect() should contain theSameElementsAs expected
  }

  it should "convert 'null' string for the aliquot id to a null value" in {
    val rawFranklinDf = Seq(
      RawFranklin(
        `analysis_id` = "SRA0001", `aliquot_id` = "null", `franklin_analysis_id` = "22",
        `variants` = Seq(VARIANTS(`variant` = VARIANT(`chromosome` = "chr3")))
      ),
    ).toDF()

    val expected = Seq(
      NormalizedFranklin(`analysis_id` = "SRA0001", `aliquot_id` = None, `franklin_analysis_id` = "22", `chromosome` = "3", acmg_evidence = Set()),
    )

    val etl = franklinETL()

    val result = etl.transformSingle(Map(raw_franklin.id -> rawFranklinDf))

    result.as[NormalizedFranklin].collect() should contain theSameElementsAs expected
    result.select("aliquot_id").as[String].collect() should contain theSameElementsAs Seq(null)
  }

  it should "cast franklin analysis id to string if parsed as a numerical value by spark" in {
    val rawFranklinDf = Seq(
      RawFranklin(
        `analysis_id` = "SRA0001", `aliquot_id` = "1",
        `variants` = Seq(VARIANTS(`variant` = VARIANT(`chromosome` = "chr3")))
      ),
    ).toDF().withColumn("franklin_analysis_id", lit(22)) // Simulating a numerical franklin_analysis_id parsed by spark

    val expected = Seq(
      NormalizedFranklin(`analysis_id` = "SRA0001", `aliquot_id` = Some("1"), `franklin_analysis_id` = "22", `chromosome` = "3", acmg_evidence = Set()),
    )

    val etl = franklinETL()

    val result = etl.transformSingle(Map(raw_franklin.id -> rawFranklinDf))
    result.as[NormalizedFranklin].collect() should contain theSameElementsAs expected
  }

  "run" should "not fail when franklin analysis is not completed" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // simulate raw franklin data
      writeRawFranklinData()(updatedConf, spark)

      // simulate incomplete analysis by creating a .txt file in the raw franklin data location
      val analysisPath = raw_franklin.path.replace("{{ANALYSIS_ID}}", defaultAnalysisIds.head)
      val analysisLocation = conf.getDataset(raw_franklin.id).copy(path = analysisPath).location(updatedConf)
      Files.createFile(Paths.get(analysisLocation).resolve("_FRANKLIN_IDS_.txt").toAbsolutePath)
      val fs = FileSystemResolver.resolve(updatedConf.getStorage(raw_franklin.storageid).filesystem)
      val sourceFiles = fs.list(analysisLocation, recursive = true)
      sourceFiles.count(_.path.endsWith(".txt")) shouldBe 1

      // run the ETL and check that it does not fail
      val etl = franklinETL()(updatedConf, spark)
      noException should be thrownBy etl.run()
      normalized_franklin.read shouldBe empty
    }
  }

  "Franklin.run" should "throw an exception if the list of analysis id is empty" in {
    an[IllegalArgumentException] should be thrownBy {
      Franklin.run(
        TestETLContext(runSteps = default_load)(conf, spark),
        analysisIds = AnalysisIds(ids = Seq.empty)
      )
    }
  }

  "parseNullString" should "parse null strings as null values" in {
    val df = Seq(
      RawFranklin(`aliquot_id` = "null"),
      RawFranklin(`aliquot_id` = "12345"),
    ).toDF()

    val expected = Seq(
      ("12345"),
      (null)
    )

    df.select(
        parseNullString("aliquot_id")
      )
      .as[String]
      .collect() should contain theSameElementsAs expected
  }


  private def franklinETL(analysisIds: Seq[String] = defaultAnalysisIds, runSteps: Seq[RunStep] = default_load)(implicit
                                                                                                                conf: SimpleConfiguration,
                                                                                                                spark: SparkSession): Franklin = {
    Franklin(TestETLContext(runSteps = runSteps)(conf, spark), analysisIds = analysisIds)
  }

  private def writeRawFranklinData(rawFranklinData: Map[String, Seq[RawFranklin]] = defaultRawFranklinData)(implicit conf: SimpleConfiguration, spark: SparkSession): Unit = {
    rawFranklinData.foreach { case (analysisId, analysisData) =>
      val analysisPath = raw_franklin.path.replace("{{ANALYSIS_ID}}", analysisId)
      val rawDs = conf.getDataset(raw_franklin.id).copy(path = analysisPath)
      write(rawDs, analysisData.toDF())(spark, conf)
    }
  }

}
