package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.{GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN, VCF_CNV_SVClustering_Parental_Origin}
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow.SVClusteringParentalOrigin
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.RunStep.default_load
import bio.ferlab.datalake.commons.config.{DatasetConf, LoadType}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class NormalizeSVClusteringParentalOriginSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin_output")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin")

  val batchId = "BAT1"
  val analysis1Id = "SRA0001"
  val analysis2Id = "SRA0002"

  val clinicalDf: DataFrame = Seq(
    // Analysis 1
    EnrichedClinical(`patient_id` = "PA0001", `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0001", `batch_id` = batchId, `aliquot_id` = "11111", `is_proband` = true, `gender` = "Male", `affected_status` = true, `family_id` = Some("FM00001"), `mother_id` = Some("PA0003"), `father_id` = Some("PA0002")), // proband
    EnrichedClinical(`patient_id` = "PA0002", `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0002", `batch_id` = batchId, `aliquot_id` = "22222", `is_proband` = false, `gender` = "Male", `affected_status` = false, `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None), // father
    EnrichedClinical(`patient_id` = "PA0003", `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0003", `batch_id` = batchId, `aliquot_id` = "33333", `is_proband` = false, `gender` = "Female", `affected_status` = true, `family_id` = Some("FM00001"), `mother_id` = None, `father_id` = None), // mother

    // Analysis 2
    EnrichedClinical(`patient_id` = "PA0004", `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0004", `batch_id` = batchId, `aliquot_id` = "44444", `is_proband` = true, `gender` = "Male", `affected_status` = true, `family_id` = Some("FM00002"), `mother_id` = Some("PA0006"), `father_id` = Some("PA0005")), // proband
    EnrichedClinical(`patient_id` = "PA0005", `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0005", `batch_id` = batchId, `aliquot_id` = "55555", `is_proband` = false, `gender` = "Male", `affected_status` = false, `family_id` = Some("FM00002"), `mother_id` = None, `father_id` = None), // father
    EnrichedClinical(`patient_id` = "PA0006", `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0006", `batch_id` = batchId, `aliquot_id` = "66666", `is_proband` = false, `gender` = "Female", `affected_status` = true, `family_id` = Some("FM00002"), `mother_id` = None, `father_id` = None), // mother
  ).toDF

  val analysisSRA0001Results: DataFrame = Seq(
    VCF_CNV_SVClustering_Parental_Origin(
      `genotypes` = List(
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "11111", `calls` = List(0, 1)), // proband
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "22222", `calls` = List(0, 0)), // father
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "33333", `calls` = List(0, 1)), // mother
      )
    )
  ).toDF

  val analysisSRA0002Results: DataFrame = Seq(
    VCF_CNV_SVClustering_Parental_Origin(
      `genotypes` = List(
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "44444", `calls` = List(1, 1)), // proband
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "55555", `calls` = List(0, 1)), // father
        GENOTYPES_CNV_SVCLUSTERING_PARENTAL_ORIGIN(`sampleId` = "66666", `calls` = List(1, 1)), // mother
      )
    )
  ).toDF

  val allAnalysisResults: Map[String, DataFrame] = Map(
    analysis1Id -> analysisSRA0001Results,
    analysis2Id -> analysisSRA0002Results
  )

  override val dsToClean: List[DatasetConf] = List(enriched_clinical, mainDestination)

  it should "normalize svclustering parental origin results" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)
      val job = NormalizeSVClusteringParentalOrigin(TestETLContext()(updatedConf, spark), batchId)

      // Load input data to get input file name, since input file name is used to extract analysis_service_request_id
      allAnalysisResults.foreach { case (analysis_service_request_id, df) =>
        val dsPath = raw_variant_calling.path
        val updatedDsPath = dsPath
          .replace("{{BATCH_ID}}", batchId)
          .replace("svclustering*/*.vcf.gz", s"svclusteringdel/$analysis_service_request_id.MAX_CLIQUE_RO80.DEL.vcf.gz")
        val updatedDs = raw_variant_calling.copy(path = updatedDsPath)
        LoadResolver
          .write(spark, updatedConf)(updatedDs.format, LoadType.Insert)
          .apply(updatedDs, df)
      }

      LoadResolver
        .write(spark, updatedConf)(enriched_clinical.format, LoadType.OverWrite)
        .apply(enriched_clinical, clinicalDf)

      val data = job.extract()
      val result = job.transformSingle(data)

      result
        .as[SVClusteringParentalOrigin]
        .collect() should contain theSameElementsAs Seq(
        // Analysis 1
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0001",
          `aliquot_id` = "11111", `patient_id` = "PA0001", `gender` = "Male", `family_id` = "FM00001", `mother_id` = Some("PA0003"), `father_id` = Some("PA0002"),
          `calls` = List(0, 1), `mother_calls` = Some(List(0, 1)), `father_calls` = Some(List(0, 0)),
          `affected_status` = true, `mother_affected_status` = Some(true), `father_affected_status` = Some(false),
          `zygosity` = "HET", `parental_origin` = Some("mother"), `transmission` = "autosomal_dominant"), // Proband
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0002",
          `aliquot_id` = "22222", `patient_id` = "PA0002", `gender` = "Male", `family_id` = "FM00001", `mother_id` = None, `father_id` = None,
          `calls` = List(0, 0), `mother_calls` = None, `father_calls` = None,
          `affected_status` = false, `mother_affected_status` = None, `father_affected_status` = None,
          `zygosity` = "WT", `parental_origin` = None, `transmission` = "unknown_parents_genotype"), // Father
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis1Id, `service_request_id` = "SRS0003",
          `aliquot_id` = "33333", `patient_id` = "PA0003", `gender` = "Female", `family_id` = "FM00001", `mother_id` = None, `father_id` = None,
          `calls` = List(0, 1), `mother_calls` = None, `father_calls` = None,
          `affected_status` = true, `mother_affected_status` = None, `father_affected_status` = None,
          `zygosity` = "HET", `parental_origin` = Some("unknown"), `transmission` = "unknown_parents_genotype"), // Mother

        // Analysis 2
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0004",
          `aliquot_id` = "44444", `patient_id` = "PA0004", `gender` = "Male", `family_id` = "FM00002", `mother_id` = Some("PA0006"), `father_id` = Some("PA0005"),
          `calls` = List(1, 1), `mother_calls` = Some(List(1, 1)), `father_calls` = Some(List(0, 1)),
          `affected_status` = true, `mother_affected_status` = Some(true), `father_affected_status` = Some(false),
          `zygosity` = "HOM", `parental_origin` = Some("both"), `transmission` = "autosomal_recessive"), // Proband
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0005",
          `aliquot_id` = "55555", `patient_id` = "PA0005", `gender` = "Male", `family_id` = "FM00002", `mother_id` = None, `father_id` = None,
          `calls` = List(0, 1), `mother_calls` = None, `father_calls` = None,
          `affected_status` = false, `mother_affected_status` = None, `father_affected_status` = None,
          `zygosity` = "HET", `parental_origin` = Some("unknown"), `transmission` = "unknown_parents_genotype"), // Father
        SVClusteringParentalOrigin(`batch_id` = batchId, `analysis_service_request_id` = analysis2Id, `service_request_id` = "SRS0006",
          `aliquot_id` = "66666", `patient_id` = "PA0006", `gender` = "Female", `family_id` = "FM00002", `mother_id` = None, `father_id` = None,
          `calls` = List(1, 1), `mother_calls` = None, `father_calls` = None,
          `affected_status` = true, `mother_affected_status` = None, `father_affected_status` = None,
          `zygosity` = "HOM", `parental_origin` = Some("unknown"), `transmission` = "unknown_parents_genotype"), // Mother
      )
    }
  }

  it should "not fail when there are no svclustering parental origin results for a batch" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)
      LoadResolver
        .write(spark, updatedConf)(enriched_clinical.format, LoadType.OverWrite)
        .apply(enriched_clinical, clinicalDf)

      val job = NormalizeSVClusteringParentalOrigin(TestETLContext(default_load)(updatedConf, spark), batchId)
      noException should be thrownBy job.run()
    }
  }
}
