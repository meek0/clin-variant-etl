package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.fhir.GenomicFile.EXOMISER
import bio.ferlab.clin.etl.utils.FileUtils.fileUrls
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class FileUtilsSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  val clinicalDf = Seq(
    EnrichedClinical(`batch_id` = "B1", `patient_id` = "PA0001", `service_request_id` = "SRS0001", `aliquot_id` = "1", `specimen_id` = "1", `exomiser_urls` = Some(Set("s3a://file1.tsv", "s3a://file2.tsv", "s3a://file2b.tsv")), `covgene_urls` = Some(Set("s3a://file3covgene.csv"))),
    EnrichedClinical(`batch_id` = "B1", `patient_id` = "PA0001", `service_request_id` = "SRS0002", `aliquot_id` = "2", `specimen_id` = "2", `exomiser_urls` = Some(Set("s3a://file3.tsv")), `covgene_urls` = Some(Set("s3a://file4covgene.csv"))),
    EnrichedClinical(`batch_id` = "B2", `patient_id` = "PA0002", `service_request_id` = "SRS0003", `aliquot_id` = "3", `specimen_id` = "3", `exomiser_urls` = Some(Set("s3a://file5.tsv")), `covgene_urls` = None),
  ).toDF()

  override val dbToCreate: List[String] = List(enriched_clinical.table.get.database)
  override val dsToClean: List[DatasetConf] = List(enriched_clinical)

  override def beforeAll(): Unit = {
    super.beforeAll()
    LoadResolver
      .write(spark, conf)(enriched_clinical.format -> enriched_clinical.loadtype)
      .apply(enriched_clinical, clinicalDf)
  }


  "fileUrls" should "return list of expected urls for a given batch id and file type" in {
    val results = fileUrls(batchId = "B1", file = EXOMISER)
    results should contain theSameElementsAs Seq(
      FileInfo(url = "s3a://file1.tsv", aliquot_id = "1", patient_id = "PA0001", specimen_id = "1", service_request_id = "SRS0001"),
      FileInfo(url = "s3a://file2.tsv", aliquot_id = "1", patient_id = "PA0001", specimen_id = "1", service_request_id = "SRS0001"),
      FileInfo(url = "s3a://file2b.tsv", aliquot_id = "1", patient_id = "PA0001", specimen_id = "1", service_request_id = "SRS0001"),
      FileInfo(url = "s3a://file3.tsv", aliquot_id = "2", patient_id = "PA0001", specimen_id = "2", service_request_id = "SRS0002"),
    )

  }

}
