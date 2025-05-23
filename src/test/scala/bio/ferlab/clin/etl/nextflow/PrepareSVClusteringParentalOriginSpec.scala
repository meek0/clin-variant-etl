package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow.SVClusteringInput
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.commons.file.{File, FileSystemResolver}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class PrepareSVClusteringParentalOriginSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val destination: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin_input")

  it should "not load dataset if no analyses in batch have cnv files" in {
    val inputData: Map[String, DataFrame] = Map(
      enriched_clinical.id -> Seq(
        EnrichedClinical(aliquot_id = "1", is_proband = true, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = Some("2"), cnv_vcf_urls = None),
        EnrichedClinical(aliquot_id = "2", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = None),
      ).toDF()
    )

    val batchId = "BAT1"
    val job = PrepareSVClusteringParentalOrigin(TestETLContext(), batchId)

    val transformedData = job.transform(inputData)
    transformedData(destination.id) shouldBe empty

    job.load(transformedData)
    val fs = FileSystemResolver.resolve(conf.getStorage(destination.storageid).filesystem)
    fs.exists(destination.replacePath("{{BATCH_ID}}", batchId).location) shouldBe false
  }

  "transform" should "prepare batch analyses for svclustering-parental-origin nextflow pipeline" in {
    val inputData: Map[String, DataFrame] = Map(
      enriched_clinical.id -> Seq(
        // Batch 1
        // Trio
        EnrichedClinical(aliquot_id = "1", is_proband = true, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = Some("2"), mother_aliquot_id = Some("3"), cnv_vcf_urls = Some(Set("s3a://1.vcf"))),
        EnrichedClinical(aliquot_id = "2", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://2.vcf"))),
        EnrichedClinical(aliquot_id = "3", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://3.vcf"))),

        // Trio+ (with siblings)
        EnrichedClinical(aliquot_id = "11", is_proband = true, batch_id = "BAT1", analysis_id = "SRA2", father_aliquot_id = Some("22"), mother_aliquot_id = Some("33"), cnv_vcf_urls = Some(Set("s3a://11.vcf"))),
        EnrichedClinical(aliquot_id = "22", is_proband = false, batch_id = "BAT1", analysis_id = "SRA2", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://22.vcf"))),
        EnrichedClinical(aliquot_id = "33", is_proband = false, batch_id = "BAT1", analysis_id = "SRA2", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://33.vcf"))),
        EnrichedClinical(aliquot_id = "44", is_proband = false, batch_id = "BAT1", analysis_id = "SRA2", father_aliquot_id = Some("22"), mother_aliquot_id = Some("33"), cnv_vcf_urls = Some(Set("s3a://44.vcf"))),
        EnrichedClinical(aliquot_id = "55", is_proband = false, batch_id = "BAT1", analysis_id = "SRA2", father_aliquot_id = Some("22"), mother_aliquot_id = Some("33"), cnv_vcf_urls = Some(Set("s3a://55.vcf"))),

        // Duo
        EnrichedClinical(aliquot_id = "111", is_proband = true, batch_id = "BAT1", analysis_id = "SRA3", father_aliquot_id = None, mother_aliquot_id = Some("222"), cnv_vcf_urls = Some(Set("s3a://111.vcf"))),
        EnrichedClinical(aliquot_id = "222", is_proband = false, batch_id = "BAT1", analysis_id = "SRA3", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://222.vcf"))),

        // Solo: Should not be included
        EnrichedClinical(aliquot_id = "1111", is_proband = true, batch_id = "BAT1", analysis_id = "SRA4", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://1111.vcf"))),

        // Duo with no CNVs: Should not be included
        EnrichedClinical(aliquot_id = "11111", is_proband = true, batch_id = "BAT1", analysis_id = "SRA5", father_aliquot_id = None, mother_aliquot_id = Some("22222"), cnv_vcf_urls = None),
        EnrichedClinical(aliquot_id = "22222", is_proband = false, batch_id = "BAT1", analysis_id = "SRA5", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = None),

        // Batch 2: Should not be included
        EnrichedClinical(aliquot_id = "111111", is_proband = true, batch_id = "BAT2", analysis_id = "SRA6", father_aliquot_id = None, mother_aliquot_id = Some("222222"), cnv_vcf_urls = Some(Set("s3a://111111.vcf"))),
        EnrichedClinical(aliquot_id = "222222", is_proband = false, batch_id = "BAT2", analysis_id = "SRA6", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://222222.vcf"))),
      ).toDF()
    )

    val job = PrepareSVClusteringParentalOrigin(TestETLContext(), batchId = "BAT1")

    val result = job.transformSingle(inputData)

    result
      .as[SVClusteringInput]
      .collect() should contain theSameElementsAs Seq(
      // Trio
      SVClusteringInput(sample = "1", familyId = "SRA1", vcf = "s3://1.vcf"),
      SVClusteringInput(sample = "2", familyId = "SRA1", vcf = "s3://2.vcf"),
      SVClusteringInput(sample = "3", familyId = "SRA1", vcf = "s3://3.vcf"),

      // Trio+ (with siblings)
      SVClusteringInput(sample = "11", familyId = "SRA2", vcf = "s3://11.vcf"),
      SVClusteringInput(sample = "22", familyId = "SRA2", vcf = "s3://22.vcf"),
      SVClusteringInput(sample = "33", familyId = "SRA2", vcf = "s3://33.vcf"),
      SVClusteringInput(sample = "44", familyId = "SRA2", vcf = "s3://44.vcf"),
      SVClusteringInput(sample = "55", familyId = "SRA2", vcf = "s3://55.vcf"),

      // Duo
      SVClusteringInput(sample = "111", familyId = "SRA3", vcf = "s3://111.vcf"),
      SVClusteringInput(sample = "222", familyId = "SRA3", vcf = "s3://222.vcf")
    )
  }

  "transform" should "include all samples from analysis" in {
    val inputData: Map[String, DataFrame] = Map(
      enriched_clinical.id -> Seq(
        // Batch 1: Incomplete trio
        EnrichedClinical(aliquot_id = "1", is_proband = true, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = Some("3"), mother_aliquot_id = Some("2"), cnv_vcf_urls = Some(Set("s3a://1.vcf"))),
        EnrichedClinical(aliquot_id = "2", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://2.vcf"))),

        // Batch 2: Father of the trio, received in a later batch
        EnrichedClinical(aliquot_id = "3", is_proband = false, batch_id = "BAT2", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://3.vcf"))),
      ).toDF()
    )

    val job = PrepareSVClusteringParentalOrigin(TestETLContext(), batchId = "BAT2")

    val result = job.transformSingle(inputData)

    result
      .as[SVClusteringInput]
      .collect() should contain theSameElementsAs Seq(
      SVClusteringInput(sample = "1", familyId = "SRA1", vcf = "s3://1.vcf"),
      SVClusteringInput(sample = "2", familyId = "SRA1", vcf = "s3://2.vcf"),
      SVClusteringInput(sample = "3", familyId = "SRA1", vcf = "s3://3.vcf"),
    )
  }

  "loadSingle" should "not overwrite existing files and rename csv" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val rootPath = Paths.get(root)
      val existingBatches = Seq("BAT1", "BAT2")

      existingBatches
        .foreach { b =>
          val ds = destination.replacePath("{{BATCH_ID}}", b)
          val updatedPath = ds.path.substring(1) // Remove leading / from path
          Files.createDirectories(Paths.get(root, updatedPath).toAbsolutePath)
          Files.createFile(rootPath.resolve(s"$updatedPath/$b.csv").toAbsolutePath)
        }

      // New file
      val newBatch = "BAT3"
      val job = PrepareSVClusteringParentalOrigin(TestETLContext()(updatedConf, spark), batchId = newBatch)
      val newFileContent = Seq(
        SVClusteringInput(sample = "1", familyId = "SRA1", vcf = "s3://1.vcf"),
        SVClusteringInput(sample = "2", familyId = "SRA1", vcf = "s3://2.vcf"),
        SVClusteringInput(sample = "3", familyId = "SRA1", vcf = "s3://3.vcf"),
      )

      val result = job.loadSingle(newFileContent.toDF())

      result
        .as[SVClusteringInput]
        .collect() should contain theSameElementsAs newFileContent

      val fs = FileSystemResolver.resolve(updatedConf.getStorage(destination.storageid).filesystem)
      val files = fs.list(s"$root${destination.replacePath("{{BATCH_ID}}", "").path}", recursive = true)

      files
        .collect { case File(_, name, _, isDir) if !isDir => name }
        .should(contain theSameElementsAs Seq("BAT1.csv", "BAT2.csv", "BAT3.csv"))
    }
  }

}
