package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow.SVClusteringInput
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class PrepareSVClusteringSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val destination: DatasetConf = conf.getDataset("nextflow_svclustering_input")

  it should "prepare input file for svclustering nextflow pipeline" in {
    val inputData: Map[String, DataFrame] = Map(
      enriched_clinical.id -> Seq(
        // Trio
        EnrichedClinical(aliquot_id = "1", is_proband = true, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = Some("2"), mother_aliquot_id = Some("3"), cnv_vcf_urls = Some(Set("s3a://1.vcf"))),
        EnrichedClinical(aliquot_id = "2", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://2.vcf"))),
        EnrichedClinical(aliquot_id = "3", is_proband = false, batch_id = "BAT1", analysis_id = "SRA1", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://3.vcf"))),

        // Solo
        EnrichedClinical(aliquot_id = "11", is_proband = true, batch_id = "BAT2", analysis_id = "SRA2", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = Some(Set("s3a://11.vcf"))),

        // Duo with no CNVs: Should not be included
        EnrichedClinical(aliquot_id = "111", is_proband = true, batch_id = "BAT3", analysis_id = "SRA3", father_aliquot_id = None, mother_aliquot_id = Some("222"), cnv_vcf_urls = None),
        EnrichedClinical(aliquot_id = "222", is_proband = false, batch_id = "BAT3", analysis_id = "SRA3", father_aliquot_id = None, mother_aliquot_id = None, cnv_vcf_urls = None),
      ).toDF()
    )

    val job = PrepareSVClustering(TestETLContext())

    val result = job.transformSingle(inputData)

    result
      .as[SVClusteringInput]
      .collect() should contain theSameElementsAs Seq(
      // Trio
      SVClusteringInput(sample = "1", familyId = "SRA1", vcf = "s3://1.vcf"),
      SVClusteringInput(sample = "2", familyId = "SRA1", vcf = "s3://2.vcf"),
      SVClusteringInput(sample = "3", familyId = "SRA1", vcf = "s3://3.vcf"),

      // Solo
      SVClusteringInput(sample = "11", familyId = "SRA2", vcf = "s3://11.vcf"),
    )
  }
}
