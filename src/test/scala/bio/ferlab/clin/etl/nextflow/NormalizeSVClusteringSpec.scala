package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.{GENOTYPES_CNV_SVCLUSTERING, VCF_CNV_SVClustering}
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow.{FREQUENCY_RQDM, SVClustering}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class NormalizeSVClusteringSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("nextflow_svclustering_output")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering")

  val job = NormalizeSVClustering(TestETLContext())

  val testData: Map[String, DataFrame] = Map(
    enriched_clinical.id -> Seq(EnrichedClinical()).toDF(),
    raw_variant_calling.id -> Seq(
      VCF_CNV_SVClustering(
        `names` = Seq("DRAGEN:LOSS:chr1:9823628-9823687"),
        `genotypes` = List(
          // Trio
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "1", `calls` = List(0, 1)), // proband
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "2", `calls` = List(0, 0)), // father
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "3", `calls` = List(0, 1)), // mother
          // Duo
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "4", `calls` = List(0, 1)), // proband
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "5", `calls` = List(1, 1)), // mother
        )
      ),
      VCF_CNV_SVClustering(
        `names` = Seq("DRAGEN:DUP:chr1:9823628-9823687"),
        `genotypes` = List(
          // Duo
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "4", `calls` = List(1, 1)), // proband
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "5", `calls` = List(1, 1)), // mother
        )
      )
    ).toDF()
  )

  it should "compute frequencies from svclustering results" in {
    val result = job.transformSingle(testData)

    result
      .as[SVClustering]
      .collect() should contain theSameElementsAs Seq(
      SVClustering(`name` = "DRAGEN:LOSS:chr1:9823628-9823687", `frequency_RQDM` = FREQUENCY_RQDM(pn = 5, pc = 4, pf = 0.8)),
      SVClustering(`name` = "DRAGEN:DUP:chr1:9823628-9823687", `frequency_RQDM` = FREQUENCY_RQDM(pn = 5, pc = 2, pf = 0.4)),
    )
  }
}
