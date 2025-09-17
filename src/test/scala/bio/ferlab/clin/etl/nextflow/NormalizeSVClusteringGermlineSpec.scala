package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.{GENOTYPES_CNV_SVCLUSTERING, VCF_CNV_SVClustering}
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow._
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class NormalizeSVClusteringGermlineSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val job = NormalizeSVClusteringGermline(TestETLContext(),
    sourceId = "nextflow_svclustering_germline_del_output", destinationId = "nextflow_svclustering_germline_del")

  val testData: Map[String, DataFrame] = Map(
    job.enriched_clinical.id -> Seq(
      EnrichedClinical(`aliquot_id` = "1", `affected_status` = true),
      EnrichedClinical(`aliquot_id` = "2", `affected_status` = true),
      EnrichedClinical(`aliquot_id` = "3", `affected_status` = true),
      EnrichedClinical(`aliquot_id` = "4", `affected_status` = false),
    ).toDF(),
    job.source.id -> Seq(
      // Cluster 1
      VCF_CNV_SVClustering(
        `contigName` = "chr12",
        `start` = 46361100,
        `end` = 46365186,
        `referenceAllele` = "A",
        `alternateAlleles` = Seq("<GAIN>"),
        `names` = Seq("DRAGEN:GAIN:chr12:46361100-46365186"),
        `INFO_MEMBERS` = Seq("DRAGEN:GAIN:chr12:46361100-46364711", "DRAGEN:GAIN:chr12:46361100-46364999", "DRAGEN:GAIN:chr12:46361100-46365186"),
        `genotypes` = Seq(
          // Affected
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "1", `calls` = Seq(1, 1)),
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "2", `calls` = Seq(0, 1)),
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "3", `calls` = Seq(0, 0)), // excluded from pc because (0,0)
          // Non affected
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "4", `calls` = Seq(0, 1)),
        )
      ),
      // Cluster 2
      VCF_CNV_SVClustering(
        `contigName` = "chr15",
        `start` = 43635335,
        `end` = 43636228,
        `referenceAllele` = "A",
        `alternateAlleles` = Seq("<GAIN>"),
        `names` = Seq("DRAGEN:GAIN:chr15:43635335-43636228"),
        `INFO_MEMBERS` = Seq("DRAGEN:GAIN:chr15:43635335-43636228", "DRAGEN:GAIN:chr15:43635335-43636228"),
        `genotypes` = Seq(
          // Affected
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "1", `calls` = Seq(1, 1)),
          // Non affected
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "4", `calls` = Seq(0, 0)), // excluded from pc because (0,0)
        )
      )).toDF()
  )

  it should "compute frequencies from svclustering results" in {
    val result = job.transformSingle(testData)

    result
      .as[SVClusteringGermline]
      .collect() should contain theSameElementsAs Seq(
      // Cluster 1
      SVClusteringGermline(
        `chromosome` = "12",
        `start` = 46361101,
        `end` = 46365187,
        `reference` = "A",
        `alternate` = "<GAIN>",
        `name` = "DRAGEN:GAIN:chr12:46361100-46365186",
        `members` = Seq("DRAGEN:GAIN:chr12:46361100-46364711", "DRAGEN:GAIN:chr12:46361100-46364999", "DRAGEN:GAIN:chr12:46361100-46365186"),
        `aliquot_ids` = Set("1", "2", "3", "4"),
        `frequency_RQDM` = FREQUENCY_RQDM_GERM(
          `germ` = GERM(
            `affected` = AFFECTED(`pc` = 2, `pn` = 4, `pf` = 0.5),
            `non_affected` = NON_AFFECTED(`pc` = 1, `pn` = 4, `pf` = 0.25),
            `total` = TOTAL(`pc` = 3, `pn` = 4, `pf` = 0.75)
          )
        )
      ),
      // Cluster 2
      SVClusteringGermline(
        `chromosome` = "15",
        `start` = 43635336,
        `end` = 43636229,
        `reference` = "A",
        `alternate` = "<GAIN>",
        `name` = "DRAGEN:GAIN:chr15:43635335-43636228",
        `members` = Seq("DRAGEN:GAIN:chr15:43635335-43636228", "DRAGEN:GAIN:chr15:43635335-43636228"),
        `aliquot_ids` = Set("1", "4"),
        `frequency_RQDM` = FREQUENCY_RQDM_GERM(
          `germ` = GERM(
            `affected` = AFFECTED(`pc` = 1, `pn` = 4, `pf` = 0.25),
            `non_affected` = NON_AFFECTED(`pc` = 0, `pn` = 4, `pf` = 0.0),
            `total` = TOTAL(`pc` = 1, `pn` = 4, `pf` = 0.25)
          )
        )
      ),
    )
  }
}
