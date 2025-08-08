package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.{GENOTYPES_CNV_SVCLUSTERING, VCF_CNV_SVClustering}
import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.clin.model.nextflow.{FREQUENCY_RQDM_SOM, SOM, SVClusteringSomatic}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class NormalizeSVClusteringSomaticSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val job = NormalizeSVClusteringSomatic(TestETLContext())

  val testData: Map[String, DataFrame] = Map(
    job.enriched_clinical.id -> Seq(
      EnrichedClinical(`aliquot_id` = "1"),
      EnrichedClinical(`aliquot_id` = "2"),
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
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "1", `calls` = Seq(1, 1)),
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "2", `calls` = Seq(0, 0)) // excluded from pc because (0,0)
        )
      ),
      // Cluster 2
      VCF_CNV_SVClustering(
        `contigName` = "chr16",
        `start` = 47430,
        `end` = 98310,
        `referenceAllele` = "A",
        `alternateAlleles` = Seq("<GAIN>"),
        `names` = Seq("DRAGEN:GAIN:chr16:47430-98310"),
        `INFO_MEMBERS` = Seq("DRAGEN:GAIN:chr16:47430-98310"),
        `genotypes` = Seq(
          GENOTYPES_CNV_SVCLUSTERING(`sampleId` = "1", `calls` = Seq(0, 1))
        )
      )
    ).toDF(),
  )

  it should "compute frequencies from svclustering results" in {
    val result = job.transformSingle(testData)

    result
      .as[SVClusteringSomatic]
      .collect() should contain theSameElementsAs Seq(
      // Cluster 1
      SVClusteringSomatic(
        `chromosome` = "12",
        `start` = 46361101,
        `end` = 46365187,
        `reference` = "A",
        `alternate` = "<GAIN>",
        `name` = "DRAGEN:GAIN:chr12:46361100-46365186",
        `members` = Seq("DRAGEN:GAIN:chr12:46361100-46364711", "DRAGEN:GAIN:chr12:46361100-46364999", "DRAGEN:GAIN:chr12:46361100-46365186"),
        `frequency_RQDM` = FREQUENCY_RQDM_SOM(
          `som` = SOM(`pc` = 1, `pn` = 2, `pf` = 0.5),
        )
      ),
      // Cluster 2
      SVClusteringSomatic(
        `chromosome` = "16",
        `start` = 47431,
        `end` = 98311,
        `reference` = "A",
        `alternate` = "<GAIN>",
        `name` = "DRAGEN:GAIN:chr16:47430-98310",
        `members` = Seq("DRAGEN:GAIN:chr16:47430-98310"),
        `frequency_RQDM` = FREQUENCY_RQDM_SOM(
          `som` = SOM(`pc` = 1, `pn` = 2, `pf` = 0.5),
        )
      )
    )
  }
}
