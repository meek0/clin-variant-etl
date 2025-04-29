package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.{EnrichedCNV, EnrichedCNVCluster, EnrichedCNVClusterFrequencies, EnrichedCNVClusterFrequenciesGnomadV4, EnrichedClinical}
import bio.ferlab.clin.model.nextflow.{SVClustering, SVClusteringParentalOrigin}
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedCNVSomaticTumorOnly, NormalizedPanels, NormalizedRefSeq}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadV4CNV
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}

class CNVSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("enriched_cnv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val normalized_refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val nextflow_svclustering: DatasetConf = conf.getDataset("nextflow_svclustering")
  val nextflow_svclustering_parental_origin: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin")
  val normalized_gnomad_cnv_v4: DatasetConf = conf.getDataset("normalized_gnomad_cnv_v4")

  val job = CNV(TestETLContext(), None)

  val testData = Map(
    normalized_cnv.id -> Seq(
      NormalizedCNV(`batch_id` = "BAT1"),
      NormalizedCNV(`batch_id` = "BAT2")
    ).toDF(),
    normalized_cnv_somatic_tumor_only.id -> Seq(
      NormalizedCNVSomaticTumorOnly(`batch_id` = "BAT1"),
      NormalizedCNVSomaticTumorOnly(`batch_id` = "BAT2")
    ).toDF(),
    nextflow_svclustering.id -> Seq(SVClustering()).toDF(),
    // This table is partitioned by analysis_service_request_id
    nextflow_svclustering_parental_origin.id -> Seq(
      SVClusteringParentalOrigin(`batch_id` = "BAT1", `analysis_service_request_id` = "SRA0001", `service_request_id` = "SRS0001"),
      SVClusteringParentalOrigin(`batch_id` = "BAT2", `analysis_service_request_id` = "SRA0002", `service_request_id` = "SRS0002")
    ).toDF(),
    normalized_refseq_annotation.id -> Seq(NormalizedRefSeq()).toDF(),
    normalized_panels.id -> Seq(NormalizedPanels()).toDF(),
    genes.id -> Seq(EnrichedGenes()).toDF(),
    enriched_clinical.id -> Seq(
      EnrichedClinical(`batch_id` = "BAT1", `analysis_service_request_id` = "SRA0001"),
      EnrichedClinical(`batch_id` = "BAT2", `analysis_service_request_id` = "SRA0002")
    ).toDF(),
    normalized_gnomad_cnv_v4.id -> Seq(
      NormalizedGnomadV4CNV(),
    ).toDF()
  )

  override val dsToClean: List[DatasetConf] = List(destination, normalized_cnv, normalized_cnv_somatic_tumor_only,
    normalized_refseq_annotation, normalized_panels, genes, enriched_clinical, nextflow_svclustering,
    nextflow_svclustering_parental_origin, normalized_gnomad_cnv_v4)

  "transform" should "enrich CNV data" in {
    val data = testData ++ Map(
      normalized_cnv.id -> Seq(NormalizedCNV(`service_request_id` = "SRS0001", `aliquot_id` = "11111")).toDF(),
      normalized_cnv_somatic_tumor_only.id -> Seq(NormalizedCNVSomaticTumorOnly(`service_request_id` = "SRS0002", `aliquot_id` = "22222")).toDF(),
    )

    val result = job.transformSingle(data)

    result
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`service_request_id` = "SRS0001", `aliquot_id` = "11111", `hash` = "65af80e7610e804b2d5d01c32ed39d9f27c9f8d5"),
      EnrichedCNV(`service_request_id` = "SRS0002", `aliquot_id` = "22222", `variant_type` = "somatic", `cn` = None, `hash` = "05c1575c45d71352d7f88c8a688956b139653661"),
    )
  }

  "transform" should "enrich CNV data with overlapping gnomad v4 exomes" in {
    val data = testData ++ Map(
      normalized_cnv.id -> Seq(
        NormalizedCNV(),
        NormalizedCNV(`chromosome` = "5", `start` = 100, `end` = 230, `reference` = "N", `alternate` = "DEL"), // overlap 70% with variant_is_100_200_5__DEL and 100% with variant_is_100_230_5__DEL
        NormalizedCNV(`chromosome` = "5", `start` = 101, `end` = 200, `reference` = "N", `alternate` = "DEL"), // overlap 99%
        NormalizedCNV(`chromosome` = "5", `start` = 102, `end` = 182, `reference` = "N", `alternate` = "DEL"), // overlap 80%
        NormalizedCNV(`chromosome` = "5", `start` = 103, `end` = 153, `reference` = "N", `alternate` = "DEL"), // overlap 50%
      ).toDF(),
      normalized_cnv_somatic_tumor_only.id -> Seq[NormalizedCNVSomaticTumorOnly]().toDF(),
      normalized_gnomad_cnv_v4.id -> Seq(
        NormalizedGnomadV4CNV(`chromosome` = "5", `start` = 100, `end` = 200, `reference` = "N", `alternate` = "DEL", `name` = "variant_is_100_200_5__DEL", `sc` = 6.0, `sn` = 0.15, `sf` = 0.001),
        NormalizedGnomadV4CNV(`chromosome` = "5", `start` = 100, `end` = 230, `reference` = "N", `alternate` = "DEL", `name` = "variant_is_100_230_5__DEL", `sc` = 7.0, `sn` = 0.16, `sf` = 0.002),
      ).toDF(),
    )

    val result = job.transformSingle(data)

    result
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`chromosome` = "5", `start` = 100, `end` = 230, `reference` = "N", `alternate` = "DEL",
        `number_genes` = 0, `genes` = List(),
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = "variant_is_100_230_5__DEL",
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 7.0, `sn` = 0.16, `sf` = 0.002)
          )
        ), `hash` = "06d5708f770eeb37bab6ddef28ad60dda7e0a908"),
      EnrichedCNV(`chromosome` = "5", `start` = 102, `end` = 182, `reference` = "N", `alternate` = "DEL",
        `number_genes` = 0, `genes` = List(),
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = "variant_is_100_200_5__DEL",
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 6.0, `sn` = 0.15, `sf` = 0.001)
          )
        ), `hash` = "06d5708f770eeb37bab6ddef28ad60dda7e0a908"),
      EnrichedCNV(`chromosome` = "5", `start` = 101, `end` = 200, `reference` = "N", `alternate` = "DEL",
        `number_genes` = 0, `genes` = List(),
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = "variant_is_100_200_5__DEL",
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 6.0, `sn` = 0.15, `sf` = 0.001)
          )
        ), `hash` = "06d5708f770eeb37bab6ddef28ad60dda7e0a908"),
      EnrichedCNV(),
      EnrichedCNV(`chromosome` = "5", `start` = 103, `end` = 153, `reference` = "N", `alternate` = "DEL",
        `number_genes` = 0, `genes` = List(),
        `hash` = "06d5708f770eeb37bab6ddef28ad60dda7e0a908"),
    )
  }

  "transform" should "set number_genes = 0 if there are no genes for a CNV" in {
    val noGeneData = testData + (normalized_refseq_annotation.id -> Seq(NormalizedRefSeq(`chromosome` = "42")).toDF())

    val result = job.transformSingle(noGeneData)

    result
      .select("number_genes")
      .as[Int]
      .collect() should contain only 0
  }

  "extract" should "return only the CNVs from the batch" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      testData.foreach { case (id, df) =>
        val ds = updatedConf.getDataset(id)

        LoadResolver
          .write(spark, updatedConf)(ds.format, ds.loadtype)
          .apply(ds, df)
      }

      val batchId = "BAT1"
      val job = CNV(TestETLContext()(updatedConf, spark), batchId = Some(batchId))
      val result = job.extract()

      val filteredDatasets = List(normalized_cnv, normalized_cnv_somatic_tumor_only, nextflow_svclustering_parental_origin)
      filteredDatasets.foreach { ds =>
        result(ds.id)
          .select("batch_id")
          .as[String]
          .collect() should contain only batchId
      }
    }
  }

  "extract" should "return all CNVs if no batch id is submitted" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      testData.foreach { case (id, df) =>
        val ds = updatedConf.getDataset(id)

        LoadResolver
          .write(spark, updatedConf)(ds.format, ds.loadtype)
          .apply(ds, df)
      }

      val job = CNV(TestETLContext()(updatedConf, spark), batchId = None)
      val result = job.extract()

      val filteredDatasets = List(normalized_cnv, normalized_cnv_somatic_tumor_only, nextflow_svclustering_parental_origin)
      filteredDatasets.foreach { ds =>
        result(ds.id)
          .select("batch_id")
          .as[String]
          .collect() should contain allElementsOf Seq("BAT1", "BAT2")
      }
    }
  }
}

