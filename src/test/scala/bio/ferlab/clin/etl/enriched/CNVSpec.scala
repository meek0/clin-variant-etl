package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched._
import bio.ferlab.clin.model.nextflow.{SVClustering, SVClusteringParentalOrigin}
import bio.ferlab.clin.model.normalized._
import bio.ferlab.clin.testutils.{LoadResolverUtils, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadV4CNV
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}

class CNVSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("enriched_cnv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val nextflow_svclustering: DatasetConf = conf.getDataset("nextflow_svclustering")
  val nextflow_svclustering_parental_origin: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin")
  val normalized_gnomad_cnv_v4: DatasetConf = conf.getDataset("normalized_gnomad_cnv_v4")

  override val dbToCreate: List[String] = List("clin")
  override val dsToClean: List[DatasetConf] = List(destination, normalized_cnv, normalized_cnv_somatic_tumor_only, normalized_snv,
    normalized_snv_somatic, normalized_refseq_annotation, normalized_panels, genes, enriched_clinical, nextflow_svclustering,
    nextflow_svclustering_parental_origin, normalized_gnomad_cnv_v4)

  val job = CNV(TestETLContext(), None)


  /*
    Test data overview:

    The test data covers both germline and somatic (tumor only) CNV analyses across 5 batches:

      - Batch1: 1 germline analysis, with matching germline SNV data, with parental origin data
      - Batch2: 1 germline analysis, without matching germline SNV data, with parental origin data
      - Batch3: 1 somatic analysis (TEBA), with both tumor only and tumor normal data
      - Batch3TN: (TNEBA) contains the snv somatic tumor normal data associated to the analysis in batch3
      - Batch4:  1 somatic analysis without matching somatic SNV data

    The SNV data (both germline and somatic) is used to compute the `snv_count` column for each CNV.

    This setup allows us to test:
      - Germline vs. somatic (tumor only) CNV handling.
      - Scenarios with and without corresponding SNV data for each CNV.
      - Scenarios for which an analysis exists as both "tumor only" and "tumor normal, ensuring that only the "tumor only" SNV data is used for 
        snv_count calculation, as somatic CNV data is available exclusively for "tumor only."
      - Parental origin metrics
  */
  val testData = Map(
    enriched_clinical.id -> Seq(
      EnrichedClinical(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001", `bioinfo_analysis_code` = "GEBA"),
      EnrichedClinical(`batch_id` = "BAT2", `analysis_id` = "SRA0002", `sequencing_id` = "SRS0002", `bioinfo_analysis_code` = "GEBA"),
      EnrichedClinical(`batch_id` = "BAT3", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003", `bioinfo_analysis_code` = "TEBA"),
      EnrichedClinical(`batch_id` = "BAT3TN", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003", `bioinfo_analysis_code` = "TNEBA"),
      EnrichedClinical(`batch_id` = "BAT4", `analysis_id` = "SRA0004", `sequencing_id` = "SRS0004", `bioinfo_analysis_code` = "TEBA")
    ).toDF(),

    normalized_cnv.id -> Seq(
      NormalizedCNV(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001"),
      NormalizedCNV(`batch_id` = "BAT2", `analysis_id` = "SRA0002", `sequencing_id` = "SRS0002")
    ).toDF(),
    normalized_cnv_somatic_tumor_only.id -> Seq(
      NormalizedCNVSomaticTumorOnly(`batch_id` = "BAT3", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003"),
      NormalizedCNVSomaticTumorOnly(`batch_id` = "BAT4", `analysis_id` = "SRA0004", `sequencing_id` = "SRS0004")
    ).toDF(),

    normalized_snv.id -> Seq(
      NormalizedSNV(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001", `bioinfo_analysis_code` = "GEBA")
    ).toDF(),

    normalized_snv_somatic.id -> Seq(
      NormalizedSNVSomatic(`batch_id` = "BAT3", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003", `bioinfo_analysis_code` = "TEBA"),
      NormalizedSNVSomatic(`batch_id` = "BAT3TN", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003", `bioinfo_analysis_code` = "TNEBA")
    ).toDF(),

    nextflow_svclustering.id -> Seq(SVClustering()).toDF(),
    // This table is partitioned by analysis_id
    nextflow_svclustering_parental_origin.id -> Seq(
      SVClusteringParentalOrigin(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001"),
      SVClusteringParentalOrigin(`batch_id` = "BAT2", `analysis_id` = "SRA0002", `sequencing_id` = "SRS0002")
    ).toDF(),

    normalized_refseq_annotation.id -> Seq(NormalizedRefSeq()).toDF(),
    normalized_panels.id -> Seq(NormalizedPanels()).toDF(),
    genes.id -> Seq(EnrichedGenes()).toDF(),
    normalized_gnomad_cnv_v4.id -> Seq(
      NormalizedGnomadV4CNV(),
    ).toDF()
  )



  //scnearios: extract: data to include in the test ... 
  // extract: 

  "extract" should "select data relevant to analysis present in the batch - germinal" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // write the test data
      testData.foreach { case (id, df) =>
        val ds = updatedConf.getDataset(id)
        LoadResolverUtils.write(ds, df)(spark, updatedConf)
      }

      // using batch 1, which has one germline analysis with both cnv and snv data, as well as parental origin data
      val batchId = "BAT1"
      val job = CNV(TestETLContext()(updatedConf, spark), batchId = Some(batchId))
      val result = job.extract()


      val nonEmptyDatasets = List(normalized_cnv, normalized_snv, nextflow_svclustering_parental_origin)
      nonEmptyDatasets.foreach { ds =>
        result(ds.id)
          .select("batch_id")
          .as[String]
          .collect() should contain only batchId
      }

      val emptyDatasets = List(normalized_cnv_somatic_tumor_only, normalized_snv_somatic)
      emptyDatasets.foreach { ds =>
        result(ds.id)
          .collect() shouldBe empty
      }
    }
  }

  it should "select data relevant to analysis present in the batch - somatic tumor only" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      // write the test data
      testData.foreach { case (id, df) =>
        val ds = updatedConf.getDataset(id)
        LoadResolverUtils.write(ds, df)(spark, updatedConf)
      }

      // using batch 3, which has one somatic tumor only analysis with both cnv and snv data
      val batchId = "BAT3"
      val job = CNV(TestETLContext()(updatedConf, spark), batchId = Some(batchId))
      val result = job.extract()

      // Only normalized_snv_somatic data from the current batch should be included.
      // Data from other tumor-only batches with the same analysis_id must be excluded.
      val nonEmptyDatasets = List(normalized_cnv_somatic_tumor_only, normalized_snv_somatic)
      nonEmptyDatasets.foreach { ds =>
        result(ds.id)
          .select("batch_id")
          .as[String]
          .collect() should contain only batchId
      }

      // there should be no germline data extracted for this batch
      val emptyDatasets = List(normalized_cnv, normalized_snv, nextflow_svclustering_parental_origin)
      emptyDatasets.foreach { ds =>
        result(ds.id)
          .collect() shouldBe empty
      }
    }
  }


  it should "return data from all batches if no id is submitted" in {
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

      //val germlineDatasets = List(normalized_cnv, normalized_snv, nextflow_svclustering_parental_origin)
      // Should contain data from both germline analyses (BAT1, BAT2)

      // should return all normalize cnv data
      result(normalized_cnv.id)
        .as[NormalizedCNV]
        .collect() should contain allElementsOf
        testData(normalized_cnv.id).as[NormalizedCNV].collect()


      // should return all normalized snv data
      result(normalized_snv.id)
        .as[NormalizedSNV]
        .collect() should contain allElementsOf
        testData(normalized_snv.id).as[NormalizedSNV].collect()

      // should return all parental origin data
      result(nextflow_svclustering_parental_origin.id)
        .as[SVClusteringParentalOrigin]
        .collect() should contain allElementsOf
        testData(nextflow_svclustering_parental_origin.id).as[SVClusteringParentalOrigin].collect()

      // should return all normalized somatic tumor only cnv data
      result(normalized_cnv_somatic_tumor_only.id)
        .as[NormalizedCNVSomaticTumorOnly]
        .collect() should contain allElementsOf
        testData(normalized_cnv_somatic_tumor_only.id).as[NormalizedCNVSomaticTumorOnly].collect()

      // should return only normalized somatic tumor only snv data
      result(normalized_snv_somatic.id)
        .as[NormalizedSNVSomatic]
        .collect() should contain allElementsOf Seq(
        NormalizedSNVSomatic(`batch_id` = "BAT3", `analysis_id` = "SRA0003", `sequencing_id` = "SRS0003", `bioinfo_analysis_code` = "TEBA"),
      )
    }
  }


  "transform" should "enrich CNV data" in {
    val data = testData ++ Map(
      normalized_cnv.id -> Seq(NormalizedCNV(`sequencing_id` = "SRS0001", `aliquot_id` = "11111")).toDF(),
      normalized_cnv_somatic_tumor_only.id -> Seq(NormalizedCNVSomaticTumorOnly(`sequencing_id` = "SRS0002", `aliquot_id` = "22222")).toDF(),
    )

    val result = job.transformSingle(data)

    result
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`sequencing_id` = "SRS0001", `aliquot_id` = "11111", `hash` = "65af80e7610e804b2d5d01c32ed39d9f27c9f8d5"),
      EnrichedCNV(`sequencing_id` = "SRS0002", `aliquot_id` = "22222", `variant_type` = "somatic", `cn` = None, `hash` = "05c1575c45d71352d7f88c8a688956b139653661"),
    )
  }

  it should "enrich CNV data with overlapping gnomad v4 exomes" in {
    val data = testData ++ Map(
      normalized_cnv_somatic_tumor_only.id -> Seq[NormalizedCNVSomaticTumorOnly]().toDF(), // empty somatics for test simplicity
      normalized_cnv.id -> Seq(
        // match CLUSTER_1_100_200
        NormalizedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01"),
        // match CLUSTER_2_100_200
        NormalizedCNV(`chromosome` = "2", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_02"),
        // match CLUSTER_3_100_200
        NormalizedCNV(`chromosome` = "3", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_03"),
        // match no cluster
        NormalizedCNV(`chromosome` = "4", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_04"),
        // match CLUSTER_5_100_200
        NormalizedCNV(`chromosome` = "5", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_05"),
        // match CLUSTER_6_100_200
        NormalizedCNV(`chromosome` = "6", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_06"),
      ).toDF(),
      nextflow_svclustering.id -> Seq(
        // has 100% overlap with GNOMAD_01
        SVClustering(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `members` = Seq("CNV_01"), `name` = "CLUSTER_1_100_200"),
        // has 80% overlap with GNOMAD_02
        SVClustering(`chromosome` = "2", `start` = 100, `end` = 180, `alternate` = "A", reference = "REF", `members` = Seq("CNV_02"), `name` = "CLUSTER_2_100_200"),
        // has 50% overlap with GNOMAD_03
        SVClustering(`chromosome` = "3", `start` = 150, `end` = 200, `alternate` = "A", reference = "REF", `members` = Seq("CNV_03"), `name` = "CLUSTER_3_100_200"),
        // has 80% overlap with both GNOMAD_05_01 and GNOMAD_05_02
        SVClustering(`chromosome` = "5", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `members` = Seq("CNV_05"), `name` = "CLUSTER_5_100_200"),
        // doesn't overlap with GNOMAD_06
        SVClustering(`chromosome` = "6", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `members` = Seq("CNV_06"), `name` = "CLUSTER_6_100_200"),
      ).toDF,
      normalized_gnomad_cnv_v4.id -> Seq(
        NormalizedGnomadV4CNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.002, `name` = "GNOMAD_01"),
        NormalizedGnomadV4CNV(`chromosome` = "2", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.002, `name` = "GNOMAD_02"),
        NormalizedGnomadV4CNV(`chromosome` = "3", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.002, `name` = "GNOMAD_03"),
        NormalizedGnomadV4CNV(`chromosome` = "5", `start` = 100, `end` = 180, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.002, `name` = "GNOMAD_05_01"),
        NormalizedGnomadV4CNV(`chromosome` = "5", `start` = 120, `end` = 200, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.003, `name` = "GNOMAD_05_02"),
        NormalizedGnomadV4CNV(`chromosome` = "6", `start` = 200, `end` = 300, `alternate` = "A", reference = "REF", `sc` = 7.0, `sn` = 0.16, `sf` = 0.003, `name` = "GNOMAD_06"),
      ).toDF(),
    )

    job.transformSingle(data)
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_01",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = Some("CLUSTER_1_100_200"),
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = Some(EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 7.0, `sn` = 0.16, `sf` = 0.002))
          )
        ), `hash` = "d770393e8488e9abd9380b5ff08e44e8689a82a5"),
      EnrichedCNV(`chromosome` = "2", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_02",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = Some("CLUSTER_2_100_200"),
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = Some(EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 7.0, `sn` = 0.16, `sf` = 0.002))
          )
        ), `hash` = "a099572eaa03cd35dcbfe01be45bd2e036b9d21e"),
      EnrichedCNV(`chromosome` = "3", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_03",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set(),
        `cluster` = EnrichedCNVCluster(
          `id` = Some("CLUSTER_3_100_200"),
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = None
          )
        ), `hash` = "f43e1ff5313885d668443e05791f03ec1c9231b8"),
      EnrichedCNV(`chromosome` = "4", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_04",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set(),
        `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(
          `id` = None,
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = None
          )
        ), `hash` = "87779ec18f24a04f353cc13d7c2930406817c735"),
      EnrichedCNV(`chromosome` = "5", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_05",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set("gnomAD"),
        `cluster` = EnrichedCNVCluster(
          `id` = Some("CLUSTER_5_100_200"),
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = Some(EnrichedCNVClusterFrequenciesGnomadV4(`sc` = 7.0, `sn` = 0.16, `sf` = 0.003))
          )
        ), `hash` = "cc6e13e9aad1772f5ab02aef08f1ffaeb0294272"),
      EnrichedCNV(`chromosome` = "6", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_06",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null,
        `variant_external_reference` = Set(),
        `cluster` = EnrichedCNVCluster(
          `id` = Some("CLUSTER_6_100_200"),
          `external_frequencies` = EnrichedCNVClusterFrequencies(
            `gnomad_exomes_4` = None
          )
        ), `hash` = "5f801a8352117ff5b6b204ba6ee78427ec5acdbd"),
    )
  }


  it should "enrich CNV data with snv count" in {
    // This test includes only germline data to focus on the overlap logic for snv count calcultation
    // The code assume that an SNV overlap a CNV if the SNV's start is within the CNV's interval.
    val data = testData ++ Map(
      normalized_cnv_somatic_tumor_only.id -> Seq[NormalizedCNVSomaticTumorOnly]().toDF(), // empty somatics for test simplicity
      normalized_cnv.id -> Seq(
        // includes no SNV
        NormalizedCNV(`chromosome` = "1", `start` = 1, `end` = 100, `alternate` = "A", reference = "REF", `name` = "CNV_00", `sequencing_id` = "SR_000"),
        // includes 3 SNV
        NormalizedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01", `sequencing_id` = "SR_001"),
        // includes 1 SNV
        NormalizedCNV(`chromosome` = "1", `start` = 200, `end` = 250, `alternate` = "A", reference = "REF", `name` = "CNV_02", `sequencing_id` = "SR_001"),
        // includes 1 SNV from another service request
        NormalizedCNV(`chromosome` = "1", `start` = 200, `end` = 250, `alternate` = "A", reference = "REF", `name` = "CNV_02", `sequencing_id` = "SR_002", `aliquot_id` = "11112"),
      ).toDF(),
      normalized_snv.id -> Seq(
        NormalizedSNV(`chromosome` = "1", `start` = 105, `end` = 195, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_01", `sequencing_id` = "SR_001"),
        NormalizedSNV(`chromosome` = "1", `start` = 140, `end` = 200, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_02", `sequencing_id` = "SR_001"),
        NormalizedSNV(`chromosome` = "1", `start` = 100, `end` = 110, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_03", `sequencing_id` = "SR_001"),
        NormalizedSNV(`chromosome` = "1", `start` = 210, `end` = 250, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_04", `sequencing_id` = "SR_001"),
        NormalizedSNV(`chromosome` = "1", `start` = 210, `end` = 250, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_04", `sequencing_id` = "SR_002"),
      ).toDF(),
    )

    job.transformSingle(data)
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`chromosome` = "1", `start` = 1, `end` = 100, `alternate` = "A", `reference` = "REF", `name` = "CNV_00", `snv_count` = 0, `sequencing_id` = "SR_000",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "54983b0279495b4e60366f31a3c352da5acc8281",
      ),
      EnrichedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_01", `snv_count` = 3, `sequencing_id` = "SR_001",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "256519903d044ebbb123d95989175e04d8c82dd2",
      ),
      EnrichedCNV(`chromosome` = "1", `start` = 200, `end` = 250, `alternate` = "A", `reference` = "REF", `name` = "CNV_02", `snv_count` = 1, `sequencing_id` = "SR_001",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "fb94d57b79ff40168cc7cb7b8cf5607eb6cf47e4",
      ),
      EnrichedCNV(`chromosome` = "1", `start` = 200, `end` = 250, `alternate` = "A", `reference` = "REF", `name` = "CNV_02", `snv_count` = 1, `sequencing_id` = "SR_002",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "37b02ad5d5fb4fcc3c783ebcdecff26480a44ef3", `aliquot_id` = "11112",
      )
    )
  }

  it should "compute snv count from the right data source" in {
    // Check that germline CNV data is enriched only with germline SNV data,
    // and that somatic tumor-only CNV data is enriched only with somatic tumor-only SNV data.
    // Reminder: a SNV is considered to overlap a CNV if the SNV's start position is within the CNV's interval.
    //
    // In this test, we have one germline CNV and one somatic CNV. For each, there are two overlapping SNVs—
    // one germline and one somatic. We expect the germline CNV to match only the germline SNV,
    // and the somatic CNV (tumor only) to match only the somatic SNV (tumor only).
    val data = testData ++ Map(
      normalized_cnv.id -> Seq(
        NormalizedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01", `sequencing_id` = "SR_001"),
      ).toDF(),

      normalized_cnv_somatic_tumor_only.id -> Seq(
        NormalizedCNVSomaticTumorOnly(`chromosome` = "1", `start` = 300, `end` = 400, `alternate` = "A", reference = "REF", `name` = "CNV_SOMATIC_01", `sequencing_id` = "SR_002", `bioinfo_analysis_code` = "TEBA")
      ).toDF(),

      normalized_snv.id -> Seq(
        NormalizedSNV(`chromosome` = "1", `start` = 150, `end` = 160, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_01", `sequencing_id` = "SR_001"),

        // should not be counted for the somatic cnv as it is germline
        NormalizedSNV(`chromosome` = "1", `start` = 350, `end` = 360, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_02", `sequencing_id` = "SR_001"),
      ).toDF(),

      normalized_snv_somatic.id -> Seq(
        // should not be counted for the germline cnv as it is somatic (tumor only)
        NormalizedSNVSomatic(`chromosome` = "1", `start` = 150, `end` = 160, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_01", `sequencing_id` = "SR_001", bioinfo_analysis_code = "TEBA"),

        NormalizedSNVSomatic(`chromosome` = "1", `start` = 350, `end` = 360, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_02", `sequencing_id` = "SR_002", bioinfo_analysis_code = "TEBA"),
      ).toDF()
    )

    job.transformSingle(data)
      .as[EnrichedCNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCNV(`chromosome` = "1", `start` = 100, `end` = 200, `alternate` = "A", `reference` = "REF", `name` = "CNV_01", `snv_count` = 1, `sequencing_id` = "SR_001",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "256519903d044ebbb123d95989175e04d8c82dd2",
      ),
      EnrichedCNV(`chromosome` = "1", `start` = 300, `end` = 400, `alternate` = "A", `reference` = "REF", `name` = "CNV_SOMATIC_01", `variant_type` = "somatic", `snv_count` = 1, `sequencing_id` = "SR_002",
        `number_genes` = 0, `genes` = List(), `transmission` = null, `parental_origin` = null, `cn` = None, `frequency_RQDM` = null,
        `cluster` = EnrichedCNVCluster(`id` = None, `external_frequencies` = EnrichedCNVClusterFrequencies(`gnomad_exomes_4` = None)),
        `hash` = "6d2e2ec49033dbbda09b1a23f7ef1cc12a161d86"
      )
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


  "load" should "save enriched CNV data correctly" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val job = CNV(TestETLContext()(updatedConf, spark), batchId = Some("BAT1"))

      val enrichedCnvData = Seq(
        EnrichedCNV(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001"),
        EnrichedCNV(`batch_id` = "BAT1", `analysis_id` = "SRA0001", `sequencing_id` = "SRS0001")
      )

      job.load(Map(destination.id -> enrichedCnvData.toDF))

      val result = destination.read(updatedConf, spark)
      result.as[EnrichedCNV].collect() should contain theSameElementsAs enrichedCnvData
    }
  }
}
