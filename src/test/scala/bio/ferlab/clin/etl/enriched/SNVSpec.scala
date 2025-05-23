package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.SNV._
import bio.ferlab.clin.model.enriched.{EXOMISER, EXOMISER_OTHER_MOI, EnrichedCNV, EnrichedCNVCluster, EnrichedCNVClusterFrequencies, EnrichedSNV}
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedCNVSomaticTumorOnly, NormalizedExomiser, NormalizedFranklin, NormalizedSNV}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class SNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")

  it should "transform data into expected format" in {
    val snvDf = Seq(NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1")).toDF()
    val cnvDf = Seq(NormalizedCNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1")).toDF()
    val exomiserDf = Seq(
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 1),
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "AD", gene_combined_score = 0.99f),
    ).toDF()
    val franklinDf = Seq(NormalizedFranklin(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = Some("aliquot1"))).toDF()

    val data = Map(
      normalized_snv.id -> snvDf,
      normalized_cnv.id -> cnvDf,
      normalized_exomiser.id -> exomiserDf,
      normalized_franklin.id -> franklinDf
    )

    val job = SNV(TestETLContext())

    val result = job.transformSingle(data).as[EnrichedSNV].collect()
    result.length shouldBe 1
    result.head shouldBe EnrichedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1")
  }

  "withExomiser" should "enrich snv with exomiser" in {
    val snvDf = Seq(
      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
      NormalizedSNV(chromosome = "1", start = 2, reference = "C", alternate = "G", aliquot_id = "aliquot1"),

      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2"), // no exomiser data

      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3"),
    ).toDF()
    val exomiserDf = Seq(
      // aliquot1
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 1), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "AD", gene_combined_score = 0.99f), // should be in exomiser_other_moi

      NormalizedExomiser(chromosome = "1", start = 2, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 0.99f), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 2, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = false, gene_combined_score = 1), // should not be in exomiser_other_moi

      // aliquot3
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3", contributing_variant = true, moi = "XR", gene_combined_score = 0.5f), // should be in exomiser
    ).toDF()

    val result = snvDf.withExomiser(exomiserDf)

    val expected = Seq(
      ("1", 1, "T", "A", "aliquot1", Some(EXOMISER(moi = "XR", gene_combined_score = 1)), Some(EXOMISER_OTHER_MOI(moi = "AD", gene_combined_score = 0.99f))),
      ("1", 2, "C", "G", "aliquot1", Some(EXOMISER(moi = "XR", gene_combined_score = 0.99f)), None),
      ("1", 1, "T", "A", "aliquot2", None, None),
      ("1", 1, "T", "A", "aliquot3", Some(EXOMISER(moi = "XR", gene_combined_score = 0.5f)), None),
    )

    result
      .selectLocus($"aliquot_id", $"exomiser", $"exomiser_other_moi")
      .as[(String, Long, String, String, String, Option[EXOMISER], Option[EXOMISER_OTHER_MOI])] // Use types because Row comparison enforces column order
      .collect() should contain theSameElementsAs expected
  }

  "withFranklin" should "enrich snv with franklin" in {
    val snvDf = Seq(
      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
      NormalizedSNV(chromosome = "1", start = 2, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2"),
      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3"),
    ).toDF()

    val franklinDf = Seq(
      NormalizedFranklin(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = Some("aliquot1"), `score` = 0.9),
      NormalizedFranklin(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = Some("aliquot2"), `score` = 0.5),
    ).toDF()

    val result = snvDf.withFranklin(franklinDf)

    val expected = Seq(
      EnrichedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", franklin_combined_score = Some(0.9)),
      EnrichedSNV(chromosome = "1", start = 2, reference = "T", alternate = "A", aliquot_id = "aliquot1", franklin_combined_score = None),
      EnrichedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2", franklin_combined_score = Some(0.5)),
      EnrichedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3", franklin_combined_score = None),
    ).toDF().selectLocus($"aliquot_id", $"franklin_combined_score").collect()

    result
      .selectLocus($"aliquot_id", $"franklin_combined_score")
      .collect() should contain theSameElementsAs expected
  }

  it should "enrich SNV data with cnv count" in {
    val data = Map(
      normalized_snv.id -> Seq(
        // cover 0 CNV
        NormalizedSNV(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_00", `analysis_id` = "SRA0001", `sequencing_id` = "SR_000"),
        // cover 1 CNV
        NormalizedSNV(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_01", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        // cover 3 CNV
        NormalizedSNV(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_02", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        // cover 0 CNV
        NormalizedSNV(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_03", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        // cover 0 CNV (cause different service_request_id)
        NormalizedSNV(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", reference = "REF", `hgvsg` = "SNV_04", `analysis_id` = "SRA0001", `sequencing_id` = "SR_002"),
      ).toDF(),
      normalized_cnv.id -> Seq(
        NormalizedCNV(`chromosome` = "1", `start` = 90, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_01", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 110, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_02", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 130, `end` = 200, `alternate` = "A", reference = "REF", `name` = "CNV_03", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
        NormalizedCNV(`chromosome` = "1", `start` = 220, `end` = 500, `alternate` = "A", reference = "REF", `name` = "CNV_04", `analysis_id` = "SRA0001", `sequencing_id` = "SR_001"),
      ).toDF(),
      normalized_exomiser.id -> Seq(NormalizedExomiser()).toDF(),
      normalized_franklin.id -> Seq(NormalizedFranklin()).toDF()
    )

    val result = SNV(TestETLContext()).transformSingle(data)

    result.as[EnrichedSNV]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNV(`chromosome` = "1", `start` = 1, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_00", `cnv_count` = 0, `analysis_id` = "SRA0001", `sequencing_id` = "SR_000",
        `exomiser` = None, `exomiser_other_moi` = None, `franklin_combined_score` = None,
      ),
      EnrichedSNV(`chromosome` = "1", `start` = 90, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_01", `cnv_count` = 1, `analysis_id` = "SRA0001", `sequencing_id` = "SR_001",
        `exomiser` = None, `exomiser_other_moi` = None, `franklin_combined_score` = None,
      ),
      EnrichedSNV(`chromosome` = "1", `start` = 130, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_02", `cnv_count` = 3, `analysis_id` = "SRA0001", `sequencing_id` = "SR_001",
        `exomiser` = None, `exomiser_other_moi` = None, `franklin_combined_score` = None,
      ),
      EnrichedSNV(`chromosome` = "1", `start` = 210, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_03", `cnv_count` = 0, `analysis_id` = "SRA0001", `sequencing_id` = "SR_001",
        `exomiser` = None, `exomiser_other_moi` = None, `franklin_combined_score` = None,
      ),
      EnrichedSNV(`chromosome` = "1", `start` = 100, `end` = 500, `alternate` = "A", `reference` = "REF", `hgvsg` = "SNV_04", `cnv_count` = 0, `analysis_id` = "SRA0001", `sequencing_id` = "SR_002",
        `exomiser` = None, `exomiser_other_moi` = None, `franklin_combined_score` = None,
      ),
    )
  }
}

