package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.SNV._
import bio.ferlab.clin.model.enriched.{EXOMISER, EXOMISER_OTHER_MOI, EnrichedSNV}
import bio.ferlab.clin.model.normalized.{NormalizedExomiser, NormalizedFranklin, NormalizedSNV}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.testutils.{DeprecatedTestETLContext, SparkSpec}

class SNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")
  val normalized_franklin: DatasetConf = conf.getDataset("normalized_franklin")

  it should "transform data into expected format" in {
    val snvDf = Seq(NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1")).toDF()
    val exomiserDf = Seq(
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 1),
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "AD", gene_combined_score = 0.99f),
    ).toDF()
    val franklinDf = Seq(NormalizedFranklin(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = Some("aliquot1"))).toDF()

    val data = Map(
      normalized_snv.id -> snvDf,
      normalized_exomiser.id -> exomiserDf,
      normalized_franklin.id -> franklinDf
    )

    val job = SNV(DeprecatedTestETLContext())

    val result = job.transformSingle(data).as[EnrichedSNV].collect()
    result.length shouldBe 1
    result.head shouldBe EnrichedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1")
  }

  "withExomiser" should "enrich snv with exomiser" in {
    val snvDf = Seq(
      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
      NormalizedSNV(chromosome = "1", start = 2, reference = "A", alternate = "C", aliquot_id = "aliquot1"),
      NormalizedSNV(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1"),

      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2"), // no exomiser data

      NormalizedSNV(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3"),
    ).toDF()
    val exomiserDf = Seq(
      // aliquot1
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 1), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "AD", gene_combined_score = 0.99f), // should be in exomiser_other_moi

      NormalizedExomiser(chromosome = "1", start = 2, reference = "A", alternate = "C", aliquot_id = "aliquot1", exomiser_variant_score = 1, contributing_variant = false), // exomiser_variant_score only

      NormalizedExomiser(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 0.99f), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = false, gene_combined_score = 1), // should not be in exomiser_other_moi

      // aliquot3
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3", contributing_variant = true, moi = "XR", gene_combined_score = 0.5f), // should be in exomiser
    ).toDF()

    val result = snvDf.withExomiser(exomiserDf)

    val expected = Seq(
      ("1", 1, "T", "A", "aliquot1", Some(0.6581f), Some(EXOMISER(moi = "XR", gene_combined_score = 1)), Some(EXOMISER_OTHER_MOI(moi = "AD", gene_combined_score = 0.99f))),
      ("1", 2, "A", "C", "aliquot1", Some(1), None, None),
      ("1", 3, "C", "G", "aliquot1", Some(0.6581f), Some(EXOMISER(moi = "XR", gene_combined_score = 0.99f)), None),
      ("1", 1, "T", "A", "aliquot2", None, None, None),
      ("1", 1, "T", "A", "aliquot3", Some(0.6581f), Some(EXOMISER(moi = "XR", gene_combined_score = 0.5f)), None),
    )

    result
      .selectLocus($"aliquot_id", $"exomiser_variant_score", $"exomiser", $"exomiser_other_moi")
      .as[(String, Long, String, String, String, Option[Float], Option[EXOMISER], Option[EXOMISER_OTHER_MOI])] // Use types because Row comparison enforces column order
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
}

