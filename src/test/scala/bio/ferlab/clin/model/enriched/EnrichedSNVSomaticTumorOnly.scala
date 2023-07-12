/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-05-16T17:35:13.734545
 */
package bio.ferlab.clin.model.enriched

import java.sql.Date


case class EnrichedSNVSomaticTumorOnly(`chromosome`: String = "1",
                       `start`: Long = 69897,
                       `reference`: String = "T",
                       `alternate`: String = "C",
                       `patient_id`: String = "PA0001",
                       `aliquot_id`: String = "11111",
                       `end`: Long = 69898,
                       `name`: String = "rs200676709",
                       `dp`: Int = 1,
                       `sq`: Double = 56.08,
                       `calls`: Seq[Int] = Seq(0, 1),
                       `has_alt`: Boolean = true,
                       `is_multi_allelic`: Boolean = false,
                       `old_multi_allelic`: Option[String] = None,
                       `filters`: Seq[String] = Seq("PASS"),
                       `ad_ref`: Int = 0,
                       `ad_alt`: Int = 30,
                       `ad_total`: Int = 30,
                       `ad_ratio`: Double = 1.0,
                       `hgvsg`: String = "chr1:g.69897T>C",
                       `variant_class`: String = "SNV",
                       `batch_id`: String = "BAT1",
                       `last_update`: Date = java.sql.Date.valueOf("2022-04-06"),
                       `variant_type`: String = "somatic_tumor_only",
                       `bioinfo_analysis_code`: String = "TEBA",
                       `service_request_id`: String = "SR0095",
                       `analysis_service_request_id`: String = "SRA0001",
                       `sequencing_strategy`: String = "WXS",
                       `genome_build`: String = "GRCh38",
                       `analysis_code`: String = "MM_PG",
                       `analysis_display_name`: String = "Maladies musculaires (Panel global)",
                       `family_id`: String = "FM00001",
                       `is_proband`: Boolean = true,
                       `gender`: String = "Male",
                       `practitioner_role_id`: String = "PPR00101",
                       `organization_id`: String = "OR00201",
                       `affected_status`: Boolean = true,
                       `affected_status_code`: String = "affected",
                       `mother_id`: String = "PA0003",
                       `father_id`: String = "PA0002",
                       `specimen_id`: String = "SP_696",
                       `sample_id`: String = "14-696",
                       `mother_calls`: Seq[Int] = Seq(0, 1),
                       `father_calls`: Seq[Int] = Seq(0, 0),
                       `mother_affected_status`: Boolean = true,
                       `father_affected_status`: Boolean = false,
                       `zygosity`: String = "HET",
                       `mother_zygosity`: String = "HET",
                       `father_zygosity`: String = "WT",
                       `parental_origin`: Option[String] = Some("mother"),
                       `transmission`: Option [String] = Some("autosomal_dominant"),
                       `is_hc`: Boolean = false,
                       `hc_complement`: Seq[HC_COMPLEMENT_SOMATIC_TUMOR_ONLY] = Seq(HC_COMPLEMENT_SOMATIC_TUMOR_ONLY()),
                       `possibly_hc_complement`: Seq[POSSIBLY_HC_COMPLEMENT_SOMATIC_TUMOR_ONLY] = Seq(POSSIBLY_HC_COMPLEMENT_SOMATIC_TUMOR_ONLY()),
                       `is_possibly_hc`: Boolean = false,
                       `exomiser_variant_score`: Option[Float] = Some(0.6581f),
                       `exomiser`: Option[EXOMISER] = Some(EXOMISER()),
                       `exomiser_other_moi`: Option[EXOMISER_OTHER_MOI] = Some(EXOMISER_OTHER_MOI()))

case class HC_COMPLEMENT_SOMATIC_TUMOR_ONLY(`symbol`: Option[String] = None,
                         `locus`: Option[Seq[String]] = None)

case class POSSIBLY_HC_COMPLEMENT_SOMATIC_TUMOR_ONLY(`symbol`: Option[String] = None,
                                  `count`: Option[Long] = None)

