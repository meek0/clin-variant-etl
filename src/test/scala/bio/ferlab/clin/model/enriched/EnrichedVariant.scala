/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-02-26T14:31:57.067
 */
package bio.ferlab.clin.model.enriched

import bio.ferlab.clin.etl.varsome.{Classification, Publication}
import bio.ferlab.clin.model.VarsomeOutput.{defaultClassifications, defaultPublications}
import bio.ferlab.clin.model._
import bio.ferlab.datalake.spark3.testmodels.enriched.EnrichedVariant.CMC
import bio.ferlab.datalake.spark3.testmodels.enriched._

import java.sql.{Date, Timestamp}
import java.time.LocalDate


case class EnrichedVariant(`chromosome`: String = "1",
                           `start`: Long = 69897,
                           `end`: Long = 69898,
                           `reference`: String = "T",
                           `alternate`: String = "C",
                           `locus`: String = "1-69897-T-C",
                           `hash`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                           `genes_symbol`: List[String] = List("OR4F5"),
                           `hgvsg`: String = "chr1:g.69897T>C",
                           `variant_class`: String = "SNV",
                           `pubmed`: Option[List[String]] = None,
                           `assembly_version`: String = "GRCh38",
                           `last_annotation_update`: Date = Date.valueOf(LocalDate.now()),
                           `created_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"),
                           `updated_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"),
                           `variant_type`: List[String] = List("germline", "somatic_tumor_only"),
                           `donors`: List[DONORS] = List(DONORS(), DONORS()),
                           `frequencies_by_analysis`: List[AnalysisCodeFrequencies] = List(AnalysisCodeFrequencies()),
                           `frequency_RQDM`: AnalysisFrequencies = AnalysisFrequencies(),
                           `external_frequencies`: FREQUENCIES = FREQUENCIES(),
                           `clinvar`: CLINVAR = CLINVAR(),
                           `rsnumber`: String = "rs200676709",
                           `dna_change`: String = "T>C",
                           `genes`: List[GENES] = List(GENES()),
                           `omim`: List[String] = List("618285"),
                           `variant_external_reference`: List[String] = List("DBSNP", "Clinvar", "Cosmic", "PubMed"),
                           `gene_external_reference`: List[String] = List("HPO", "Orphanet", "OMIM", "DDD", "Cosmic", "gnomAD", "SpliceAI"),
                           `panels`: List[String] = List("DYSTM", "MITN"),
                           `varsome`: Option[VARSOME] = Some(VARSOME()),
                           `exomiser_variant_score`: Option[Float] = Some(0.6581f),
                           `cmc`: CMC = CMC())


case class DONORS(`dp`: Int = 1,
                  `gq`: Option[Int] = Some(30),
                  `sq`: Option[Double] = Some(56.08),
                  `calls`: List[Int] = List(1, 1),
                  `qd`: Option[Double] = Some(8.07),
                  `mother_gq`: Option[Int] = Some(30),
                  `mother_qd`: Option[Double] = Some(8.07),
                  `father_gq`: Option[Int] = Some(30),
                  `father_qd`: Option[Double] = Some(8.07),
                  `has_alt`: Boolean = true,
                  `filters`: List[String] = List("PASS"),
                  `ad_ref`: Int = 0,
                  `ad_alt`: Int = 30,
                  `ad_total`: Int = 30,
                  `ad_ratio`: Double = 1.0,
                  `zygosity`: String = "HOM",
                  `hgvsg`: String = "chr1:g.69897T>C",
                  `variant_class`: String = "SNV",
                  `batch_id`: String = "BAT1",
                  `service_request_id`: String = "SR0095",
                  `sample_id`: String = "14-696",
                  `specimen_id`: String = "SP_696",
                  `last_update`: Date = Date.valueOf("2022-04-06"),
                  `variant_type`: String = "germline",
                  `bioinfo_analysis_code`: String = "GEAN",
                  `patient_id`: String = "PA0001",
                  `family_id`: String = "FM00001",
                  `practitioner_role_id`: String = "PPR00101",
                  `organization_id`: String = "OR00201",
                  `sequencing_strategy`: String = "WXS",
                  `aliquot_id`: String = "11111",
                  `analysis_code`: String = "MM_PG",
                  `analysis_display_name`: String = "Maladies musculaires (Panel global)",
                  `mother_id`: String = "PA0003",
                  `father_id`: String = "PA0002",
                  `mother_calls`: Option[List[Int]] = None,
                  `father_calls`: Option[List[Int]] = None,
                  `mother_affected_status`: Option[Boolean] = None,
                  `father_affected_status`: Option[Boolean] = None,
                  `mother_zygosity`: Option[String] = None,
                  `father_zygosity`: Option[String] = None,
                  `parental_origin`: Option[String] = None,
                  `transmission`: Option[String] = None,
                  `is_hc`: Boolean = false,
                  `hc_complement`: List[HC_COMPLEMENT] = List(HC_COMPLEMENT()),
                  `possibly_hc_complement`: List[POSSIBLY_HC_COMPLEMENT] = List(POSSIBLY_HC_COMPLEMENT()),
                  `is_possibly_hc`: Boolean = false,
                  `exomiser`: Option[EXOMISER] = Some(EXOMISER()),
                  `exomiser_other_moi`: Option[EXOMISER_OTHER_MOI] = Some(EXOMISER_OTHER_MOI()))

case class FREQUENCIES(thousand_genomes: ThousandGenomesFreq = ThousandGenomesFreq(3446, 5008, 0.688099),
                       topmed_bravo: Freq = Freq(2, 125568, 0.0000159276, 0, 2),
                       gnomad_genomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(1, 26342, 0.000037962189659099535, 0),
                       gnomad_exomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(0, 2, 0.0, 0),
                       gnomad_genomes_3_0: GnomadFreqOutput = GnomadFreqOutput(0, 53780, 0.0, 0),
                       gnomad_genomes_3_1_1: GnomadFreqOutput = GnomadFreqOutput(10, 20, 0.5, 10))


case class ThousandGenomesFreq(ac: Long = 10,
                               an: Long = 20,
                               af: Double = 0.5)

case class GnomadFreqOutput(ac: Long = 10,
                            an: Long = 20,
                            af: Double = 0.5,
                            hom: Long = 10)

case class Freq(ac: Long = 10,
                an: Long = 20,
                af: Double = 0.5,
                hom: Long = 10,
                het: Long = 10)

case class CLINVAR(`clinvar_id`: String = "445750",
                   `clin_sig`: List[String] = List("Likely_benign"),
                   `conditions`: List[String] = List("not provided"),
                   `inheritance`: List[String] = List("germline"),
                   `interpretations`: List[String] = List("", "Likely_benign"))

case class GENES(`symbol`: Option[String] = Some("OR4F5"),
                 `entrez_gene_id`: Option[Int] = Some(777),
                 `omim_gene_id`: Option[String] = Some("601013"),
                 `hgnc`: Option[String] = Some("HGNC:1392"),
                 `ensembl_gene_id`: Option[String] = Some("ENSG00000198216"),
                 `location`: Option[String] = Some("1q25.3"),
                 `name`: Option[String] = Some("calcium voltage-gated channel subunit alpha1 E"),
                 `alias`: Option[List[String]] = Some(List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")),
                 `biotype`: Option[String] = Some("protein_coding"),
                 `orphanet`: List[ORPHANET] = List(ORPHANET()),
                 `hpo`: List[HPO] = List(HPO()),
                 `omim`: List[OMIM] = List(OMIM()),
                 `ddd`: List[DDD] = List(DDD()),
                 `cosmic`: List[COSMIC] = List(COSMIC()),
                 `spliceai`: Option[SPLICEAI] = Some(SPLICEAI()),
                 `gnomad`: Option[GNOMAD] = Some(GNOMAD()))

case class VARSOME(
                    `variant_id`: Option[String] = Some("10190150730274780002"),
                    `publications`: Seq[Publication] = defaultPublications,
                    `has_publication`:Boolean = true,
                    `acmg`: Option[ACMG] = Some(ACMG())
                  )

case class ACMG(
                 `verdict`: Option[VERDICT] = Some(VERDICT()),
                 `classifications`: Seq[Classification] = defaultClassifications,
               )

case class VERDICT(
                    `clinical_score`: Option[Double] = Some(1.242361132330188),
                    `verdict`: Option[String] = Some("Benign"),
                    `approx_score`: Option[Int] = Some(-11),
                    `pathogenic_subscore`: Option[String] = Some("Uncertain Significance"),
                    `benign_subscore`: Option[String] = Some("Benign")
                  )

case class SPLICEAI(`ds`: Double = 0.01,
                    `type`: Seq[String] = Seq("AG"))

case class GNOMAD(`pli`: Float = 1.0f,
                  `loeuf`: Float = 0.054f)
