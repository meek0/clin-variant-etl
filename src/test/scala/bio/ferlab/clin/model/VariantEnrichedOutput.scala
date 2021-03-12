/**
 * Generated by [[bio.ferlab.clin.testutils.ClassGenerator]]
 * on 2021-02-26T14:31:57.067
 */
package bio.ferlab.clin.model

import java.sql.Date
import java.time.LocalDate


case class VariantEnrichedOutput(`chromosome`: String = "1",
                                 `start`: Long = 69897,
                                 `end`: Long = 69898,
                                 `reference`: String = "T",
                                 `alternate`: String = "C",
                                 `name`: String = "rs200676709",
                                 `is_multi_allelic`: Boolean = false,
                                 `old_multi_allelic`: Option[String] = None,
                                 `genes_symbol`: List[String] = List("OR4F5"),
                                 `hgvsg`: String = "chr1:g.69897T>C",
                                 `variant_class`: String = "SNV",
                                 `pubmed`: Option[List[String]] = None,
                                 `batch_id`: String = "BAT1",
                                 `last_batch_id`: Option[String] = None,
                                 `assembly_version`: String = "GRCh38",
                                 `last_annotation_update`: Date = Date.valueOf(LocalDate.now()),
                                 `createdOn`: String = "BAT1",//Timestamp = Timestamp.valueOf("2021-02-26 14:31:48.242"),
                                 `updatedOn`: String = "BAT1",//Timestamp = Timestamp.valueOf("2021-02-26 14:31:48.243"),
                                 `variant_type`: String = "germline",
                                 `donors`: List[DONORS] = List(DONORS(), DONORS(`organization_id` = "OR00202")),
                                 `lab_frequencies`: Map[String, Freq] = Map("OR00201" -> Freq(2, 2, 1.0, 1, 0), "OR00202" -> Freq(2, 2, 1.0, 1, 0)),
                                 `frequencies`: FREQUENCIES = FREQUENCIES(),
                                 `clinvar`: CLINVAR = CLINVAR(),
                                 `dbsnp`: String = "rs200676709",
                                 `participant_number`: Long = 2,
                                 `dna_change`: String = "T>C",
                                 `genes`: List[GENES] = List(GENES()),
                                 `omim`: List[String] = List("618285"),
                                 `ext_db`: EXT_DB = EXT_DB())


case class DONORS(`dp`: Int = 1,
                  `gq`: Int = 2,
                  `calls`: List[Int] = List(1, 1),
                  `qd`: Double = 8.07,
                  `has_alt`: Boolean = true,
                  `ad_ref`: Int = 0,
                  `ad_alt`: Int = 1,
                  `ad_total`: Int = 1,
                  `ad_ratio`: Double = 1.0,
                  `zygosity`: String = "HOM",
                  `hgvsg`: String = "chr1:g.69897T>C",
                  `variant_class`: String = "SNV",
                  `batch_id`: String = "BAT1",
                  `last_update`: Date = Date.valueOf("2020-11-29"),
                  `variant_type`: String = "germline",
                  `biospecimen_id`: String = "SP14909",
                  `patient_id`: String = "PA0001",
                  `family_id`: String = "FA0001",
                  `practitioner_id`: String = "PPR00101",
                  `organization_id`: String = "OR00201",
                  `sequencing_strategy`: String = "WXS",
                  `study_id`: String = "ET00010")

case class FREQUENCIES(//`1000_genomes`: Freq = Freq(3446, 5008,  0.688099),
                       topmed_bravo: Freq = Freq(2, 125568, 0.0000159276, 0, 2),
                       gnomad_genomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(1, 26342, 0.000037962189659099535, 0),
                       exac: GnomadFreqOutput = GnomadFreqOutput(0, 2, 0.0, 0),
                       gnomad_genomes_3_0: GnomadFreqOutput = GnomadFreqOutput(0, 53780, 0.0, 0),
                       internal: Freq = Freq(4, 4, 1.0, 2, 0))


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
                 `omim`: List[OMIM] = List(OMIM()))


case class EXT_DB(`is_pubmed`: Boolean = false,
                  `is_dbsnp`: Boolean = true,
                  `is_clinvar`: Boolean = true,
                  `is_hpo`: Boolean = true,
                  `is_orphanet`: Boolean = true,
                  `is_omim`: Boolean = true)
