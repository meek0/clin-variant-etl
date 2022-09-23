/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-04-06T13:41:33.446119
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class NormalizedConsequences(`chromosome`: String = "1",
                                  `start`: Long = 69897,
                                  `end`: Long = 69898,
                                  `reference`: String = "T",
                                  `alternate`: String = "C",
                                  `name`: String = "rs200676709",
                                  `consequences`: List[String] = List("synonymous_variant"),
                                  `impact`: String = "LOW",
                                  `symbol`: String = "OR4F5",
                                  `ensembl_gene_id`: String = "ENSG00000186092",
                                  `ensembl_feature_id`: String = "ENST00000335137",
                                  `ensembl_transcript_id`: String = "ENST00000335137",
                                  `ensembl_regulatory_id`: Option[String] = None,
                                  `feature_type`: String = "Transcript",
                                  `strand`: Int = 1,
                                  `biotype`: String = "protein_coding",
                                  `variant_class`: String = "SNV",
                                  `exon`: EXON = EXON(),
                                  `intron`: INTRON = INTRON(),
                                  `hgvsc`: String = "ENST00000335137.4:c.807T>C",
                                  `hgvsp`: String = "ENSP00000334393.3:p.Ser269=",
                                  `hgvsg`: String = "chr1:g.69897T>C",
                                  `cds_position`: Int = 807,
                                  `cdna_position`: Int = 843,
                                  `protein_position`: Int = 269,
                                  `amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                                  `codons`: CODONS = CODONS(),
                                  `pick`: Boolean = true,
                                  `original_canonical`: Boolean = true,
                                  `refseq_mrna_id`: List[String] = List("NM_001005484.1", "NM_001005484.2"),
                                  `aa_change`: String = "p.Ser269=",
                                  `coding_dna_change`: String = "c.807T>C",
                                  `impact_score`: Int = 2,
                                  `batch_id`: String = "BAT1",
                                  `created_on`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                  `updated_on`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                  `normalized_consequences_oid`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"))

case class EXON(`rank`: Int = 1,
                `total`: Int = 1)

case class INTRON(`rank`: Option[Int] = None,
                  `total`: Option[Int] = None)

case class AMINO_ACIDS(`reference`: String = "S",
                       `variant`: Option[String] = None)

case class CODONS(`reference`: String = "tcT",
                  `variant`: String = "tcC")