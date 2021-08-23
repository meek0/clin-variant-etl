package bio.ferlab.clin.model

import java.sql.Timestamp

case class ConsequenceRawOutput(`chromosome`: String = "1",
                                `start`: Long = 69897,
                                `end`: Long = 69898,
                                `reference`: String = "T",
                                `alternate`: String = "C",
                                `name`: String = "rs200676709",
                                `consequences`: List[String] = List("downstream_gene_variant"),
                                `impact`: String = "MODIFIER",
                                `symbol`: String = "DDX11L1",
                                `ensembl_gene_id`: String = "ENSG00000223972",
                                `ensembl_feature_id`: String = "ENST00000450305",
                                `ensembl_transcript_id`: String = "ENST00000450305",
                                `ensembl_regulatory_id`: Option[String] = None,
                                `feature_type`: String = "Transcript",
                                `strand`: Int = 1,
                                `biotype`: String = "transcribed_unprocessed_pseudogene",
                                `variant_class`: String = "SNV",
                                `exon`: EXON = EXON(),
                                `intron`: INTRON = INTRON(),
                                `hgvsc`: Option[String] = None,
                                `hgvsp`: Option[String] = None,
                                `hgvsg`: String = "chr1:g.16856A>G",
                                `cds_position`: Option[Int] = None,
                                `cdna_position`: Option[Int] = None,
                                `protein_position`: Option[Int] = None,
                                `amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                                `codons`: CODONS = CODONS(),
                                `pick`: Boolean = false,
                                `original_canonical`: Boolean = false,
                                `aa_change`: Option[String] = None,
                                `coding_dna_change`: Option[String] = None,
                                `impact_score`: Int = 1,
                                `batch_id`: String = "BAT1",
                                `created_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"),
                                `updated_on`: Timestamp = Timestamp.valueOf("2021-02-26 14:50:08.108"))

case class EXON(`rank`: Option[Int] = None,
                `total`: Option[Int] = None)

case class INTRON(`rank`: Option[Int] = None,
                  `total`: Option[Int] = None)

case class AMINO_ACIDS(`reference`: Option[String] = None,
                       `variant`: Option[String] = None)

case class CODONS(`reference`: Option[String] = None,
                  `variant`: Option[String] = None)
