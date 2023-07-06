/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-12-05T17:38:33.300234
 */
package bio.ferlab.clin.model




case class CnvCentricOutput(`aliquot_id`: String = "1",
                            `chromosome`: String = "1",
                            `start`: Long = 10000,
                            `end`: Long = 10059,
                            `reference`: String = "A",
                            `alternate`: String = "TAA",
                            `name`: String = "DRAGEN:LOSS:chr1:9823628-9823687",
                            `qual`: Double = 27.0,
                            `bc`: Int = 1,
                            `sm`: Double = 0.57165,
                            `calls`: Seq[Int] = Seq(0, 1),
                            `cn`: Int = 1,
                            `pe`: Seq[Int] = Seq(0, 0),
                            `is_multi_allelic`: Boolean = false,
                            `old_multi_allelic`: Option[String] = None,
                            `ciend`: Option[Seq[Int]] = None,
                            `cipos`: Option[Seq[Int]] = None,
                            `svlen`: Int = -60,
                            `reflen`: Int = 60,
                            `svtype`: String = "CNV",
                            `filters`: Seq[String] = Seq("cnvQual"),
                            `batch_id`: String = "BAT1",
                            `type`: String = "LOSS",
                            `sort_chromosome`: Int = 1,
                            `service_request_id`: String = "SRS0001",
                            `patient_id`: String = "PA0001",
                            `analysis_service_request_id`: String = "SRA0001",
                            `sequencing_strategy`: String = "WXS",
                            `genome_build`: String = "GRCh38",
                            `analysis_code`: String = "MMG",
                            `analysis_display_name`: String = "Maladies musculaires (Panel global)",
                            `affected_status`: Boolean = true,
                            `affected_status_code`: String = "affected",
                            `family_id`: String = "FM00001",
                            `is_proband`: Boolean = true,
                            `gender`: String = "Male",
                            `practitioner_role_id`: String = "PPR00101",
                            `organization_id`: String = "OR00201",
                            `mother_id`: String = "PA0003",
                            `father_id`: String = "PA0002",
                            `specimen_id`: String = "SP_001",
                            `sample_id`: String = "SA_001",
                            `genes`: Seq[CNV_CENTRIC_GENES] = Seq(CNV_CENTRIC_GENES()),
                            `number_genes`: Int = 1)

case class CNV_CENTRIC_GENES(`symbol`: String = "OR4F5",
                 `refseq_id`: String = "NC_000001.11",
                 `gene_length`: String = "60.0",
                 `overlap_bases`: String = "60.0",
                 `overlap_cnv_ratio`: Double = 1.0,
                 `overlap_gene_ratio`: Double = 1.0,
                 `panels`: Seq[String] = Seq("DYSTM", "MITN"),
                 `overlap_exons`: String = "1")
