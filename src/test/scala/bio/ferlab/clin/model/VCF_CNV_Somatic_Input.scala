/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-12-05T15:15:37.294787
 */
package bio.ferlab.clin.model

case class VCF_CNV_Somatic_Input(`contigName`: String = "chr1",
                         `start`: Long = 9999,
                         `names`: Seq[String] = Seq("DRAGEN:LOSS:chr1:9823628-9823687"),
                         `referenceAllele`: String = "A",
                         `alternateAlleles`: Seq[String] = Seq("TAA"),
                         `qual`: Double = 27.0,
                         `filters`: Seq[String] = Seq("cnvQual"),
                         `splitFromMultiAllelic`: Boolean = false,
                         `INFO_OLD_MULTIALLELIC`: Boolean = false,
                         `INFO_CIEND`: Option[Seq[Int]] = None,
                         `INFO_CIPOS`: Option[Seq[Int]] = None,
                         `INFO_SVLEN`: Seq[Int] = Seq(-60),
                         `INFO_REFLEN`: Int = 60,
                         `INFO_END`: Option[Int] = None,
                         `INFO_SVTYPE`: String = "CNV",
                         `INFO_FILTERS`: Seq[String] = Seq("cnvQual"),
                         `genotypes`: Seq[CNV_GENOTYPES_somatic] = Seq(CNV_GENOTYPES_somatic()))

case class CNV_GENOTYPES_somatic(`sampleId`: String = "11111",
                     `SM`: Double = 0.57165,
                     `phased`: Boolean = false,
                     `calls`: Seq[Int] = Seq(0, 1),
                     `BC`: Int = 1,
                     `PE`: Seq[Int] = Seq(0, 0))
