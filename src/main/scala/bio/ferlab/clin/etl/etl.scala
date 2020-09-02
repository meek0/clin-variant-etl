package bio.ferlab.clin

import io.projectglow.Glow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DecimalType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

package object etl {
  def vcf(input: String)(implicit spark: SparkSession): DataFrame = {
    val inputs = input.split(",")
    val df = spark.read.format("vcf")
      .option("flattenInfoFields", "true")
      .load(inputs: _*)

    Glow.transform("split_multiallelics", df)
  }

  def tableName(table: String, studyId: String, releaseId: String): String = {
    s"${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def colFromArrayOrField(df: DataFrame, colName: String): Column = {
    df.schema(colName).dataType match {
      case ArrayType(_, _) => df(colName)(0)
      case _ => df(colName)
    }
  }

  def firstAs(c: String): Column = first(col(c)) as c

  object columns {
    val zygosity: Column = when(col("calls")(0) === 1 && col("calls")(1) === 1, "HOM")
      .when(col("calls")(0) === 0 && col("calls")(1) === 1, "HET")
      .when(col("calls")(0) === 0 && col("calls")(1) === 0, "HOM REF")
      .when(col("calls")(0) === 1 && col("calls")(1) === 0, "HET")
      .otherwise("UNK")

    val chromosome: Column = ltrim(col("contigName"), "chr") as "chromosome"
    val reference: Column = col("referenceAllele") as "reference"
    val start: Column = (col("start") + 1) as "start"
    val end: Column = (col("end") + 1) as "end"

    val alternate: Column = col("alternateAlleles")(0) as "alternate"
    val name: Column = col("names")(0) as "name"
    val calculated_af: Column = col("ac").divide(col("an")).cast(DecimalType(8, 8)) as "af"

    def array_sum(c: Column) = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

    val ac: Column = sum(array_sum(filter(col("calls"), c => c === 1))) as "ac"
    val an: Column = sum(array_sum(transform(col("calls"), c => when(c === 1 || c === 0, 1).otherwise(0)))) as "an"
    val info_ac: Column = col("INFO_AC")(0) as "ac"
    val info_af: Column = col("INFO_AF")(0) as "af"
    val info_an: Column = col("INFO_AN") as "an"
    val info_afr_af: Column = col("INFO_AFR_AF")(0) as "afr_af"

    val info_eur_af: Column = col("INFO_EUR_AF")(0) as "eur_af"
    val info_sas_af: Column = col("INFO_SAS_AF")(0) as "sas_af"
    val info_amr_af: Column = col("INFO_AMR_AF")(0) as "amr_af"
    val info_eas_af: Column = col("INFO_EAS_AF")(0) as "eas_af"

    val dp: Column = col("INFO_DP") as "dp"

    val countHomozygotesUDF: UserDefinedFunction = udf { calls: Seq[Seq[Int]] => calls.map(_.sum).count(_ == 2) }
    val homozygotes: Column = countHomozygotesUDF(col("genotypes.calls")) as "homozygotes"
    val countHeterozygotesUDF: UserDefinedFunction = udf { calls: Seq[Seq[Int]] => calls.map(_.sum).count(_ == 1) }
    val heterozygotes: Column = countHeterozygotesUDF(col("genotypes.calls")) as "heterozygotes"

    private val csq: Column = col("INFO_CSQ")
    //Annotations
    val annotations: Column = when(col("splitFromMultiAllelic"), filter(csq, ann => ann.getItem("Allele") === alternate)).otherwise(csq) as "annotations"
    val firstAnn: Column = annotations(0) as "annotation"
    val consequences: Column = col("annotation.Consequence") as "consequences"
    val impact: Column = col("annotation.IMPACT") as "impact"
    val symbol: Column = col("annotation.SYMBOL") as "symbol"
    val feature_type: Column = col("annotation.Feature_type") as "feature_type"
    val ensembl_gene_id: Column = col("annotation.Gene") as "ensembl_gene_id"
    val pubmed: Column = split(col("annotation.PUBMED"), "&") as "pubmed"
    val pick: Column = when(col("annotation.PICK") === "1", lit(true)).otherwise(false) as "pick"
    val canonical: Column = when(col("annotation.CANONICAL") === "YES", lit(true)).otherwise(lit(false)) as "canonical"
    val dbsnp: Column = col("annotation.rs_dbSNP151") as "dbsnp"
    val is_dbsnp: Column = col("INFO_DB") as "is_dbsnp"
    val ensembl_feature_id: Column = col("annotation.Feature") as "ensembl_feature_id"
    val ensembl_transcript_id: Column = when(col("annotation.Feature_type") === "Transcript", col("annotation.Feature")).otherwise(null) as "ensembl_transcript_id"
    val ensembl_regulatory_id: Column = when(col("annotation.Feature_type") === "RegulatoryFeature", col("annotation.Feature")).otherwise(null) as "ensembl_regulatory_id"
    val exon: Column = col("annotation.EXON") as "exon"
    val biotype: Column = col("annotation.BIOTYPE") as "biotype"
    val intron: Column = col("annotation.INTRON") as "intron"
    val hgvsc: Column = col("annotation.HGVSc") as "hgvsc"
    val hgvsp: Column = col("annotation.HGVSp") as "hgvsp"

    val strand: Column = col("annotation.STRAND") as "strand"
    val cds_position: Column = col("annotation.CDS_position") as "cds_position"
    val cdna_position: Column = col("annotation.cDNA_position") as "cdna_position"
    val protein_position: Column = col("annotation.Protein_position") as "protein_position"
    val amino_acids: Column = col("annotation.Amino_acids") as "amino_acids"
    val codons: Column = col("annotation.Codons") as "codons"
    val variant_class: Column = col("annotation.VARIANT_CLASS") as "variant_class"
    val hgvsg: Column = col("annotation.HGVSg") as "hgvsg"
    val is_multi_allelic: Column = col("splitFromMultiAllelic") as "is_multi_allelic"
    val old_multi_allelic: Column = col("INFO_OLD_MULTIALLELIC") as "old_multi_allelic"


    val locus: Seq[Column] = Seq(
      col("chromosome"),
      col("start"),
      col("end"),
      col("reference"),
      col("alternate"))
  }

}
