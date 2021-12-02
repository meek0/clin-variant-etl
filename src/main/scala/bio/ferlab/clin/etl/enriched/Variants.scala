package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.Variants._
import bio.ferlab.clin.etl.utils.VcfUtils
import bio.ferlab.clin.etl.utils.VcfUtils._
import bio.ferlab.datalake.commons.config.RunType.FIRST_LOAD
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class Variants(chromosome: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_occurrences: DatasetConf = conf.getDataset("normalized_occurrences")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    runType match {
      case FIRST_LOAD =>
        FileSystemResolver.resolve(conf.getStorage(destination.storageid).filesystem).remove(destination.location)
        destination.table.foreach(t => spark.sql(s"DROP TABLE IF EXISTS ${t.fullName}"))
        run(minDateTime, LocalDateTime.now())
      case _ =>
        run(minDateTime, LocalDateTime.now())
    }
  }

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      normalized_variants.id -> normalized_variants.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)).where(s"chromosome='$chromosome'"),
      normalized_occurrences.id -> normalized_occurrences.read.where(s"chromosome='$chromosome'"),
      thousand_genomes.id -> thousand_genomes.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1.read,
      gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1.read,
      gnomad_genomes_3_0.id -> gnomad_genomes_3_0.read,
      gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variants = data(normalized_variants.id)
      .drop("normalized_variants_oid")

    val occurrences = data(normalized_occurrences.id)
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end")
      .as("occurrences")

    //val occurrencesWithAlt = occurrences.where($"has_alt" === true)

    val genomesDf = data(`thousand_genomes`.id)
      .selectLocus($"ac".cast("long"), $"af", $"an".cast("long"))
    val topmed_bravoDf = data(topmed_bravo.id)
      .selectLocus(
        $"ac".cast("long"),
        $"af",
        $"an".cast("long"),
        $"homozygotes".cast("long") as "hom",
        $"heterozygotes".cast("long") as "het")

    val gnomad_genomes_2_1Df = data(gnomad_genomes_2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_exomes_2_1Df = data(gnomad_exomes_2_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_3_0Df = data(gnomad_genomes_3_0.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
    val gnomad_genomes_3_1_1Df = data(gnomad_genomes_3_1_1.id).selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"nhomalt".cast("long") as "hom")


    //val joinWithTransmissions = variantsWithAggregate("transmission", variants, occurrencesWithAlt)
    //val joinWithParentalOrigin = variantsWithAggregate("parental_origin", joinWithTransmissions, occurrencesWithAlt)
    val joinWithFrequencies = variantsWithFrequencies(variants, occurrences)
    val joinWithPop = joinWithPopulations(joinWithFrequencies, genomesDf, topmed_bravoDf, gnomad_genomes_2_1Df, gnomad_exomes_2_1Df, gnomad_genomes_3_0Df, gnomad_genomes_3_1_1Df)
    val joinDbSNP = joinWithDbSNP(joinWithPop, data(dbsnp.id))
    val joinClinvar = joinWithClinvar(joinDbSNP, data(clinvar.id))
    val joinGenes = joinWithGenes(joinClinvar, data(genes.id))
    joinGenes
      .withGeneExternalReference
      .withVariantExternalReference
      .withColumn("locus", concat_ws("-", locus:_*))
      .withColumn("hash", sha1(col("locus")))
      .withColumn(destination.oid, col("created_on"))
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }

  def variantsWithAggregate(aggregate: String, variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val aggregateVariantLevel =
      occurrences
        .groupBy(locus :+ col(aggregate): _*)
        .agg(count(aggregate) as s"${aggregate}_count")
        .groupBy(locus : _*)
        .agg(
          map_from_entries(filter(collect_list(struct(col(aggregate), col(s"${aggregate}_count"))), c => c(aggregate).isNotNull)) as s"${aggregate}s",
        )
    //val aggregateByLab =
    //  occurrences
    //    .groupBy(locus :+ col(aggregate) :+ col("organization_id"): _*)
    //    .agg(count(aggregate) as s"${aggregate}_count_by_lab")
    //    .groupBy(locus :+ col("organization_id"): _*)
    //    .agg(
    //      map_from_entries(filter(collect_list(struct(col(aggregate), col(s"${aggregate}_count_by_lab"))), c => c(aggregate).isNotNull)) as s"${aggregate}s_by_lab",
    //    )
    //    .groupBy(locus : _*)
    //    .agg(map_from_entries(collect_list(struct(col("organization_id"), col(s"${aggregate}s_by_lab")))) as s"${aggregate}s_by_lab")
    variants
      .joinByLocus(aggregateVariantLevel, "left")
      //.joinByLocus(aggregateByLab, "left")
  }

  def variantsWithFrequencies(variants: DataFrame, occurrences: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val emptyFrequency =
      struct(
        lit(0) as "ac",
        lit(0) as "an",
        lit(0.0) as "af",
        lit(0) as "pc",
        lit(0) as "pn",
        lit(0.0) as "pf",
        lit(0) as "hom"
      )

    val frequency: String =>  Column = {
      case "" =>
        struct(
          $"ac",
          $"an",
          coalesce($"ac" / $"an", lit(0.0)) as "af",
          $"pc",
          $"pn",
          coalesce($"pc" / $"pn", lit(0.0)) as "pf",
          $"hom"
        )
      case prefix: String =>
        struct(
          col(s"${prefix}_ac") as "ac",
          col(s"${prefix}_an") as "an",
          coalesce(col(s"${prefix}_ac") / col(s"${prefix}_an"), lit(0.0)) as "af",
          col(s"${prefix}_pc") as "pc",
          col(s"${prefix}_pn") as "pn",
          coalesce(col(s"${prefix}_pc") / col(s"${prefix}_pn"), lit(0.0)) as "pf",
          col(s"${prefix}_hom") as "hom"
        )
    }

    variants
      .joinByLocus(occurrences.filter(col("filters") === Array("PASS")), "inner")
      .withColumn("affected_status_str", when(col("affected_status"), lit("affected")).otherwise("non_affected"))
      .groupBy(locus :+ col("analysis_code"):+ col("affected_status_str"): _*)
      .agg(
        first($"affected_status") as "affected_status",
        ac,
        an,
        het,
        hom,
        pc,
        pn,
        first(struct(variants("*"), $"variant_type")) as "variant",
        filter(collect_list(struct("occurrences.*")), c => c("zygosity").isin("HOM", "HET")) as "donors")
      .withColumn("frequency_by_status", frequency(""))
      .groupBy(locus :+ col("analysis_code"): _*)
      .agg(
        map_from_entries(collect_list(struct($"affected_status_str", $"frequency_by_status"))) as "frequency_by_status",

        sum(when($"affected_status", $"ac").otherwise(0)) as "affected_ac",
        sum(when($"affected_status", $"an").otherwise(0)) as "affected_an",
        sum(when($"affected_status", $"pc").otherwise(0)) as "affected_pc",
        sum(when($"affected_status", $"pn").otherwise(0)) as "affected_pn",
        sum(when($"affected_status", $"hom").otherwise(0)) as "affected_hom",

        sum(when($"affected_status", 0).otherwise($"ac")) as "non_affected_ac",
        sum(when($"affected_status", 0).otherwise($"an")) as "non_affected_an",
        sum(when($"affected_status", 0).otherwise($"pc")) as "non_affected_pc",
        sum(when($"affected_status", 0).otherwise($"pn")) as "non_affected_pn",
        sum(when($"affected_status", 0).otherwise($"hom")) as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first(col("variant")) as "variant",
        flatten(collect_list(col("donors"))) as "donors",
      )
      .withColumn("frequency_by_status_total", map_from_entries(array(struct(lit("total"), frequency("")))))
      .withColumn("frequency_by_status", map_concat($"frequency_by_status_total", $"frequency_by_status"))
      .withColumn("frequency_by_status", when(array_contains(map_keys($"frequency_by_status"), "non_affected"), $"frequency_by_status")
        .otherwise(map_concat($"frequency_by_status", map_from_entries(array(struct(lit("non_affected"), emptyFrequency))))))
      .groupBy(locus: _*)
      .agg(
        collect_list(struct(
          $"analysis_code" as "analysis_code",
          col("frequency_by_status")("affected") as "affected",
          col("frequency_by_status")("non_affected") as "non_affected",
          col("frequency_by_status")("total") as "total"
        )) as "frequencies_by_analysis",
        sum($"affected_ac") as "affected_ac",
        sum($"affected_an") as "affected_an",
        sum($"affected_pc") as "affected_pc",
        sum($"affected_pn") as "affected_pn",
        sum($"affected_hom") as "affected_hom",

        sum($"non_affected_ac") as "non_affected_ac",
        sum($"non_affected_an") as "non_affected_an",
        sum($"non_affected_pc") as "non_affected_pc",
        sum($"non_affected_pn") as "non_affected_pn",
        sum($"non_affected_hom") as "non_affected_hom",

        sum($"ac") as "ac",
        sum($"an") as "an",
        sum($"pc") as "pc",
        sum($"pn") as "pn",
        sum($"hom") as "hom",
        first(col("variant")) as "variant",
        flatten(collect_list(col("donors"))) as "donors",
      )
      .withColumn("frequency_RQDM", struct(
        frequency("affected") as "affected",
        frequency("non_affected") as "non_affected",
        frequency("") as "total"
      ))
      .drop("ac", "an", "pc", "pn", "hom")
      .drop("non_affected_ac", "non_affected_an", "non_affected_pc", "non_affected_pn", "non_affected_hom")
      .drop("affected_ac", "affected_an", "affected_pc", "affected_pn", "affected_hom")
      .select($"variant.*",
        $"donors",
        $"frequencies_by_analysis",
        $"frequency_RQDM"
      )
      .withColumn("assembly_version", lit("GRCh38"))
      .withColumn("last_annotation_update", current_date())
      .withColumn("dna_change", concat_ws(">", $"reference", $"alternate"))
  }

  def joinWithPopulations(variants: DataFrame,
                          genomesDf: DataFrame,
                          topmed_bravoDf: DataFrame,
                          gnomad_genomes_2_1Df: DataFrame,
                          gnomad_exomes_2_1Df: DataFrame,
                          gnomad_genomes_3_0Df: DataFrame,
                          gnomad_genomes_3_1_1Df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    broadcast(variants)
      .joinAndMerge(genomesDf, "thousand_genomes", "left")
      .joinAndMerge(topmed_bravoDf, "topmed_bravo", "left")
      .joinAndMerge(gnomad_genomes_2_1Df, "gnomad_genomes_2_1_1", "left")
      .joinAndMerge(gnomad_exomes_2_1Df, "gnomad_exomes_2_1_1", "left")
      .joinAndMerge(gnomad_genomes_3_0Df, "gnomad_genomes_3_0", "left")
      .joinAndMerge(gnomad_genomes_3_1_1Df, "gnomad_genomes_3_1_1", "left")
      .select(variants("*"),
        struct(
          col("thousand_genomes"),
          col("topmed_bravo"),
          col("gnomad_genomes_2_1_1"),
          col("gnomad_exomes_2_1_1"),
          col("gnomad_genomes_3_0"),
          col("gnomad_genomes_3_1_1")) as "external_frequencies")
  }

  def joinWithDbSNP(variants: DataFrame, dbsnp: DataFrame)(implicit spark: SparkSession): DataFrame = {
    variants
      .joinByLocus(dbsnp, "left")
      .select(variants.drop("name")("*"), dbsnp("name") as "rsnumber")
  }

  def joinWithClinvar(variants: DataFrame, clinvar: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    variants
      .joinAndMerge(
        clinvar.selectLocus($"name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations"),
        "clinvar",
        "left")
  }

  def joinWithGenes(variants: DataFrame, genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    variants
      .join(genes, variants("chromosome") === genes("chromosome") && array_contains(variants("genes_symbol"), genes("symbol")), "left")
      .drop(genes("chromosome"))
      .groupByLocus()
      .agg(
        first(struct(variants("*"))) as "variant",
        collect_list(struct(genes.drop("chromosome")("*"))) as "genes",
        flatten(collect_set(genes("omim.omim_id"))) as "omim"
      )
      .select("variant.*", "genes", "omim")
  }
}

object Variants {
  implicit class DataFrameOps(df: DataFrame) {
    def withGeneExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "gene_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        exists($"genes", gene => gene("orphanet").isNotNull and size(gene("orphanet")) > 0) -> "Orphanet",
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when(exists($"genes", gene => gene("hpo").isNotNull and size(gene("hpo")) > 0), array(lit("HPO"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }

    def withVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "variant_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        $"clinvar".isNotNull -> "Clinvar",
        $"pubmed".isNotNull -> "Pubmed"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }

  }
}

