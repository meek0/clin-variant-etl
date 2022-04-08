package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.etl.varsome.VarsomeUtils.{transformPartition, varsomeSchema}
import bio.ferlab.datalake.commons.config.RunStep.reset
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

object Varsome extends SparkApp {

  implicit val (conf, steps, spark) = init()
  val varsomeToken = spark.conf.get("spark.varsome.token")
  val varsomeUrl = spark.conf.get("spark.varsome.url")
  val chromosome = if (args.length >= 3 && args(2) != "all") {
    Some(args(2))
  } else {
    None
  }
  if (steps.contains(reset)) {
    new Varsome(Reload, varsomeUrl, varsomeToken, chromosome).run()
  } else {
    if (args.length < 4) {
      throw new IllegalArgumentException("Batch id is required if reset is not included in steps")
    }
    val batchId = args(3)
    new Varsome(ForBatch(batchId), varsomeUrl, varsomeToken, chromosome).run()
  }
}


class Varsome(jobType: VarsomeJobType,
              varsomeUrl: String,
              varsomeToken: String,
              chromosome: Option[String] = None)(override implicit val conf: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val variants = normalized_variants.read.select("chromosome", "start", "reference", "alternate", "batch_id", "genes_symbol")
    val panels = normalized_panels.read.select("symbol")
      
    val variantFilterByLength = variants
      .where(length(col("reference")) <= 200 && length(col("alternate")) <= 200) // Varsome limit variant length to 200 bases
    
    val variantsFilterByPanels =  variantFilterByLength.join(panels, array_contains(variantFilterByLength("genes_symbol"), panels("symbol"))) // only variants in panels
      .drop("genes_symbol").drop("symbol")
    
    val variantsFilterByChr = chromosome.map(chr => variantsFilterByPanels.where(col("chromosome") === chr))
      .getOrElse(variantsFilterByPanels)

    jobType match {
      case Reload => Map(normalized_variants.id -> variantsFilterByChr)
      case ForBatch(batchId) =>
        val batchVariants = variantsFilterByChr
          .where(col("batch_id") === batchId)
        val extractedVariants = if (destination.tableExist) {
          val varsome = destination.read.where(col("updated_on") >= Timestamp.valueOf(currentRunDateTime.minusDays(7)))
          batchVariants.join(varsome, Seq("chromosome", "start", "reference", "alternate"), "leftanti")
        }
        else {
          batchVariants
        }
        Map(normalized_variants.id -> extractedVariants)
    }


  }


  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val input = data(normalized_variants.id).persist()
    val numPartitions = (input.count() / 1000) + 1

    input.select(concat_ws("-", locus: _*) as "locus")
      .repartition(numPartitions.toInt)
      .mapPartitions(transformPartition(varsomeUrl, varsomeToken))
      .select(from_json(col("response"), varsomeSchema) as "response")
      .select(explode(col("response")) as "response")
      .select("response.*")
      .withColumn("chromosome", ltrim(col("chromosome"), "chr"))
      .withColumnRenamed("pos", "start")
      .withColumnRenamed("ref", "reference")
      .withColumnRenamed("alt", "alternate")
      .withColumn("updated_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .repartition(25)


  }


}

