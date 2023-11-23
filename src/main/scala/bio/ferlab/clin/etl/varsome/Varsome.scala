package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.etl.mainutils.{Chromosome, OptionalBatch}
import bio.ferlab.clin.etl.varsome.VarsomeUtils.{transformPartition, varsomeSchema}
import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RunStep, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

object Varsome {
  @main
  def run(rc: DeprecatedRuntimeETLContext, chromosome: Chromosome, batch: OptionalBatch): Unit = {
    val varsomeToken = rc.spark.conf.get("spark.varsome.token")
    val varsomeUrl = rc.spark.conf.get("spark.varsome.url")
    val jobType = if (rc.runSteps.contains(RunStep.reset)) Reload else batch.id match {
      case Some(id) => ForBatch(id)
      case None => throw new IllegalArgumentException("Batch id is required if reset is not included in steps")
    }
    Varsome(rc, jobType, varsomeUrl, varsomeToken, chromosome.name).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}


case class Varsome(rc: DeprecatedRuntimeETLContext,
                   jobType: VarsomeJobType,
                   varsomeUrl: String,
                   varsomeToken: String,
                   chromosome: Option[String] = None) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    val variants = normalized_variants.read.select("chromosome", "start", "reference", "alternate", "batch_id", "genes_symbol")
    val panels = normalized_panels.read.select("symbol")

    val variantFilterByLength = variants
      .where(length(col("reference")) <= 200 && length(col("alternate")) <= 200) // Varsome limit variant length to 200 bases

    val variantsFilterByPanels = variantFilterByLength.join(panels, array_contains(variantFilterByLength("genes_symbol"), panels("symbol")), "left_semi") // only variants in panels
      .drop("genes_symbol")

    val variantsFilterByChr = chromosome.map(chr => variantsFilterByPanels.where(col("chromosome") === chr))
      .getOrElse(variantsFilterByPanels)

    jobType match {
      case Reload => Map(normalized_variants.id -> variantsFilterByChr)
      case ForBatch(batchId) =>
        val batchVariants = variantsFilterByChr
          .where(col("batch_id") === batchId)
        val extractedVariants = if (mainDestination.tableExist) {
          val varsome = mainDestination.read.where(col("updated_on") >= Timestamp.valueOf(currentRunDateTime.minusDays(7)))
          batchVariants.join(varsome, Seq("chromosome", "start", "reference", "alternate"), "leftanti")
        }
        else {
          batchVariants
        }
        Map(normalized_variants.id -> extractedVariants)
    }


  }

  override def defaultRepartition: DataFrame => DataFrame = Coalesce(1)

  override def transformSingle(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): DataFrame = {
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


  }


}

