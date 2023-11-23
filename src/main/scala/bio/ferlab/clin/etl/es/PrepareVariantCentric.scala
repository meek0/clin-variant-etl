package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.mainutils.Release
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

case class PrepareVariantCentric(rc: DeprecatedRuntimeETLContext, releaseId: String) extends PrepareCentric(rc, releaseId) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_variant_centric")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val variants = data(enriched_variants.id)
      .drop("transmissions", "transmissions_by_lab", "parental_origins", "parental_origins_by_lab",
        "normalized_variants_oid", "variants_oid", "created_on", "updated_on")
      .withColumn("locus_id_1", col("locus"))
      .as("variants")
      .repartition(100)

    val consequences = data(enriched_consequences.id)
      .drop("normalized_consequences_oid", "consequences_oid", "created_on", "updated_on", "original_canonical", "consequence")
      .withColumn("symbol_id_1", col("symbol"))
      .as("consequences")
      .repartition(100)
    
    joinWithConsequences(variants, consequences)
  }

  private def joinWithConsequences(variantDF: DataFrame,
                                   consequencesDf: DataFrame): DataFrame = {
    import spark.implicits._

    variantDF
      .joinByLocus(consequencesDf, "left")
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences",
        max("impact_score") as "max_impact_score")
      .select($"variant.*", $"consequences", $"max_impact_score")
  }

  private def getUpdate(consequencesDf: DataFrame,
                        variantsDf: DataFrame,
                        lastExecution: Timestamp): DataFrame = {
    val updatedVariants = variantsDf
      .where(col("updated_on") >= lastExecution and col("created_on") =!= col("updated_on"))
      .drop("created_on", "updated_on")

    joinWithConsequences(updatedVariants, consequencesDf)
      .withColumn("frequencies", map(lit("internal"), col("frequencies.internal")))
      .select("chromosome", "start", "reference", "alternate", "donors", "frequencies_by_lab", "frequencies",
        "participant_number")
  }
}

object PrepareVariantCentric {
  @main
  def run(rc: DeprecatedRuntimeETLContext, release: Release): Unit = {
    PrepareVariantCentric(rc, release.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
