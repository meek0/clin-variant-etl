package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.es.PrepareVariantCentric._
import bio.ferlab.clin.etl.utils.transformation.RenameFieldsInArrayStruct
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

case class PrepareVariantCentric(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_variant_centric")
  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100), sortColumns = Seq("start"))

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minValue,
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
      // To prevent compatibility issues with the frontend, which still expects 'analysis_service_request_id' and 'service_request_id'
      .withDonorsFieldsRenamed(Map(
        "analysis_id"-> "analysis_service_request_id", 
        "sequencing_id" -> "service_request_id")
      )
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
      .withColumn("conseq_exploded", explode(col("consequences")))
      .filter(col("conseq_exploded.picked") === true)
      .select($"variant.*", $"consequences", $"max_impact_score", col("conseq_exploded.symbol").as("gene_symbol_picked_consequence"))
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

  implicit class DataFrameOps(df: DataFrame) {
    def withDonorsFieldsRenamed(renameMap: Map[String, String]): DataFrame = {
      RenameFieldsInArrayStruct("donors", renameMap).transform(df)
    }
  }

  @main
  def run(rc: RuntimeETLContext): Unit = {
    PrepareVariantCentric(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
