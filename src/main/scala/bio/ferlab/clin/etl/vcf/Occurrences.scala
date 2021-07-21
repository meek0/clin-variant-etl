package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.SparkUtils.vcf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Occurrences(batchId: String)(implicit configuration: Configuration) extends ETL {

  val OCCURRENCES_TABLE = "occurrences"

  override val destination: DatasetConf = conf.getDataset("normalized_occurrences")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      //TODO add vcf normalization
      "complete_joint_calling" -> vcf(conf.getDataset("complete_joint_calling").location, referenceGenomePath = None),
      "patient" -> spark.table("clin.patient"),
      "biospecimens" -> spark.table("clin.biospecimens")
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val inputDf = data("complete_joint_calling")
    val patients = data("patient")
    val biospecimens = data("biospecimens")

    val occurrences = inputDf
      .withColumn("genotype", explode(col("genotypes")))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        concat(lit("SP"), $"genotype.sampleId") as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        $"INFO_QD" as "qd",
        array_contains($"genotype.calls", 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic
      )
      .withColumn("ad_ref", $"ad"(0))
      .withColumn("ad_alt", $"ad"(1))
      .withColumn("ad_total", $"ad_ref" + $"ad_alt")
      .withColumn("ad_ratio", when($"ad_total" === 0, 0).otherwise($"ad_alt" / $"ad_total"))
      .drop("ad")
      .withColumn("zygosity", zygosity)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_update", current_date())
      .withColumn("variant_type", lit("germline"))
      .drop("annotation")

    val biospecimensWithPatient = broadcast(
      biospecimens
        .join(patients, Seq("patient_id"))
        .select($"biospecimen_id", $"patient_id", $"family_id", $"practitioner_id", $"organization_id", $"sequencing_strategy", $"study_id")
    )
    occurrences.join(biospecimensWithPatient, Seq("biospecimen_id"), "inner")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    DeltaUtils.insert(
      data,
      Some(destination.location),
      "clin",
      OCCURRENCES_TABLE,
      {
        _.repartition(1, col("chromosome"))
          .sortWithinPartitions(col("chromosome"), col("start"))
      },
      Seq("chromosome")
    )
    data
  }
}
