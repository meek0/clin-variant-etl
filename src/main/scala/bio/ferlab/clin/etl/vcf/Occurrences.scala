package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.clin.etl.vcf.Occurrences.{affected_status, getFamilyRelationships, getOccurrences}
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{familyInfo, fatherAffectedStatus, fatherCalls, motherAffectedStatus, motherCalls, zygosity}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Occurrences(batchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_occurrences")
  val complete_joint_calling: DatasetConf = conf.getDataset("complete_joint_calling")
  val patient: DatasetConf = conf.getDataset("patient")
  val biospecimens: DatasetConf = conf.getDataset("biospecimens")
  val family_relationships: DatasetConf = conf.getDataset("family_relationships")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      //TODO add vcf normalization
      complete_joint_calling.id -> vcf(complete_joint_calling.location, referenceGenomePath = None),
      patient.id -> patient.read,
      biospecimens.id -> biospecimens.read,
      family_relationships.id -> family_relationships.read
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    val patients = data(patient.id)
      .select("patient_id", "is_proband", "affected_status", "gender", "practitioner_id", "organization_id", "study_id")
      .withColumn("affected_status", affected_status)
      .withColumn("gender",
        when(col("gender") === "male", lit("Male"))
          .when(col("gender") === "female", lit("Female"))
          .otherwise(col("gender")))

    val family_relationshipsDf = getFamilyRelationships(data(family_relationships.id))

    val bios = data(biospecimens.id)
      .select("biospecimen_id", "patient_id", "family_id", "sequencing_strategy")

    val biospecimensWithPatient = bios.join(patients, Seq("patient_id"))

    val joinedRelation = biospecimensWithPatient.join(family_relationshipsDf, Seq("patient_id"), "left")

    val occurrences = getOccurrences(data(complete_joint_calling.id), batchId)
    occurrences
      .join(joinedRelation, Seq("biospecimen_id"), "inner")
      .withColumn("participant_id", col("patient_id"))
      .withColumn("family_info", familyInfo)
      .withColumn("mother_calls", motherCalls)
      .withColumn("father_calls", fatherCalls)
      .withColumn("mother_affected_status", motherAffectedStatus)
      .withColumn("father_affected_status", fatherAffectedStatus)
      .drop("family_info", "participant_id")
      .withColumn("zygosity", zygosity(col("calls")))
      .withColumn("mother_zygosity", zygosity(col("mother_calls")))
      .withColumn("father_zygosity", zygosity(col("father_calls")))
      .withParentalOrigin("parental_origin", col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission("transmission", col("father_calls"), col("mother_calls"))
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions(col("chromosome"), col("start"))
    )
  }
}

object Occurrences {
  val affected_status: Column = {
    when(col("affected_status").cast(StringType) === "true", lit(true))
      .otherwise(
        when(col("affected_status") === "affected", lit(true))
          .otherwise(lit(false)))
  }

  def getFamilyRelationships(familyRelationshipsDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    familyRelationshipsDf.where($"patient1_to_patient2_relation".isin("Mother", "Father"))
      .groupBy("patient2")
      .agg(
        map_from_entries(
          collect_list(
            struct(
              $"patient1_to_patient2_relation" as "relation",
              $"patient1" as "patient_id"
            )
          )
        ) as "relations"
      )
      .select(
        $"patient2" as "patient_id",
        $"relations.Mother" as "mother_id",
        $"relations.Father" as "father_id"
      )
  }

  def getOccurrences(inputDf: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    inputDf
      .withColumn("genotype", explode(col("genotypes")))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        concat(lit("SP"), $"genotype.sampleId") as "biospecimen_id", //TODO double check this
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
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_update", current_date())
      .withColumn("variant_type", lit("germline"))
      .drop("annotation")
  }
}
