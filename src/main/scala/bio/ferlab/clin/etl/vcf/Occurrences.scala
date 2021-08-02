package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.vcf.Occurrences.{getFamilyRelationships, getOccurrences}
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Occurrences(batchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_occurrences")
  val complete_joint_calling: DatasetConf = conf.getDataset("complete_joint_calling")
  val patient: DatasetConf = conf.getDataset("patient")
  val biospecimen: DatasetConf = conf.getDataset("biospecimen")
  val group: DatasetConf = conf.getDataset("group")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      //TODO add vcf normalization
      complete_joint_calling.id -> vcf(complete_joint_calling.location, referenceGenomePath = None),
      patient.id -> patient.read,
      biospecimen.id -> biospecimen.read,
      group.id -> group.read
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val groupDf = data(group.id)
      .withColumn("member", explode(col("members")))
      .select(
        col("member.affected_status") as "affected_status",
        col("member.patient_id") as "patient_id"
      )

    val patients = data(patient.id)
      .select(
        col("id") as "patient_id",
        col("family_id"),
        col("is_proband"),
        col("gender"),
        col("practitioner_id"),
        col("organization_id")
      )
      .join(groupDf, Seq("patient_id"), "left")
      .withColumn("gender",
        when(col("gender") === "male", lit("Male"))
          .when(col("gender") === "female", lit("Female"))
          .otherwise(col("gender")))

    val familyRelationshipDf = getFamilyRelationships(data(patient.id))

    val bios = data(biospecimen.id)
      .select("biospecimen_id", "patient_id", "sequencing_strategy")

    val joinedRelation =
      bios
        .join(patients, Seq("patient_id"))
        .join(familyRelationshipDf, Seq("patient_id"), "left")

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

  def getFamilyRelationships(patientDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    patientDf
      .withColumn("fr", explode(col("family_relationship")))
      .select(
        $"id" as "patient1",
        $"fr.patient2" as "patient2",
        $"fr.patient1_to_patient2_relation" as "patient1_to_patient2_relation"
      ).filter($"patient1_to_patient2_relation".isin("MTH", "FTH"))
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
        $"relations.MTH" as "mother_id",
        $"relations.FTH" as "father_id"
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
        firstCsq,
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
