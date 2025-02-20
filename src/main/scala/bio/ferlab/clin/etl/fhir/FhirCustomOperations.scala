package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirRawToNormalizedMappings.INGESTION_TIMESTAMP
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object FhirCustomOperations {

  case class REFERENCE(reference: String)

  case class ENTITY(entity: REFERENCE)

  val extractmemberUdf: UserDefinedFunction = udf { entities: Seq[ENTITY] => entities.map(_.entity.reference.replace("Patient/", "")) }

  def extractCodeFromCoding(c: Column, system: String): Column = {
    filter(c("coding"), coding => coding("system") === system)(0)("code")
  }

  val patientReference: Column => Column = patientIdColumn => regexp_replace(patientIdColumn, "Patient/", "")
  val patient_id: Column = patientReference(col("subject.reference"))
  val practitioner_id: Column = regexp_replace(col("assessor.reference"), "Practitioner/", "")
  val organization_id: Column = regexp_replace(col("organization.reference"), "Organization/", "")

  implicit class DataFrameOps(df: DataFrame) {

    def withTelecoms: DataFrame = {
      df.where(col("telecom").isNull).drop("telecom")
        .withColumn("phone_numbers", array())
        .withColumn("email_addresses", array())
        .unionByName {
          df.withColumn("telecom", explode(col("telecom")))
            .withColumn("phone_numbers", when(col("telecom.system") === "phone", col("telecom.value")))
            .withColumn("email_addresses", when(col("telecom.system") === "email", col("telecom.value")))
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              collect_set(col("phone_numbers")) as "phone_numbers",
              df.drop("id", INGESTION_TIMESTAMP, "telecom", "phones").columns.map(c => first(c) as c) :+
                (collect_set(col("email_addresses")) as "email_addresses"): _*
            )
        }
    }

    def extractIdentifier(codeList: List[(String, String)]): DataFrame = {
      val explodedDf = df.withColumn("identifier", explode(col("identifier")))

      val withColumnDf = codeList.foldLeft(explodedDf) { case (d, (code, destColumn)) =>
        d.withColumn(destColumn, when(col("identifier.type.coding.code")(0).isin(code), col("identifier.value")))
      }
      val dfWithIdentifiers = codeList match {
        case head :: Nil =>
          withColumnDf
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max(head._2) as head._2,
              df.drop("id", INGESTION_TIMESTAMP, "identifier").columns.map(c => first(c) as c): _*
            )
        case head :: tail =>
          withColumnDf
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max(head._2) as head._2,
              df.drop("id", INGESTION_TIMESTAMP, "identifier").columns.map(c => first(c) as c) ++
                tail.map(t => max(t._2) as t._2): _*
            )
      }

      val dfWithNulls = codeList.foldLeft(df.where(col("identifier").isNull)) { case (d, (code, destColumn)) =>
        d.withColumn(destColumn, lit(null).cast(StringType))
      }

      dfWithIdentifiers.unionByName(dfWithNulls.drop("identifier"))

    }

    def withTaskExtension: DataFrame = {

      val extensionsLike: String => Column = like => filter(col("extension"), c => c("url").like(like))(0)("extension")
      val workflowExtensions = extensionsLike("%workflow")
      val sequencingExperimentExtensions = extensionsLike("%sequencing-experiment")

      df
        .withColumn("experiment",
          struct(
            filter(sequencingExperimentExtensions, c => c("url") === "experimentalStrategy")(0)("valueCoding")("code") as "sequencing_strategy",
            filter(sequencingExperimentExtensions, c => c("url") === "runName")(0)("valueString") as "name",
            filter(sequencingExperimentExtensions, c => c("url") === "runAlias")(0)("valueString") as "alias",
            filter(sequencingExperimentExtensions, c => c("url") === "platform")(0)("valueString") as "platform",
            filter(sequencingExperimentExtensions, c => c("url") === "captureKit")(0)("valueString") as "capture_kit",
            filter(sequencingExperimentExtensions, c => c("url") === "sequencerId")(0)("valueString") as "sequencer_id",
            filter(sequencingExperimentExtensions, c => c("url") === "labAliquotId")(0)("valueString") as "aliquot_id",
            to_date(filter(sequencingExperimentExtensions, c => c("url") === "runDate")(0)("valueDateTime"), "yyyy-MM-dd") as "run_date"
          ))
        .withColumn("workflow",
          struct(
            filter(workflowExtensions, c => c("url") === "genomeBuild")(0)("valueCoding")("code") as "genome_build",
            filter(workflowExtensions, c => c("url") === "workflowName")(0)("valueString") as "name",
            filter(workflowExtensions, c => c("url") === "workflowVersion")(0)("valueString") as "version"
          ))
    }

    def withFamilyIdentifier: DataFrame = {
      df.withColumn("family_identifier", filter(col("identifier"), i => i("system") === "https://cqgc.qc.ca/family"))
        .withColumn("family_id", col("family_identifier.value")(0))
        .drop("family_identifier")
    }

    def withServiceRequestExtension: DataFrame = {
      df.withFamilyExtensions
        .withFamily
        .drop("family_extensions")
    }

    def withFamilyExtensions: DataFrame = {
      df.withColumn("family_extensions", filter(col("extension"), e => e("url") === "http://fhir.cqgc.ferlab.bio/StructureDefinition/family-member"))
    }

    def withFamily: DataFrame = {
      df.withColumn("family", aggregate(col("family_extensions"), struct(lit(null).cast("string").as("mother"), lit(null).cast("string").as("father")), (comb, current) => {
        val currentExtension = current("extension")
        val relationship = filter(currentExtension, ext => ext("url") === "parent-relationship")(0)
        val member = filter(currentExtension, ext => ext("url") === "parent")(0)
        when(relationship("valueCodeableConcept")("coding")(0)("code") === "MTH", struct(patientReference(member("valueReference")("reference")) as "mother", comb("father") as "father"))
          .when(relationship("valueCodeableConcept")("coding")(0)("code") === "FTH", struct(comb("mother") as "mother", patientReference(member("valueReference")("reference")) as "father"))
          .otherwise(comb)
      }))
    }


    def withFhirMetadata: DataFrame = {
      val metaColumns = df.select("meta.*").columns
      df.withColumn("version_id", col("meta.versionId"))
        .withColumn("updated_on", to_timestamp(col("meta.lastUpdated"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz"))
        .withColumn("created_on", col("updated_on"))
        .withColumn("profile", if (metaColumns.contains("profile")) col("meta.profile") else lit(null).cast(StringType))
    }
  }


}
