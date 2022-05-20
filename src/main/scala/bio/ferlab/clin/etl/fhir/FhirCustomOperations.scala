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

    def withPatientExtension: DataFrame = {
      val familyRelationshipType =
        ArrayType(StructType(List(StructField("patient2", StringType), StructField("patient1_to_patient2_relation", StringType))))

      df.where(col("extension").isNull).drop("extension")
        .withColumn("family_id", lit(null).cast(StringType))
        .withColumn("is_fetus", lit(null).cast(BooleanType))
        .withColumn("is_proband", lit(null).cast(BooleanType))
        .withColumn("family_relationship", lit(null).cast(familyRelationshipType))
        .unionByName {
          df.withColumn("extension", explode(col("extension")))
            .withColumn("family_relationship",
              when(col("extension.url").like("%/family-relation"),
                struct(
                  regexp_replace(filter(col("extension")("extension"), c => c("url") === "subject")(0)("valueReference")("reference"), "Patient/", "") as "patient2",
                  filter(col("extension")("extension"), c => c("url") === "relation")(0)("valueCodeableConcept")("coding")(0)("code") as "patient1_to_patient2_relation",
                )))
            .withColumn("family_id",
              when(col("extension.url").like("%/family-id"),
                regexp_replace(col("extension.valueReference.reference"), "Group/", "")))
            .withColumn("is_fetus",
              when(col("extension.url").like("%/is-fetus"), col("extension.valueBoolean")))
            .withColumn("is_proband",
              when(col("extension.url").like("%/is-proband"), col("extension.valueBoolean")))
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max("family_id") as "family_id",
              df.drop("id", INGESTION_TIMESTAMP, "extension").columns.map(c => first(c) as c) :+
                (max("is_proband") as "is_proband") :+
                (max("is_fetus") as "is_fetus") :+
                (collect_list("family_relationship") as "family_relationship"): _*
            )
        }
    }

    def withServiceRequestExtension: DataFrame = {
      df.withColumn("family_extensions", filter(col("extension"), e => e("url") === "http://fhir.cqgc.ferlab.bio/StructureDefinition/family-member"))
        .withColumn("family", aggregate(col("family_extensions"), struct(lit(null).cast("string").as("mother"), lit(null).cast("string").as("father")), (comb, current) => {
          val currentExtension = current("extension")
          val relationship = filter(currentExtension, ext => ext("url") === "parent-relationship")(0)
          val member = filter(currentExtension, ext => ext("url") === "parent")(0)
          when(relationship("valueCodeableConcept")("coding")(0)("code") === "MTH", struct(patientReference(member("valueReference")("reference")) as "mother", comb("father") as "father"))
            .when(relationship("valueCodeableConcept")("coding")(0)("code") === "FTH", struct(comb("mother") as "mother", patientReference(member("valueReference")("reference")) as "father"))
            .otherwise(comb)
        }))
        .drop("family_extensions")
    }

    def withObservationExtension: DataFrame = {
      val hpo_category = "hpo_category"
      val age_at_onset = "age_at_onset"
      df.where(col("extension").isNull)
        .drop("extension")
        .withColumn(age_at_onset, lit(null).cast(StringType))
        .withColumn(hpo_category, lit(null).cast(StringType))
        .unionByName {
          df.withColumn("extension", explode(col("extension")))
            .withColumn(age_at_onset,
              when(col("extension.url").like("%/age-at-onset"), col("extension.valueCoding.code")))
            .withColumn(hpo_category,
              when(col("extension.url").like("%/hpo-category"), col("extension.valueCoding.code")))
            .groupBy("id")
            .agg(
              max(age_at_onset) as age_at_onset,
              df.drop("id", "extension").columns.map(c => first(c) as c) :+
                (max(hpo_category) as hpo_category): _*
            )
        }
    }

    def withNested(sourceColumn: String, destColName: String, valuePath: String, whenUrlExpr: String, urlLikeExpr: String, castInto: DataType): DataFrame = {
      df.filter(col(sourceColumn).isNull)
        .withColumn(destColName, lit(null).cast(castInto))
        .unionByName {
          df
            .withColumn(sourceColumn, explode(col(sourceColumn)))
            .withColumn(destColName,
              when(col(whenUrlExpr).like(urlLikeExpr),
                col(valuePath)))
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max(destColName) as destColName,
              df.drop("id", INGESTION_TIMESTAMP, sourceColumn).columns.map(c => first(c) as c) :+
                (collect_set(col(sourceColumn)) as sourceColumn): _*
            )
        }
    }

    def withExtention(destColName: String, valuePath: String, urlLikeExpr: String, castInto: DataType = StringType): DataFrame = {
      withNested("extension", destColName, valuePath: String, "extension.url", urlLikeExpr, castInto)
    }

    def withPatientNames: DataFrame = {
      //format : "name":[{"family":"Specter","given":["Harvey"]}]
      df.withColumn("name", col("name")(0))
        .withColumn("last_name", col("name.family"))
        .withColumn("first_name", col("name.given")(0))
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    }

    def withMetadata: DataFrame = {
      df.withColumn("version_id", col("meta.versionId"))
        .withColumn("updated_on", to_timestamp(col("meta.lastUpdated"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz"))
        .withColumn("created_on", col("updated_on"))
        .withColumn("profile", col("meta.profile"))
    }
  }

}
