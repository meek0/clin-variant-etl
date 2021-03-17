package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirRawToNormalizedMappings.INGESTION_TIMESTAMP
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame}

object FhirCustomOperations {

  case class REFERENCE(reference: String)
  case class ENTITY(entity: REFERENCE)
  val extractmemberUdf: UserDefinedFunction = udf { entities: Seq[ENTITY] => entities.map(_.entity.reference.replace("Patient/", "")) }

  val patientId: Column = regexp_replace(col("subject.reference"), "Patient/", "")
  val practitionerId: Column = regexp_replace(col("assessor.reference"), "Practitioner/", "")
  val organizationId: Column = regexp_replace(col("organization.reference"), "Organization/", "")

  implicit class DataFrameOps(df: DataFrame) {

    def withTelecoms: DataFrame = {
      df.where(col("telecom").isNull).drop("telecom")
        .withColumn("phones", array(struct(
          lit(null).cast(StringType) as "phoneNumber",
          lit(null).cast(LongType) as "rank")))
        .withColumn("emailAddresses", array())
        .unionByName {
          df.withColumn("telecom", explode(col("telecom")))
            .withColumn("phones", when(col("telecom.system") === "phone",
              struct(
                col("telecom.value") as "phoneNumber",
                col("telecom.rank") as "rank")
            ))
            .withColumn("emailAddresses", when(col("telecom.system") === "email", col("telecom.value")))
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              collect_set(col("phones")) as "phones",
              df.drop("id", INGESTION_TIMESTAMP, "telecom").columns.map(c => first(c) as c):+
                (collect_set(col("emailAddresses")) as "emailAddresses"):_*
            )
        }
    }

    def extractIdentifier(codeList: List[(String, String)]): DataFrame = {
      val explodedDf = df.withColumn("identifier", explode(col("identifier")))

      val withColumnDf = codeList.foldLeft(explodedDf){ case (d, (code, destColumn)) =>
        d.withColumn(destColumn, when(col("identifier.type.coding.code")(0).isin(code), col("identifier.value")))
      }
      val dfWithIdentifiers = codeList match {
        case head::Nil =>
          withColumnDf
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max(head._2) as head._2,
              df.drop("id", INGESTION_TIMESTAMP, "identifier").columns.map(c => first(c) as c):_*
            )
        case head::tail =>
          withColumnDf
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max(head._2) as head._2,
              df.drop("id", INGESTION_TIMESTAMP, "identifier").columns.map(c => first(c) as c) ++
                tail.map(t => max(t._2) as t._2):_*
            )
      }

      val dfWithNulls = codeList.foldLeft(df.where(col("identifier").isNull)){ case (d, (code, destColumn)) =>
        d.withColumn(destColumn, lit(null).cast(StringType))
      }

      dfWithIdentifiers.unionByName(dfWithNulls.drop("identifier"))

    }

    def withPatientExtension: DataFrame = {
      df.where(col("extension").isNull).drop("extension")
        .withColumn("family-id", lit(null).cast(StringType))
        .withColumn("is-fetus", lit(null).cast(BooleanType))
        .withColumn("is-proband", lit(null).cast(BooleanType))
        .unionByName {
          df.withColumn("extension", explode(col("extension")))
            .withColumn("family-id",
              when(col("extension.url").like("%/family-id"),
                regexp_replace(col("extension.valueReference.reference"), "Group/", "")))
            .withColumn("is-fetus",
              when(col("extension.url").like("%/is-fetus"), col("extension.valueBoolean")))
            .withColumn("is-proband",
              when(col("extension.url").like("%/is-proband"), col("extension.valueBoolean")))
            .groupBy("id", INGESTION_TIMESTAMP)
            .agg(
              max("family-id") as "family-id",
              df.drop("id", INGESTION_TIMESTAMP, "extension").columns.map(c => first(c) as c):+
                (max("is-proband") as "is-proband"):+
                (max("is-fetus") as "is-fetus"):_*
            )
        }
    }

    def withServiceRequestExtension: DataFrame = {
      df.where(col("extension").isNull)
        .drop("extension")
        .withColumn("ref-clin-impression", lit(null).cast(StringType))
        .withColumn("is-submitted", lit(null).cast(BooleanType))
        .unionByName {
          df.withColumn("extension", explode(col("extension")))
            .withColumn("ref-clin-impression",
              when(col("extension.url").like("%/ref-clin-impression"),
                regexp_replace(col("extension.valueReference.reference"), "ClinicalImpression/", "")))
            .withColumn("is-submitted",
              when(col("extension.url").like("%/is-submitted"), col("extension.valueBoolean")))
            .groupBy("id")
            .agg(
              max("ref-clin-impression") as "ref-clin-impression",
              df.drop("id", "extension").columns.map(c => first(c) as c):+
                (max("is-submitted") as "is-submitted"):_*
            )
        }
    }

    def withObservationExtension: DataFrame = {
      df.where(col("extension").isNull)
        .drop("extension")
        .withColumn("age-at-onset", lit(null).cast(StringType))
        .withColumn("hpo-category", lit(null).cast(StringType))
        .unionByName {
          df.withColumn("extension", explode(col("extension")))
            .withColumn("age-at-onset",
              when(col("extension.url").like("%/age-at-onset"), col("extension.valueCoding.code")))
            .withColumn("hpo-category",
              when(col("extension.url").like("%/hpo-category"), col("extension.valueCoding.code")))
            .groupBy("id")
            .agg(
              max("age-at-onset") as "age-at-onset",
              df.drop("id", "extension").columns.map(c => first(c) as c):+
                (max("hpo-category") as "hpo-category"):_*
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
              df.drop("id", INGESTION_TIMESTAMP, sourceColumn).columns.map(c => first(c) as c):+
                (collect_set(col(sourceColumn)) as sourceColumn):_*
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
      df.withColumn("versionId", col("meta.versionId"))
        .withColumn("updatedOn", to_timestamp(col("meta.lastUpdated"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz"))
        .withColumn("createdOn", col("updatedOn"))
    }
  }

}
