package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Random

object FhirMetadataSpec extends App with WithSparkSession {

  val output: String = getClass.getClassLoader.getResource(".").getFile

  implicit val conf: Configuration = Configuration(List(StorageConf("raw", output, LOCAL), StorageConf("normalized", output, LOCAL)))
  import spark.implicits._

    val orgDs = conf.getDataset("normalized_organization")
    val orgsLdm: List[String] = spark.read.format("delta")
      .load(orgDs.location)
      .withColumn("alias", explode(col("alias")))
      .select("alias").as[String].collect().toList.distinct

    println(orgsLdm)

    val bodySites: Map[Int, Int] = List(107008, 108003, 110001, 111002, 116007, 124002, 149003, 155008, 167005, 202009,
      205006, 206007, 221001, 227002, 233006, 235004, 246001, 247005, 251007, 256002, 263002, 266005, 272005, 273000, 283001, 284007,
      289002, 301000, 311007, 315003, 318001, 344001, 345000, 356000, 393006, 402006, 405008, 414003, 420002, 422005, 446003, 457008,
      461002, 464005, 465006)
      .zipWithIndex.map {case (v, i) => i -> v }.toMap

    val randomBodySite = udf(() => bodySites(Random.nextInt(bodySites.size)))
    spark.udf.register("randomBodySite", randomBodySite)

    val ldms: Map[Int, String] = orgsLdm
      .zipWithIndex.map {case (v, i) => i -> v }.toMap

    val randomLDM = udf(() => ldms(Random.nextInt(ldms.size)))
    spark.udf.register("randomLDM", randomLDM)

    val biospecimens = spark.read.option("header", "true").csv(s"$output/raw/landing/fhir/Biospecimen")

    val patientDs = conf.getDataset("normalized_patient")
    val patientDf =
      spark.read.format("delta").load(patientDs.location)
        .withColumnRenamed("id", "patient_id")

    val sr = conf.getDataset("normalized_service_request")
    val serviceRequestDf =
      spark.read.format("delta").load(sr.location)
        .withColumnRenamed("id", "service_request_id")

    val experiment =
      struct(
        lit("Illumina") as "platform",
        lit("NB552318") as "sequencerId",
        lit("runNameExample") as "runName",
        lit("2014-09-21T11:50:23-05") as "runDate",
        lit("runAliasExample") as "runAlias",
        lit("0") as "flowcellId",
        lit(true) as "isPairedEnd",
        lit(100) as "fragmentSize",
        lit("WXS") as "experimentalStrategy",
        lit("RocheKapaHyperExome") as "captureKit",
        lit("KAPA_HyperExome_hg38_capture_targets") as "baitDefinition"
      )

    val workflow = struct(
      lit("Dragen") as "name",
      lit("1.1.0") as "version",
      lit("GRCh38") as "genomeBuild"
    )

    val df =
      patientDf
        .join(biospecimens, Seq("patient_id"))
        .join(serviceRequestDf, Seq("patient_id"))
        .groupBy("patient_id")
        .agg(
          first(col("service_request_id")) as "service_request_id",
          first(col("biospecimen_id")) as "sample_id",
          first(col("first_name")) as "first_name",
          first(col("last_name")) as "last_name",
          first(col("gender")) as "gender"
        )
        .withColumn("submissionSchema", lit("CQGC_Germline"))
        .withColumn("experiment", experiment)
        .withColumn("workflow", workflow)
        .withColumn("bodySite", randomBodySite())
        .withColumn("ldm", randomLDM())
        .withColumn("id", row_number().over(Window.partitionBy("experiment", "workflow", "submissionSchema").orderBy("patient_id")))

    val finalDf =
      df.groupBy("experiment", "workflow", "submissionSchema")
        .agg(collect_list(
          struct(
            struct(
              col("patient_id") as "id",
              col("first_name") as "firstName",
              col("last_name") as "lastName",
              col("gender") as "sex"
            ) as "patient",
            col("ldm") as "ldm",
            col("sample_id") as "sampleId",
            concat(lit("SN"), substring(col("sample_id"),3, 10)) as "specimenId",
            lit("NBL") as "specimenType",
            col("bodySite") as "bodySite",
            col("service_request_id") as "serviceRequestId",
            lit("nanuq_sample_id") as "labAliquotId",
            struct(
              concat(lit("file"), col("id"), lit("_GRCh38.cram")) as "cram",
              concat(lit("file"), col("id"), lit("_GRCh38.crai")) as "crai",
              concat(lit("file"), col("id"), lit("_GRCh38.vcf")) as "vcf",
              concat(lit("file"), col("id"), lit("_GRCh38.tbi")) as "tbi",
              concat(lit("file"), col("id"), lit("_GRCh38.json")) as "json"
            ) as "files"
          ),
        ) as "analyses")

    finalDf.show(2)
    finalDf
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$output/analyses")

}
