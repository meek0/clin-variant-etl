package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirCatalog.Raw
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import bio.ferlab.datalake.core.etl.RawToNormalizedETL
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FhirRawToNormalizedMappingsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  val output: String = getClass.getClassLoader.getResource(".").getFile

  implicit val conf: Configuration = Configuration(List(StorageConf("raw", output), StorageConf("normalized", output)))
  import spark.implicits._

  "clinicalImpression raw job" should "return data in the expected format" in {
    val inputDs = Raw.clinicalImpression

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where(col(dst.idName) ==="CI0005").show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ClinicalImpressionOutput", output, "src/test/scala/")

    output.count() shouldBe 7
    val head = output.where(col(dst.idName) ==="CI0005").as[ClinicalImpressionOutput].head()
    head shouldBe ClinicalImpressionOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "group raw job" should "return data in the expected format" in {

    val inputDs = Raw.group

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where(col(dst.idName) ==="13636").show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "GroupOutput", output, "src/test/scala/")

    output.count() shouldBe 5
    val head = output.where(col(dst.idName) ==="13636").as[GroupOutput].head()
    head shouldBe GroupOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "observation raw job" should "return data in the expected format" in {

    val inputDs = Raw.observation

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("observation_id='OB0001'").show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ObservationOutput", output, "src/test/scala/")

    output.count() shouldBe 2
    val head = output.where("observation_id='OB0001'").as[ObservationOutput].head()
    head shouldBe ObservationOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "patient raw job" should "return data in the expected format" in {

    val inputDs = Raw.patient

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("patient_id='17771'").show(false)

    output.count() shouldBe 6
    val head = output.where("patient_id='17771'").as[PatientOutput].head()
    head shouldBe PatientOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "organziation raw job" should "return data in the expected format" in {

    val inputDs = Raw.organization

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("organization_id='OR00207'").show(false)

    output.count() shouldBe 7
    val head = output.where("organization_id='OR00207'").as[OrganizationOutput].head()
    head shouldBe OrganizationOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "practitioner raw job" should "return data in the expected format" in {

    val inputDs = Raw.practitioner

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("practitioner_id='PR00108'").show(false)

    output.count() shouldBe 6
    val head = output.where("practitioner_id='PR00108'").as[PartitionerOutput].head()
    head shouldBe PartitionerOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "practitioner role raw job" should "return data in the expected format" in {

    val inputDs = Raw.practitionerRole

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("practitioner_role_id='PROLE-c4becdcf-87e1-4fa7-ae87-9bbf555b1c4f'").show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerRoleOutput", output, "src/test/scala/")

    output.count() shouldBe 3
    val head = output.where("practitioner_role_id='PROLE-c4becdcf-87e1-4fa7-ae87-9bbf555b1c4f'").as[PartitionerRoleOutput].head()
    head shouldBe PartitionerRoleOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "service request raw job" should "return data in the expected format" in {

    val inputDs = Raw.serviceRequest

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    job.run()

    val output = spark.table(s"${dst.database}.${dst.name}")
    output.where("service_request_id='32130'").show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ServiceRequestOutput", output, "src/test/scala/")

    output.count() shouldBe 3
    val head = output.where("service_request_id='32130'").as[ServiceRequestOutput].head()
    head shouldBe ServiceRequestOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

}
