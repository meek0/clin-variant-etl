package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirCatalog.Raw
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import bio.ferlab.datalake.core.etl.RawToNormalizedETL
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
    val input = job.extract()

    val output = job.transform(input).where("id='CI0005'")
    output.show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ClinicalImpressionOutput", output, "src/test/scala/")

    val head = output.as[ClinicalImpressionOutput].head()
    head shouldBe ClinicalImpressionOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

  "group raw job" should "return data in the expected format" in {

    val inputDs = Raw.group

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='13636'")
    output.show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "GroupOutput", output, "src/test/scala/")

    val head = output.as[GroupOutput].head()
    head shouldBe GroupOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

  "observation raw job" should "return data in the expected format" in {

    val inputDs = Raw.observation

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='OB0001'")
    output.show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ObservationOutput", output, "src/test/scala/")

    val head = output.as[ObservationOutput].head()
    head shouldBe ObservationOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

  "patient raw job" should "return data in the expected format" in {

    val inputDs = Raw.patient

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='17771'")
    output.show(false)

    val head = output.as[PatientOutput].head()
    head shouldBe PatientOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)
  }

  "organziation raw job" should "return data in the expected format" in {

    val inputDs = Raw.organization

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='OR00207'")
    output.show(false)

    val head = output.as[OrganizationOutput].head()
    head shouldBe OrganizationOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)
  }

  "practitioner raw job" should "return data in the expected format" in {

    val inputDs = Raw.practitioner

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='PR00108'")
    output.show(false)

    val head = output.as[PartitionerOutput].head()
    head shouldBe PartitionerOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

  "practitioner role raw job" should "return data in the expected format" in {

    val inputDs = Raw.practitionerRole

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='PROLE-c4becdcf-87e1-4fa7-ae87-9bbf555b1c4f'")
    output.show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerRoleOutput", output, "src/test/scala/")

    val head = output.as[PartitionerRoleOutput].head()
    head shouldBe PartitionerRoleOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

  "service request raw job" should "return data in the expected format" in {

    val inputDs = Raw.serviceRequest

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new RawToNormalizedETL(src, dst, mapping)
    val input = job.extract()

    val output = job.transform(input).where("id='32130'")
    output.show(false)

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ServiceRequestOutput", output, "src/test/scala/")

    //input(inputDs).dropDuplicates("id").count() shouldBe output.count()
    val head = output.as[ServiceRequestOutput].head()
    head shouldBe ServiceRequestOutput()
      .copy(`ingestionFileName` = head.`ingestionFileName`, `ingestedOn` = head.`ingestedOn`,
        `updatedOn` = head.`updatedOn`, `createdOn` = head.`createdOn`)

  }

}
