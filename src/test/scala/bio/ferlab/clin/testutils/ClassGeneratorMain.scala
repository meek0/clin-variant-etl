package bio.ferlab.clin.testutils

import bio.ferlab.clin.etl.fhir.FhirRawToNormalizedMappings.{defaultTransformations, serviceRequestMappings}
import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.etl.RawToNormalizedETL
import bio.ferlab.datalake.spark3.utils.ClassGenerator

object ClassGeneratorMain extends App with WithSparkSession {

  val output: String = getClass.getClassLoader.getResource(".").getFile

  implicit val conf: Configuration = Configuration(List(StorageConf("clin", output, LOCAL)))
  val inputDs = conf.getDataset("raw_service_request")
  val outputDs = conf.getDataset("normalized_service_request")

  val data = Map(inputDs.id ->
    spark.read.json("src/test/resources/raw/landing/fhir/ServiceRequest")

  )
  val job = new RawToNormalizedETL(inputDs, outputDs , defaultTransformations ++ serviceRequestMappings)
  val df = job.transform(data)
//
  //df.show(false)
  //df.printSchema()

  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "OrganizationOutput", df, "src/test/scala/")
  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerOutput", df, "src/test/scala/")

  ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ServiceRequestOutput", df.where("id='SR0095'"), "src/test/scala/")

}
