package bio.ferlab.clin.testutils

import bio.ferlab.clin.etl.fhir.FhirRawToNormalizedMappings.{defaultTransformations, serviceRequestMappings}
import bio.ferlab.datalake.spark3.etl.v3.TransformationsETL
import bio.ferlab.datalake.testutils.{ClassGenerator, DeprecatedTestETLContext, WithSparkSession}

object ClassGeneratorMain extends App with WithSparkSession with WithTestConfig {

  val inputDs = conf.getDataset("raw_service_request")
  val outputDs = conf.getDataset("normalized_service_request")

  val data = Map(inputDs.id ->
    spark.read.json("src/test/resources/raw/landing/fhir/ServiceRequest")

  )
  val job = new TransformationsETL(DeprecatedTestETLContext(), inputDs, outputDs, defaultTransformations ++ serviceRequestMappings)
  val df = job.transformSingle(data)

  //df.show(false)
  //df.printSchema()

  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "OrganizationOutput", df, "src/test/scala/")
  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerOutput", df, "src/test/scala/")

  ClassGenerator.writeClassFile("bio.ferlab.clin.model", "ServiceRequestOutput", df.where("id='SR0095'"), "src/test/scala/")

}
