package bio.ferlab.clin.testutils

import bio.ferlab.clin.etl.fhir.FhirRawToNormalizedMappings.practitionerMappings
import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.etl.RawToNormalizedETL

object ClassGeneratorMain extends App with WithSparkSession {

  val output: String = getClass.getClassLoader.getResource(".").getFile

  implicit val conf: Configuration = Configuration(List(StorageConf("clin", output, LOCAL)))

  //val job = new RawToNormalizedETL(Raw.practitioner, Normalized.practitioner, practitionerMappings)
//
  //val df = job.transform(job.extract()).where("id='PR00108'")
//
  //df.show(false)
  //df.printSchema()

  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "OrganizationOutput", df, "src/test/scala/")
  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PartitionerOutput", df, "src/test/scala/")

}
