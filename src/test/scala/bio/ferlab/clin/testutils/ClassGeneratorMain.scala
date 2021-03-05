package bio.ferlab.clin.testutils

import bio.ferlab.datalake.core.config.Configuration

object ClassGeneratorMain extends App with WithSparkSession {

  val df = spark.read.json(this.getClass.getClassLoader.getResource("raw/landing/Organization").getFile)

  df.show(false)
  df.printSchema()

  //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "OrganizationInput",)

}
