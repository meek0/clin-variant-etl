package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{BiospecimenOutput, OccurrenceRawOutput, PatientOutput, VCFInput}
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class OccurrencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  spark.sql("CREATE DATABASE IF NOT EXISTS clin")
  spark.sql("USE clin")

  Seq(PatientOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/patients")
    .saveAsTable("clin.patients")

  Seq(BiospecimenOutput()).toDF.write.format("parquet").mode(SaveMode.Overwrite)
    .option("path", "spark-warehouse/clin.db/biospecimens")
    .saveAsTable("clin.biospecimens")


  "occurrences job" should "transform data in expected format" in {

    val df = Seq(VCFInput()).toDF()

    Occurrences.build(df, "BAT1").as[OccurrenceRawOutput].collect() should contain allElementsOf Seq(
      OccurrenceRawOutput(`last_update` = Date.valueOf(LocalDate.now()))
    )
  }
}
