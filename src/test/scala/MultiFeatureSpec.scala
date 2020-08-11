import bio.ferlab.clin.testutils.WithSparkHiveSession
import io.projectglow.Glow
import org.apache.spark.sql.functions.col
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class MultiFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkHiveSession with Matchers {

  Feature("Multi") {
    Scenario("Transform vcf with vep") {
      val input = getClass.getResource("/multi.vcf").getFile
      val df = spark.read.format("vcf")
        .option("flattenInfoFields", "true").load(input)
      df.printSchema()
      df.show(false)
      val output = Glow.transform("split_multiallelics", df)
      output.show(false)
    }
  }

  Feature("Multi select") {
    Scenario("Read JSSON") {
      spark.read.json(getClass.getResource("test_json").getFile ).show()
    }
    Scenario("Transform vcf with vep") {
      spark.table("spark_tests.variants").where(col("pubmed").isNotNull).show()

    }
    Scenario("Delete tables") {
      spark.sql("drop table spark_tests.variants")



    }

    Scenario("Create tables") {
      //      import spark.implicits._
      //      val df = Seq(("A", "B")).toDF("col1", "col2")
      //      df.write.format("parquet")
      //        .option("path", s"s3a://spark/test_create")
      //
      //        .saveAsTable("spark_tests.test_create")
      //

            spark.sql("show create table spark_tests.test_create").show(false)
      spark.sql(
        """CREATE TABLE `spark_tests`.`clinvar` (
          |   `chromosome` string,
          |   `start` bigint,
          |   `end` bigint,
          |   `name` string,
          |   `reference` string,
          |   `alternate` string,
          |   `clin_sig_original` array<string>,
          |   `clin_sig_conflict` array<string>,
          |   `clin_sig` array<string>
          |)
          |USING parquet
          |LOCATION 's3a://spark/public/clinvar'
          |""".stripMargin)
      spark.table("spark_tests.clinvar").show(false)
    }
  }


}

