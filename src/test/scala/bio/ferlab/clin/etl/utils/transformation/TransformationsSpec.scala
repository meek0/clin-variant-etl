package bio.ferlab.clin.etl.utils.transformation

import bio.ferlab.clin.model.enriched.EnrichedClinical
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}

class TransformationsSpec extends SparkSpec {

  import spark.implicits._

  "RenameFieldsInArrayStruct" should "rename specified fields within each struct of the array column" in {
    val df = testRenameFieldsInArrayStructDf()
    val job = RenameFieldsInArrayStruct("languages", Map("language" -> "lang", "level" -> "proficiency"))
    val resultDf = job.transform(df)

    resultDf.collect() should contain theSameElementsAs df.collect()
    resultDf.schema shouldBe StructType(List(
      StructField("name", StringType),
      StructField("languages", ArrayType(
        StructType(List(StructField("lang", StringType), StructField("proficiency", StringType)))
      ))
    ))
  }

  "RenameFieldsInArrayStruct" should "leave the DataFrame unchanged if the rename map is empty" in {
    val df = testRenameFieldsInArrayStructDf()
    val job = RenameFieldsInArrayStruct("languages", Map())
    val resultDf = job.transform(df)

    resultDf.collect() should contain theSameElementsAs df.collect()
    resultDf.schema shouldBe df.schema
  }

  "RenameFieldsInArrayStruct" should "fail if the specified array column does not exist" in {
    val df = testRenameFieldsInArrayStructDf()
    val job = RenameFieldsInArrayStruct("FOO", Map("language" -> "lang", "level" -> "proficiency"))

    an[AnalysisException] should be thrownBy job.transform(df)
  }

  "RenameFieldsInArrayStruct" should "fail if a field to rename does not exist in the struct" in {
    val df = testRenameFieldsInArrayStructDf()
    val job = RenameFieldsInArrayStruct("languages", Map("FOO" -> "lang", "level" -> "proficiency"))

    an[AnalysisException] should be thrownBy job.transform(df)
  }

  "RenameFieldsInArrayStruct" should "fail if the target column is not an array of structs" in {
    val df = testRenameFieldsInArrayStructDf().select(col("name"), explode(col("languages")) as "languages")

    val job = RenameFieldsInArrayStruct("languages", Map("language" -> "lang", "level" -> "proficiency"))

    an[AnalysisException] should be thrownBy job.transform(df)
  }

  "EnrichWithClinicalInfo" should "enrich a DataFrame with clinical information" in {
    val df = Seq(
      ("aliquot1", "1"),
      ("aliquot2", "2")
    ).toDF("aliquot_id", "x")

    val clinicalDf = Seq(
      EnrichedClinical(aliquot_id = "aliquot1", analysis_id = "analysis1"),
      EnrichedClinical(aliquot_id = "aliquot2", analysis_id = "analysis2"),
      EnrichedClinical(aliquot_id = "aliquot3", analysis_id = "analysis3"),
      EnrichedClinical(aliquot_id = "aliquot1", analysis_id = "analysis1") // duplicate for testing
    ).toDF()

    val job = EnrichWithClinicalInfo(clinicalDf, Seq("aliquot_id"), Seq("analysis_id"))

    val resultDf = job.transform(df)
    resultDf.collect() should contain theSameElementsAs Seq(
      Row("aliquot1", "1", "analysis1"),
      Row("aliquot2", "2", "analysis2")
    )
  }

  private def testRenameFieldsInArrayStructDf(): DataFrame = {
    val data = Seq(
      Row("Alice", Seq(Row("french", "excellent"), Row("english", "good"))),
      Row("Bob", Seq(Row("french", "good"), Row("english", "excellent")))
    )
    val inputSchema = StructType(List(
      StructField("name", StringType),
      StructField("languages", ArrayType(
        StructType(List(StructField("language", StringType), StructField("level", StringType)))
      ))
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(data), inputSchema)
  }
}