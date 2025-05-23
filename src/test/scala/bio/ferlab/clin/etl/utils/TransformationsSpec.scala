package bio.ferlab.clin.etl.utils

import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}

class TransformationsSpec extends SparkSpec {

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

    an [AnalysisException] should be thrownBy job.transform(df)
  }

  "RenameFieldsInArrayStruct" should "fail if a field to rename does not exist in the struct" in {
    val df = testRenameFieldsInArrayStructDf()
    val job = RenameFieldsInArrayStruct("languages", Map("FOO" -> "lang", "level" -> "proficiency"))

    an [AnalysisException] should be thrownBy job.transform(df)
  }

  "RenameFieldsInArrayStruct" should "fail if the target column is not an array of structs" in {
    val df = testRenameFieldsInArrayStructDf().select(col("name"), explode(col("languages")) as "languages")
   
    val job = RenameFieldsInArrayStruct("languages", Map("language" -> "lang", "level" -> "proficiency"))

    an [AnalysisException] should be thrownBy job.transform(df)
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