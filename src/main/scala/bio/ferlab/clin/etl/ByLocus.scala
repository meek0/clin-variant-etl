package bio.ferlab.clin.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}

object ByLocus {

  val id: Column = sha1(concat(col("chromosome"), col("start"), col("reference"), col("alternate"))) as "id"

  implicit class ByLocusDataframe(df: DataFrame) {

    def joinAndDrop(other: DataFrame, joinType: String = "inner"): DataFrame = {
      joinByLocus(other, joinType)
        .drop(other("chromosome"))
        .drop(other("start"))
        .drop(other("reference"))
        .drop(other("alternate"))
    }


    def joinAndMerge(other: DataFrame, outputColumnName: String, joinType: String = "inner"): DataFrame = {
      joinByLocus(other, joinType)
        .select(df("*"), when(other("chromosome").isNull, lit(null)).otherwise(struct(other.drop("chromosome", "start", "end", "name", "reference", "alternate")("*"))) as outputColumnName)
    }

    private def joinByLocus(other: DataFrame, joinType: String): DataFrame = {
      df.join(other, df("chromosome") === other("chromosome") && df("start") === other("start") && df("reference") === other("reference") && df("alternate") === other("alternate"), joinType)
    }

    def groupByLocus(): RelationalGroupedDataset = {
      df.groupBy(df("chromosome"), df("start"), df("reference"), df("alternate"))
    }

    def selectLocus(cols: Column*): DataFrame = {
      val allCols = df("chromosome") :: df("start") :: df("reference") :: df("alternate") :: cols.toList
      df.select(allCols: _*)
    }

  }

}
