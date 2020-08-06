package bio.ferlab.clin.etl

import org.apache.spark.sql.functions.{col, concat, sha1}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}

object ByLocus {

  val id: Column = sha1(concat(col("chromosome"), col("start"), col("reference"), col("alternate"))) as "id"

  implicit class ByLocusDataframe(df: DataFrame) {

    def joinByLocus(other: DataFrame, joinType:String = "inner"): DataFrame = {
      df.join(other, df("chromosome") === other("chromosome") && df("start") === other("start") && df("reference") === other("reference") && df("alternate") === other("alternate"), joinType)
        .drop(other("chromosome"))
        .drop(other("start"))
        .drop(other("reference"))
        .drop(other("alternate"))
    }

    def groupByLocus(): RelationalGroupedDataset = {
      df.groupBy(df("chromosome"), df("start"), df("reference"), df("alternate"))
    }

  }

}
