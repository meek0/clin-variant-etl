package bio.ferlab.clin.etl

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, array_union, col, lit, when}

package object enriched {

  def withExternalReference(df: DataFrame, conditionValueMap: List[(Column, String)], outputColumn: String = "variant_external_reference"): DataFrame = {
    conditionValueMap
      .tail
      .foldLeft(
        df.withColumn(outputColumn, when(conditionValueMap.head._1, array(lit(conditionValueMap.head._2))).otherwise(array()))
      ) { case (currDf, (cond, value)) =>
        currDf.withColumn(outputColumn, when(cond, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
  }

}
