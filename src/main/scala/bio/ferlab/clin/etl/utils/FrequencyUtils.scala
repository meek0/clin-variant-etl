package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object FrequencyUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

  val isFilterPass: Column = col("filters") === Array("PASS")

  /**
   * allele count
   */
  val ac: Column = sum(array_sum(filter(col("calls"), c => c === 1))) as "ac"
  /**
   * allele total number
   */
  val an: Column = sum(lit(2)) as "an"

  /**
   * participant count
   */
  val pc: Column = sum(when(col("zygosity").isin("HOM", "HET"), 1).otherwise(0)) as "pc"

  /**
   * participant total number
   */
  val pn: Column = sum(lit(1)) as "pn"

  val hom: Column = sum(when(col("zygosity") === "HOM", 1).otherwise(0)) as "hom"
  val het: Column = sum(when(col("zygosity") === "HET", 1).otherwise(0)) as "het"

}
