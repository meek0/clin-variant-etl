package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object FrequencyUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

  val includeFilter: Column = col("ad_alt") >= 3

  val frequencyFilter: Column = array_contains(col("filters"), "PASS") && includeFilter && col("gq") >= 20

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
