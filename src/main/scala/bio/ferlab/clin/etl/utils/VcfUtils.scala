package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object VcfUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)
  val ac: Column = sum(array_sum(filter(col("calls"), c => c === 1))) as "ac"
  val an: Column = sum(array_sum(transform(col("calls"), c => when(c === 1 || c === 0, 1).otherwise(0)))) as "an"
  val participant_number: Column = sum(when(col("zygosity") isin("HOM", "HET"), 1).otherwise(0)) as "participant_number"
  val hom: Column = sum(when(col("zygosity") === "HOM", 1).otherwise(0)) as "hom"
  val het: Column = sum(when(col("zygosity") === "HET", 1).otherwise(0)) as "het"

}
