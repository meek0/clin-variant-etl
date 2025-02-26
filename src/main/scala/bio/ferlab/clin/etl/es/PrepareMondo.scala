package bio.ferlab.clin.etl.es

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, collect_list, explode, struct}

object PrepareMondo {
  /**
   * Transform the DataSet into to desired output DataFrame.
   *
   * @param dataSet Dataset to transform
   * @return Transformed DataFrame
   */
  def transform(dataSet: Dataset[MondoEntry]): DataFrame = {
    dataSet
      .select(col("*"), explode(col("ancestors")).as("ancestor")).drop("ancestors")
      .select(
        col("id"),
        col("name").as("mondo_name"),
        col("parents"),
        col("is_leaf"),
        col("ancestor.id").as("mondo_id"),
        col("ancestor.name").as("name"))
      .groupBy(col("id"), col("mondo_name"), col("parents"), col("is_leaf"))
      .agg(
        collect_list(
          struct("mondo_id", "name")
        ) as "compact_ancestors"
      )
      .withColumnRenamed("id", "mondo_id")
      .withColumnRenamed("mondo_name", "name")
  }
}

case class MondoEntry(id: String, name: String, parents: Set[String] = Set.empty, ancestors: Set[MondoAncestorData] = Set.empty, is_leaf: Boolean)

case class MondoAncestorData(id: String, name: String, parents: Set[String] = Set.empty)