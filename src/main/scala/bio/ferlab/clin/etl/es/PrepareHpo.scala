package bio.ferlab.clin.etl.es

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, collect_list, explode, struct}

object PrepareHpo {
  /**
   * Transform the DataSet into to desired output DataFrame.
   *
   * @param dataSet Dataset to transform
   * @return Transformed DataFrame
   */
  def transform(dataSet: Dataset[HPOEntry]): DataFrame = {
    dataSet
      .select(col("*"), explode(col("ancestors")).as("ancestor")).drop("ancestors")
      .select(
        col("id"),
        col("name").as("hpo_name"),
        col("parents"),
        col("is_leaf"),
        col("ancestor.id").as("hpo_id"),
        col("ancestor.name").as("name"))
      .groupBy(col("id"), col("hpo_name"), col("parents"), col("is_leaf"))
      .agg(
        collect_list(
          struct("hpo_id", "name")
        ) as "compact_ancestors"
      )
      .withColumnRenamed("id", "hpo_id")
      .withColumnRenamed("hpo_name", "name")
  }
}

case class HPOEntry(id: String, name: String, parents: Set[String] = Set.empty, ancestors: Set[AncestorData] = Set.empty, is_leaf: Boolean)

case class AncestorData(id: String, name: String, parents: Set[String] = Set.empty)