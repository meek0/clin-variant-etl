package bio.ferlab.clin.etl.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object DeltaUtils {

  def writeOnce(df: DataFrame,
                outputFolder: Option[String],
                database: String,
                tableName: String,
                dataChange: Boolean = true,
                partitionBy: Seq[String],
                mode: SaveMode = SaveMode.Overwrite)(implicit spark: SparkSession): Unit = {

    val dfw = df
      .write
      .option("dataChange", dataChange)
      .mode(mode)
      .partitionBy(partitionBy: _*)
      .format("delta")

    outputFolder.fold(dfw.saveAsTable(s"$database.$tableName"))(output =>
      dfw
        .option("path", s"$output/$tableName")
        .saveAsTable(s"$database.$tableName"))


  }

  def insert(df: DataFrame,
             outputFolder: Option[String],
             database: String,
             tableName: String,
             repartition: DataFrame => DataFrame,
             partitionBy: Seq[String])(implicit spark: SparkSession): Unit = {

    writeOnce(
      df = repartition(df),
      outputFolder = outputFolder,
      database = database,
      tableName = tableName,
      dataChange = true,
      partitionBy = partitionBy,
      mode = SaveMode.Append)

    writeOnce(
      df = repartition(spark.table(tableName)),
      outputFolder = outputFolder,
      database = database,
      tableName = tableName,
      dataChange = false,
      partitionBy = partitionBy)
  }

  def upsert(updates: DataFrame,
             outputFolder: Option[String],
             database: String,
             tableName: String,
             repartition: DataFrame => DataFrame,
             primaryKeys: Seq[String],
             partitionBy: Seq[String],
             createdOn: String = "createdOn")(implicit spark: SparkSession): Unit = {

    import spark.implicits._
    Try(DeltaTable.forName(tableName)) match {
      case Failure(_) =>
        DeltaUtils.writeOnce(
          repartition(updates),
          database = database,
          tableName = tableName,
          outputFolder = outputFolder,
          dataChange = true,
          partitionBy = partitionBy
        )
      case Success(existingData) =>

        val mergeCondition: Column = primaryKeys.map(c => updates(c) === $"e.$c").reduce((a, b) => a && b)

        /** Merge */
        existingData.as("e")
          .merge(
            updates.as("u"),
            mergeCondition
          )
          .whenMatched()
          .updateExpr(updates.columns.filterNot(_.equals(createdOn)).map(c => c -> s"u.$c").toMap)
          .whenNotMatched()
          .insertAll()
          .execute()

        /** Compact */
        DeltaUtils.writeOnce(
          repartition(updates),
          database = database,
          tableName = tableName,
          outputFolder = outputFolder,
          dataChange = false,
          partitionBy = partitionBy
        )
    }
  }

}
