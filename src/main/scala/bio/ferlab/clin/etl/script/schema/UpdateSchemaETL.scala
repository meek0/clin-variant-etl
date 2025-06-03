package bio.ferlab.clin.etl.script.schema

import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.TransformationsETL
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 * ETL job for updating the schema of a dataset.
 *
 * This class applies a list of transformations to the specified source dataset and overwrites
 * the destination with the updated schema.
 *
 * @param rc              The runtime ETL context.
 * @param source          The source dataset configuration.
 * @param transformations The list of transformations to apply to the dataset.
 */
class UpdateSchemaETL(rc: RuntimeETLContext,
                          override val source: DatasetConf,
                          override val transformations: List[Transformation])
  extends TransformationsETL(rc, source, null, transformations)  {

  override val mainDestination: DatasetConf = source.copy(loadtype = OverWrite, writeoptions = Map("overwriteSchema" -> "true"))

  override def reset(): Unit = {
    log.warn("Reset operation is not supported to prevent accidental data loss during schema updates. Skipping reset step.")
  }
}
