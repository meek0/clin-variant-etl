package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.script.schema.SchemaUtils.runUpdateSchemaAndVacuum
import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.spark3.transformation.Transformation
import io.delta.tables.DeltaTable

import org.slf4j

/*
Adjusts the Delta table’s partitioning to match the current configuration if it differs.
 */
case class UpdatePartitioning(rc: RuntimeETLContext) {

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  val mapping = new DatasetTransformationMapping() {

    // The new partitioning will take effect when the dataset is overwritten
    // with the updated configuration.
    override val mapping: Map[String, List[Transformation]] = rc.config.sources
      .filter(_.format == DELTA)
      .map(ds => (ds.id, List.empty[Transformation])).toMap
  }

  def isMigrated(datasetId: String): Boolean = {
    import rc.spark.implicits._
    val dataset: DatasetConf = rc.config.getDataset(datasetId)
    val expectedPartitions = dataset.partitionby

    val datasetLocation = dataset.location(rc.config)
    val detailDF = DeltaTable.forPath(rc.spark, datasetLocation).detail()
    val actualPartitions = detailDF.select("partitionColumns").as[Array[String]].first

    log.info(
      s"Checking migration for dataset: $datasetId : expected partitions: $expectedPartitions, actual partitions: $actualPartitions"
    )
    actualPartitions sameElements expectedPartitions
  }

  def run(vacuum: Boolean = false, dryrun: Boolean = false): Unit = {
    runUpdateSchemaAndVacuum(
      rc,
      isMigrated,
      mapping,
      vacuum,
      1, // Number of versions to keep if vacuum is true
      dryrun
    )
  }
}
