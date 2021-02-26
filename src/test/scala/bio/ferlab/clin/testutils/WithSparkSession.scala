package bio.ferlab.clin.testutils

import org.apache.spark.sql.SparkSession

trait WithSparkSession extends WithOutputFolder {


  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
