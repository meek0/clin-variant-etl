package bio.ferlab.clin.testutils

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

trait WithSparkHiveSession extends WithOutputFolder {

  private val tmp = new File("tmp").getAbsolutePath
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.sql.warehouse.dir", s"s3a://spark/wharehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/spark_staging")
    .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp/spark_local_buf")
    .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "fail")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", "accesskey")
    .config("spark.hadoop.fs.s3a.secret.key", "secretkey")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")


    .enableHiveSupport()
    .master("local")
    .getOrCreate()


}
