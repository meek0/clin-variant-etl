package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.ByLocus._
import bio.ferlab.clin.etl.columns.{ac, an}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, collect_set, filter, first, lit, struct, sum, when}

object PrepareIndex extends App {

  val Array(output, batchId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .enableHiveSupport()
    .appName(s"Extract").getOrCreate()

  run(output, batchId)

  def run(output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    spark.sql("use spark_tests")

    import spark.implicits._
    val newVariants = spark.table("variants").where($"batch_id" === batchId).as("variants")

    val consequences = spark.table("consequences")
      .drop("batch_id", "name", "end", "hgvsg", "variant_class")
      .as("consequences")

    val occurrences = spark.table("occurrences")
      .drop("is_multi_allelic", "old_multi_allelic", "name", "end").where($"has_alt" === true)
      .as("occurrences")

    val clinvar = spark.table("clinvar")
    val dbsnp = spark.table("dbsnp")

    newVariants.sample(0.0001)
      .joinByLocus(consequences)
      .joinByLocus(occurrences)
      .joinByLocus(clinvar, "left")
      .joinByLocus(dbsnp, "left")
      .groupByLocus()
      .agg(
        first(struct("variants.*")) as "variant",
        collect_list(struct("consequences.*")) as "consequences",
        collect_list(struct("occurrences.*")) as "occurrences",
        struct(ac, an) as "internal_frequencies",
        struct(first($"clinvar.name") as "clinvar_id", first("clin_sig") as "clin_sig") as "clinvar",
        collect_set($"dbsnp.name") as "dbsnp"
      )
      .select($"variant.*",
        $"consequences",
        $"occurrences",
        $"internal_frequencies",
        $"dbsnp",
        when($"clinvar.clinvar_id".isNotNull, $"clinvar").otherwise(lit(null)) as "clinvar"
      )
      .write.mode("overwrite")
      .json(s"$output/extract")

    //    val updatedVariants = spark.table("variants").where($"last_batch_id" === batchId)
    //      .join(occurrences, newVariants("chromosome") === occurrences("chromosome") && newVariants("start") === occurrences("start") && newVariants("reference") === occurrences("reference") && newVariants("alternate") === occurrences("alternate"))


  }


}

