package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.script.schema.SchemaUtils.runUpdateSchemaFor
import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.transformation.{Custom, Drop, Transformation}
import mainargs.main
import org.apache.spark.sql.functions.{col, transform}

object DropUrlColumns {

  @main
  def run(rc: RuntimeETLContext): Unit = {
    runUpdateSchemaFor(
      rc,
      filter = _ => true, // Process all datasets in the mapping
      DropUrlColumnsMapping
    )
  }
}

object DropUrlColumnsMapping extends DatasetTransformationMapping {
  private val dropUrlColumns: List[Transformation] = List(
    Drop("exomiser_urls", "cnv_vcf_urls", "snv_vcf_urls")
  )

  private val dropUrlColumnsInDonors: List[Transformation] = List(
    Custom(df => {
      df.withColumn("donors", transform(col("donors"), donor =>
        donor.dropFields("exomiser_urls", "cnv_vcf_urls", "snv_vcf_urls")))
    })
  )

  override val mapping: Map[String, List[Transformation]] = Map(
    "normalized_snv"                    -> dropUrlColumns,
    "normalized_snv_somatic"            -> dropUrlColumns,
    "normalized_cnv"                    -> dropUrlColumns,
    "normalized_cnv_somatic_tumor_only" -> dropUrlColumns,
    "enriched_snv"                      -> dropUrlColumns,
    "enriched_snv_somatic"              -> dropUrlColumns,
    "enriched_cnv"                      -> dropUrlColumns,
    "enriched_variants"                 -> dropUrlColumnsInDonors,
    // We de not need to drop the fields in enriched_clinical since in runs in initial mode
    // We do not need to drop the fields in es_index tables since they are overwritten entirely every time
  )
}
