package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_ExtFreq extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.thousand_genomes.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.topmed_bravo.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select($"external_frequencies.gnomad_genomes_4.*")
      ),
    )
  }
}
