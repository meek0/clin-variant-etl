package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_ExtFreq extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.thousand_genomes.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.topmed_bravo.*"),
        variant_centric.select($"external_frequencies.topmed_bravo.*").columns.filterNot(List("an").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_2_1_1.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_exomes_2_1_1.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_0.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"external_frequencies.gnomad_genomes_3_1_1.*")
      ),
    )
  }
}
