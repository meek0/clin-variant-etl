package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        cnv_centric, cnv_centric.columns.filterNot(List("reference", "is_multi_allelic", "svtype", "sequencing_strategy", "genome_build").contains(_)): _*
      ),
      shouldNotContainSameValue(
        cnv_centric.select(explode($"genes")).select("col.*")
      ),
    )
  }
}
