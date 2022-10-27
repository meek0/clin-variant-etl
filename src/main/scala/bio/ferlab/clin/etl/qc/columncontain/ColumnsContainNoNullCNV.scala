package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        cnv_centric,
        "aliquot_id", "chromosome", "start", "reference", "alternate"
      ),
      shouldNotContainNull(
        cnv_centric.select(explode($"genes") as "gene").select($"gene.overlap_exons" as "overlap_exons")
      ),
    )
  }
}
