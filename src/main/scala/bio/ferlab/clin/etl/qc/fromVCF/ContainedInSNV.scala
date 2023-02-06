package bio.ferlab.clin.etl.qc.fromVCF

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ContainedInSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    val listBatchId = normalized_snv.select("batch_id").dropDuplicates.as[String].collect.toList
    listBatchId.foreach(b => handleErrors(TestDfContainsAllVarFromBatch(normalized_variants, b, 0, database)(spark)))
  }
}
