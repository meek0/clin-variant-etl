package bio.ferlab.clin.etl.qc.fromVCF

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ContainedInSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    val listBatchId = normalized_snv.select("batch_id").dropDuplicates.map(f=>f.getString(0)).collect.toList
    listBatchId.foreach(b => handleErrors(TestDfContainsAllVarFromBatch(normalized_snv, b, 0)))
  }
}
