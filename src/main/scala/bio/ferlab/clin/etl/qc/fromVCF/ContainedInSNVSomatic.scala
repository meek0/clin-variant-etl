package bio.ferlab.clin.etl.qc.fromVCF

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ContainedInSNVSomatic extends TestingApp {
  run { spark =>
    import spark.implicits._

    val listBatchId = normalized_snv_somatic.select("batch_id").dropDuplicates.as[String].collect.toList
    listBatchId.foreach(b => handleErrors(TestDfContainsAllVarFromBatch(normalized_snv_somatic, b, 3, database)(spark)))
  }
}
