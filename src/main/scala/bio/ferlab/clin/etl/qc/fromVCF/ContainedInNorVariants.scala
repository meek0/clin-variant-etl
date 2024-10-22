package bio.ferlab.clin.etl.qc.fromVCF

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ContainedInNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val listBatchId = normalized_variants.select("batch_id").dropDuplicates.as[String].collect.toList
                      .filterNot(_ == "201106_A00516_0169_AHFM3HDSXY")
    listBatchId.foreach(b => handleErrors(TestDfContainsAllVarFromBatch(normalized_variants.filter($"batch_id" =!= "201106_A00516_0169_AHFM3HDSXY"), b, 3, database)(spark)))
  }
}
