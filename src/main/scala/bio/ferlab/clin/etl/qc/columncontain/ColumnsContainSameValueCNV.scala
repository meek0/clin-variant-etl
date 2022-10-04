package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullCNV.run
import org.apache.spark.sql.functions._

object ColumnsContainSameValueCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    var df = cnv_centric
    df.columns.filterNot(List("reference", "is_multi_allelic", "svtype", "sequencing_strategy", "genome_build").contains(_)).foreach{
      c => assert(df.select(col(c)).na.drop.distinct().count() != 1, "La colonne ne devrait pas contenir uniquement la même valeur")
    }

    df = cnv_centric.select(explode($"genes")).select("col.*")
    df.columns.filterNot(List().contains(_)).foreach{
      c => assert(df.select(col(c)).na.drop.distinct().count() != 1, "La colonne ne devrait pas contenir uniquement la même valeur")
    }
  }
}
