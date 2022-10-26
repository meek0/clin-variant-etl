package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullGene extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        gene_centric.select($"entrez_gene_id")
      ),
      shouldNotContainNull(
        gene_centric.select(explode($"hpo") as "hpo").select($"hpo.hpo_term_id" as "hpo_term_id")
      ),
      shouldNotContainNull(
        gene_centric.select(explode($"omim") as "omim").select($"omim.omim_id" as "omim_id")
      ),
      shouldNotContainNull(
        gene_centric.select(explode($"ddd") as "ddd").select($"ddd.disease_name" as "disease_name")
      ),
      shouldNotContainNull(
        gene_centric.select(explode($"cosmic") as "cosmic").select($"cosmic.tumour_types_germline" as "tumour_types_germline")
      ),
      shouldNotContainNull(
        gene_centric.select(explode($"number_of_variants_per_patient") as "number_of_variants_per_patient").select($"number_of_variants_per_patient.patient_id" as "patient_id")
      ),
    )
  }
}
