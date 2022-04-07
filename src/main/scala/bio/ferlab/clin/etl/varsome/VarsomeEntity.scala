package bio.ferlab.clin.etl.varsome

case class VarsomeEntity(
                          chromosome: String,
                          pos: Long,
                          ref: String,
                          alt: String,
                          variant_id: String,
                          publications:Option[VariantPublication] = None,
                          acmg_annotation: Option[ACMGAnnotation]= None,
                        )

case class VariantPublication(publications: Seq[Publication], genes:Seq[GenePublication])

case class Publication(referenced_by: Seq[String], pub_med_id: String)

case class GenePublication(gene_id: Option[String], gene_symbol: Option[String], publications: Seq[Publication])

case class ACMGAnnotation(
                           verdict: Option[Verdict],
                           classifications: Seq[Classification],
                           transcript: Option[String],
                           gene_symbol: Option[String],
                           transcript_reason: Option[String],
                           version_name: Option[String],
                           coding_impact: Option[String],
                           gene_id: Option[String]
                         )


case class Verdict(classifications: Seq[String], ACMG_rules: Option[ACMGRules])

case class ACMGRules(clinical_score: Option[Double],
                     verdict: Option[String],
                     approx_score: Option[Int],
                     pathogenic_subscore: Option[String],
                     benign_subscore: Option[String])

case class Classification(met_criteria: Option[Boolean], user_explain: Seq[String], strength: Option[String], name: String)