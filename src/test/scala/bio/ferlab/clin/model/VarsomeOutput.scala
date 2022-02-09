package bio.ferlab.clin.model

import bio.ferlab.clin.etl.varsome.{ACMGAnnotation, ACMGRules, Classification, GenePublication, Publication, VariantPublication, Verdict}

import java.sql.Timestamp
import java.time.LocalDateTime

case class VarsomeOutput(
                          chromosome: String = "1",
                          start: Long = 69897,
                          reference: String = "T",
                          alternate: String = "C",
                          variant_id: String = "10190150730274780002",
                          `updated_on`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                          publications: Option[VariantPublication] = Some(VariantPublication(
                            publications = VarsomeOutput.defaultPublications,
                            genes = Seq(GenePublication(
                              publications = Seq(
                                Publication(pub_med_id = "7711739", referenced_by = Seq("CGD")),
                                Publication(pub_med_id = "11381270", referenced_by = Seq("gene2phenotype", "CGD"))
                              ),
                              gene_id = Some("1959"),
                              gene_symbol = Some("BBS4")
                            ))
                          )),
                          acmg_annotation: Option[ACMGAnnotation] = Some(
                            ACMGAnnotation(
                              verdict = Some(
                                Verdict(classifications = Seq("BA1", "BP6_Very Strong"),
                                  ACMG_rules = Some(ACMGRules(
                                    clinical_score = Some(1.242361132330188),
                                    verdict = Some("Benign"),
                                    approx_score = Some(-11),
                                    pathogenic_subscore = Some("Uncertain Significance"),
                                    benign_subscore = Some("Benign")
                                  )))
                              ),
                              classifications = VarsomeOutput.defaultClassifications,
                              transcript = Some("NM_033028.5"),
                              gene_symbol = Some("BBS4"),
                              transcript_reason = Some("canonical"),
                              version_name = Some("11.1.6"),
                              coding_impact = Some("missense"),
                              gene_id = Some("1959"),
                            )
                          )


                        )

object VarsomeOutput {
  val defaultClassifications = Seq(
    Classification(
      met_criteria = Some(true),
      user_explain = Seq("GnomAD exomes allele frequency = 0.573 is greater than 0.05 threshold (good gnomAD exomes coverage = 75.8)."),
      name = "BA1",
      strength = None
    ),
    Classification(
      met_criteria = Some(true),
      user_explain = Seq(
        "UniProt Variants classifies this variant as Benign, citing 3 articles (%%PUBMED:15770229%%, %%PUBMED:15666242%% and %%PUBMED:14702039%%), associated with Bardet-Biedl syndrome, Bardet-Biedl syndrome 1 and Bardet-Biedl syndrome 4.",
        "Using strength Strong because ClinVar classifies this variant as Benign, 2 stars (multiple consistent, 9 submissions), citing %%PUBMED:12016587%%, associated with Allhighlypenetrant, Bardet-Biedl Syndrome, Bardet-Biedl Syndrome 1 and Bardet-Biedl Syndrome 4.",
        "Using strength Very Strong because of the combined evidence from ClinVar and UniProt Variants."
      ),
      name = "BP6",
      strength = Some("Very Strong")
    ),
  )

  val defaultPublications = Seq(
    Publication(pub_med_id = "12016587", referenced_by = Seq("gene2phenotype", "ClinVar", "CGD")),
    Publication(pub_med_id = "15666242", referenced_by = Seq("UniProt Variants"))
  )
}