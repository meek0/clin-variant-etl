/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-11-23T15:13:30.986006
 */
package bio.ferlab.clin.model


case class VariantSuggestionsOutput(`chromosome`: String = "1",
                                    `locus`: String = "1-69897-T-C",
                                    `suggestion_id`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                                    `hgvsg`: String = "chr1:g.69897T>C",
                                    `rsnumber`: String = "rs200676709",
                                    `clinvar_id`: String = "445750",
                                    `symbol_aa_change`: List[String] = List("DDX11L1"),
                                    `type`: String = "variant",
                                    `suggest`: List[SUGGEST] = List(SUGGEST(), SUGGEST(2, List("DDX11L1", "ENST00000450305", "ENSG00000223972"))))

case class SUGGEST(`weight`: Int = 4,
                   `input`: List[String] = List("chr1:g.69897T>C", "rs200676709", "1-69897-T-C", "445750"))