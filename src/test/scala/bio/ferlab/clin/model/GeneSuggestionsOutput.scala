/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-11-24T13:57:01.502663
 */
package bio.ferlab.clin.model


case class GeneSuggestionsOutput(`type`: String = "gene",
                                 `symbol`: String = "OR4F5",
                                 `ensembl_gene_id`: String = "ENSG00000198216",
                                 `suggestion_id`: String = "9b8016c31b93a7504a8314ce3d060792f67ca2ad",
                                 `suggest`: List[SUGGEST] = List(
                                   SUGGEST(4, List("OR4F5")),
                                   SUGGEST(2, List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139", "ENSG00000198216"))))
