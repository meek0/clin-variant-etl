package bio.ferlab.clin.model

case class OmimGeneSetOutput(`chromosome`: String = "chr1",
                             `start`: Int = 181317711,
                             `end`: Int = 181808083,
                             `cypto_location`: String = "1q25-q31",
                             `computed_cypto_location`: String = "1q25.3",
                             `omim_gene_id`: Int = 601013,
                             `symbols`: List[String] = List("CACNA1E", "CACNL1A6", "EIEE69"),
                             `name`: String = "Calcium channel, voltage-dependent, alpha 1E subunit",
                             `approved_symbol`: String = "CACNA1E",
                             `entrez_gene_id`: Int = 777,
                             `ensembl_gene_id`: String = "ENSG00000198216",
                             `comments`: Option[String] = None,
                             `phenotype`: PHENOTYPE = PHENOTYPE() )

case class PHENOTYPE(`name`: String = "Epileptic encephalopathy, early infantile, 69",
                     `omim_id`: String = "618285",
                     `inheritance`: List[String] = List("AD"))
