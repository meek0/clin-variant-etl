package bio.ferlab.clin.model

case class GeneCentricOutput(`hash`: String = "9b8016c31b93a7504a8314ce3d060792f67ca2ad",
                             `symbol`: String = "OR4F5",
                             `entrez_gene_id`: Int = 777,
                             `omim_gene_id`: String = "601013",
                             `hgnc`: String = "HGNC:1392",
                             `ensembl_gene_id`: String = "ENSG00000198216",
                             `location`: String = "1q25.3",
                             `name`: String = "calcium voltage-gated channel subunit alpha1 E",
                             `alias`: List[String] = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139"),
                             `biotype`: String = "protein_coding",
                             `orphanet`: List[ORPHANET] = List(ORPHANET()),
                             `hpo`: List[HPO] = List(HPO()),
                             `omim`: List[OMIM] = List(OMIM()),
                             `chromosome`: String = "1",
                             `ddd`: List[DDD] = List(DDD()),
                             `cosmic`: List[COSMIC] = List(COSMIC()))

