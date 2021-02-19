package bio.ferlab.clin.model

case class HumanGenesOutput(`tax_id`: Int = 9606,
                            `entrez_gene_id`: Int = 105259599,
                            `symbol`: String = "H19-ICR",
                            `locus_tag`: Option[String] = None,
                            `synonyms`: List[String] = List("H19-DMD", "IC1", "ICR1", "ICR1-DMR"),
                            `external_references`: Map[String,String] = Map("mim" -> "616186"),
                            `chromosome`: String = "11",
                            `map_location`: String = "11p15.5",
                            `description`: String = "H19/IGF2 imprinting control region",
                            `type_of_gene`: String = "biological-region",
                            `symbol_from_nomenclature_authority`: Option[String] = None,
                            `full_name_from_nomenclature_authority`: Option[String] = None,
                            `nomenclature_status`: Option[String] = None,
                            `other_designations`: List[String] = List("ICR1 differentially methylated region", "imprinting center 1"),
                            `feature_types`: Map[String,String] = Map("regulatory" -> "imprinting_control_region"),
                            `ensembl_gene_id`: Option[String] = None,
                            `omim_gene_id`: String = "616186")

