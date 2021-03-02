package bio.ferlab.clin.model

case class VariantRawOutput(`chromosome`: String = "1",
                            `start`: Long = 69897,
                            `end`: Long = 69898,
                            `reference`: String = "T",
                            `alternate`: String = "C",
                            `name`: String = "rs200676709",
                            `is_multi_allelic`: Boolean = false,
                            `old_multi_allelic`: Option[String] = None,
                            `genes_symbol`: List[String] = List("OR4F5"),
                            `hgvsg`: String = "chr1:g.69897T>C",
                            `variant_class`: String = "SNV",
                            `pubmed`: Option[List[String]] = None,
                            `batch_id`: String = "BAT1",
                            `last_batch_id`: Option[String] = None,
                            `createdOn`: String = "BAT1", //Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                            `updatedOn`: String = "BAT1") //Timestamp = Timestamp.valueOf(LocalDateTime.now()))
