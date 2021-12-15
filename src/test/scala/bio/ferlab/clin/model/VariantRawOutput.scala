package bio.ferlab.clin.model

import java.sql.Timestamp
import java.time.LocalDateTime

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
                            //`last_batch_id`: Option[String] = None,
                            `created_on`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                            `updated_on`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                            `normalized_variants_oid`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                            `locus`: String = "1-69897-T-C")