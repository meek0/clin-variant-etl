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
                            `variant_type`: String = "germline",
                            `created_on`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                            `frequencies_by_analysis`: List[AnalysisCodeFrequencies] = List(
                              AnalysisCodeFrequencies("MM_PG", "Maladies musculaires (Panel global)", Frequency(2,4,0.5,1,2,0.5,1), Frequency(1,4,0.25,1,2,0.5,0), Frequency(3,8,0.375,2,4,0.5,1))),
                            `frequency_RQDM`: AnalysisFrequencies = AnalysisFrequencies(Frequency(2,4,0.5,1,2,0.5,1), Frequency(1,4,0.25,1,2,0.5,0), Frequency(3,8,0.375,2,4,0.5,1))
                           )