package bio.ferlab.clin.model

case class SpliceAiOutput(`chromosome`: String = "1",
                          `start`: Long = 69897,
                          `end`: Long = 69898,
                          `reference`: String = "T",
                          `alternate`: String = "C",
                          `allele`: String = "C",
                          `symbol`: String = "OR4F5",
                          `ds_ag`: Double = 0.01,
                          `ds_al`: Double = 0.0,
                          `ds_dg`: Double = 0.0,
                          `ds_dl`: Double = 0.0,
                          `dp_ag`: Int = -32,
                          `dp_al`: Int = 36,
                          `dp_dg`: Int = 27,
                          `dp_dl`: Int = 36,
                          `max_score`: MAX_SCORE = MAX_SCORE())

case class MAX_SCORE(`ds`: Double = 0.01,
                     `type`: Seq[String] = Seq("AG"))
