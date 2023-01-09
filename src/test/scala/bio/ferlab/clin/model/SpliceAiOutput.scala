package bio.ferlab.clin.model

case class SpliceAiOutput(`chromosome`: String = "1",
                          `start`: Long = 69897,
                          `end`: Long = 69898,
                          `reference`: String = "T",
                          `alternate`: String = "C",
                          `allele`: String = "C",
                          `symbol`: String = "OR4F5",
                          `ds_ag`: String = "0.01",
                          `ds_al`: String = "0.00",
                          `ds_dg`: String = "0.00",
                          `ds_dl`: String = "0.00",
                          `dp_ag`: String = "-32",
                          `dp_al`: String = "36",
                          `dp_dg`: String = "27",
                          `dp_dl`: String = "36")
