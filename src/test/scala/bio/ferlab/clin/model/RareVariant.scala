package bio.ferlab.clin.model

case class RareVariant(chromosome: String = "1",
                       start: Long = 69897,
                       reference: String = "T",
                       alternate: String = "C")

case class RareVariantOutput(chromosome: String = "1",
                             start: Long = 69897,
                             reference: String = "T",
                             alternate: String = "C",
                             is_rare: Boolean = false
                            )
