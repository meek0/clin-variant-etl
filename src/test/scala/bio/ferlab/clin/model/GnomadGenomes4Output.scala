package bio.ferlab.clin.model

case class GnomadGenomes4Output(chromosome: String = "1",
                                start: Long = 69897,
                                end: Long = 69899,
                                reference: String = "T",
                                alternate: String = "C",
                                qual: Double = 0.5,
                                name: String = "BRAF",
                                ac: Long = 2,
                                af: Double = 2.0,
                                an: Long = 20,
                                hom: Long = 10)
