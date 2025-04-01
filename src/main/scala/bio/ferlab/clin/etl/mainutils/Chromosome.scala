package bio.ferlab.clin.etl.mainutils

import mainargs.{ParserForClass, arg}

case class Chromosome(@arg(name = "chromosome", short = 'c', doc = "Chromosome") name: Option[String])

object Chromosome {
  implicit def configParser: ParserForClass[Chromosome] = ParserForClass[Chromosome]
}

case class OptionalChromosome(@arg(name = "chromosome", short = 'c', doc = "Chromosome") name: Option[String])

object OptionalChromosome {
  implicit def configParser: ParserForClass[OptionalChromosome] = ParserForClass[OptionalChromosome]
}
