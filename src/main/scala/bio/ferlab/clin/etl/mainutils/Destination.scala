package bio.ferlab.clin.etl.mainutils

import mainargs.{ParserForClass, arg}

case class Destination(@arg(name = "destination", short = 'd', doc = "Destination dataset id") id: String)

object Destination {
  implicit def configParser: ParserForClass[Destination] = ParserForClass[Destination]
}
