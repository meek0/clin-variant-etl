package bio.ferlab.clin.etl.mainutils

import mainargs.{ParserForClass, arg}

case class Release(@arg(name = "releaseId", short = 'r', doc = "Release id") id: String)

object Release {
  implicit def configParser: ParserForClass[Release] = ParserForClass[Release]
}
