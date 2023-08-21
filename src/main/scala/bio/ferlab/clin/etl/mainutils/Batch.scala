package bio.ferlab.clin.etl.mainutils

import mainargs.{ParserForClass, arg}

case class Batch(@arg(name = "batchId", short = 'b', doc = "Batch id") id: String)

object Batch {
  implicit def configParser: ParserForClass[Batch] = ParserForClass[Batch]
}

case class OptionalBatch(@arg(name = "batchId", short = 'b', doc = "Batch id") id: Option[String])

object OptionalBatch {
  implicit def configParser: ParserForClass[OptionalBatch] = ParserForClass[OptionalBatch]
}
