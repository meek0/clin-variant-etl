package bio.ferlab.clin.etl.mainutils

import mainargs.{ParserForClass, arg}


case class AnalysisIds(@arg(name = "analysisId", short = 'a', doc = "List of analysis ids") ids: Seq[String])

object AnalysisIds {
  implicit def configParser: ParserForClass[AnalysisIds] = ParserForClass[AnalysisIds]
}