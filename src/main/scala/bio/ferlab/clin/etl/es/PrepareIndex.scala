package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}


object PrepareIndex {
  @main
  def gene_centric(rc: RuntimeETLContext): Unit = PrepareGeneCentric.run(rc)

  @main
  def gene_suggestions(rc: RuntimeETLContext): Unit = PrepareGeneSuggestions.run(rc)

  @main
  def variant_centric(rc: RuntimeETLContext): Unit = PrepareVariantCentric.run(rc)

  @main
  def variant_suggestions(rc: RuntimeETLContext): Unit = PrepareVariantSuggestions.run(rc)

  @main
  def cnv_centric(rc: RuntimeETLContext): Unit = PrepareCnvCentric.run(rc)

  @main
  def coverage_by_gene_centric(rc: RuntimeETLContext): Unit = PrepareCoverageByGeneCentric.run(rc)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
