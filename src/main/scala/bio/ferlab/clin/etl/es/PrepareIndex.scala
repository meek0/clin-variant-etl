package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import mainargs.{ParserForMethods, main}


object PrepareIndex {
  @main
  def gene_centric(rc: DeprecatedRuntimeETLContext): Unit = PrepareGeneCentric.run(rc)

  @main
  def gene_suggestions(rc: DeprecatedRuntimeETLContext): Unit = PrepareGeneSuggestions.run(rc)

  @main
  def variant_centric(rc: DeprecatedRuntimeETLContext): Unit = PrepareVariantCentric.run(rc)

  @main
  def variant_suggestions(rc: DeprecatedRuntimeETLContext): Unit = PrepareVariantSuggestions.run(rc)

  @main
  def cnv_centric(rc: DeprecatedRuntimeETLContext): Unit = PrepareCnvCentric.run(rc)

  @main
  def coverage_by_gene_centric(rc: DeprecatedRuntimeETLContext): Unit = PrepareCoverageByGeneCentric.run(rc)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
