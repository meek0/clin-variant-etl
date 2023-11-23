package bio.ferlab.clin.etl.es

import bio.ferlab.clin.etl.mainutils.Release
import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import mainargs.{ParserForMethods, main}


object PrepareIndex {
  @main
  def gene_centric(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareGeneCentric.run(rc, release)

  @main
  def gene_suggestions(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareGeneSuggestions.run(rc, release)

  @main
  def variant_centric(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareVariantCentric.run(rc, release)

  @main
  def variant_suggestions(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareVariantSuggestions.run(rc, release)

  @main
  def cnv_centric(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareCnvCentric.run(rc, release)

  @main
  def coverage_by_gene_centric(rc: DeprecatedRuntimeETLContext, release: Release): Unit = PrepareCoverageByGeneCentric.run(rc, release)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
