package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunEnriched {

  @main
  def variants(rc: DeprecatedRuntimeETLContext): Unit = Variants.run(rc)

  @main
  def consequences(rc: DeprecatedRuntimeETLContext): Unit = Consequences.run(rc)

  @main
  def cnv(rc: DeprecatedRuntimeETLContext): Unit = CNV.run(rc)

  @main
  def snv(rc: DeprecatedRuntimeETLContext): Unit = SNV.run(rc)

  @main
  def snv_somatic_tumor_only(rc: DeprecatedRuntimeETLContext): Unit = SNVSomaticTumorOnly.run(rc)

  @main
  def coverage_by_gene(rc: DeprecatedRuntimeETLContext): Unit = CoverageByGene.run(rc)
  @main
  def all(rc: DeprecatedRuntimeETLContext): Unit = {
    variants(rc)
    consequences(rc)
    cnv(rc)
    snv(rc)
    snv_somatic_tumor_only(rc)
    coverage_by_gene(rc)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}