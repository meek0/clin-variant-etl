package bio.ferlab.clin.etl.enriched

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunEnriched {

  @main
  def variants(rc: RuntimeETLContext): Unit = Variants.run(rc)

  @main
  def consequences(rc: RuntimeETLContext): Unit = Consequences.run(rc)

  @main
  def cnv(rc: RuntimeETLContext): Unit = CNV.run(rc)

  @main
  def snv(rc: RuntimeETLContext): Unit = SNV.run(rc)

  @main
  def snv_somatic_tumor_only(rc: RuntimeETLContext): Unit = SNVSomaticTumorOnly.run(rc)

  @main
  def coverage_by_gene(rc: RuntimeETLContext): Unit = CoverageByGene.run(rc)
  @main
  def all(rc: RuntimeETLContext): Unit = {
    variants(rc)
    consequences(rc)
    cnv(rc)
    snv(rc)
    snv_somatic_tumor_only(rc)
    coverage_by_gene(rc)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}