package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunNormalized {
  @main
  def variants(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = Variants.run(rc, batch)

  @main
  def consequences(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = Consequences.run(rc, batch)

  @main
  def snv(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = SNV.run(rc, batch)

  @main
  def snv_somatic_tumor_only(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = SNVSomaticTumorOnly.run(rc, batch)

  @main
  def cnv(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = CNV.run(rc, batch)

  @main
  def cnv_somatic_tumor_only(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = CNVSomaticTumorOnly.run(rc, batch)

  @main
  def panels(rc: DeprecatedRuntimeETLContext): Unit = Panels.run(rc)

  @main
  def exomiser(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = Exomiser.run(rc, batch)

  @main
  def coverage_by_gene(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = CoverageByGene.run(rc, batch)

  @main
  def all(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = {
    snv(rc, batch)
    snv_somatic_tumor_only(rc, batch)
    cnv(rc, batch)
    cnv_somatic_tumor_only(rc, batch)
    variants(rc, batch)
    consequences(rc, batch)
    panels(rc)
    exomiser(rc, batch)
    coverage_by_gene(rc, batch)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
