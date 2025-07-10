package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.{AnalysisIds, Batch}
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunNormalized {
  @main
  def variants(rc: RuntimeETLContext, batch: Batch): Unit = Variants.run(rc, batch)

  @main
  def consequences(rc: RuntimeETLContext, batch: Batch): Unit = Consequences.run(rc, batch)

  @main
  def snv(rc: RuntimeETLContext, batch: Batch): Unit = SNV.run(rc, batch)

  @main
  def snv_somatic(rc: RuntimeETLContext, batch: Batch): Unit = SNVSomatic.run(rc, batch)

  @main
  def cnv(rc: RuntimeETLContext, batch: Batch): Unit = CNV.run(rc, batch)

  @main
  def cnv_somatic_tumor_only(rc: RuntimeETLContext, batch: Batch): Unit = CNVSomaticTumorOnly.run(rc, batch)

  @main
  def panels(rc: RuntimeETLContext): Unit = Panels.run(rc)

  @main
  def exomiser(rc: RuntimeETLContext, batch: Batch): Unit = Exomiser.run(rc, batch)

  @main
  def coverage_by_gene(rc: RuntimeETLContext, batch: Batch): Unit = CoverageByGene.run(rc, batch)

  @main
  def franklin(rc: RuntimeETLContext, analysisIds: AnalysisIds): Unit = Franklin.run(rc, analysisIds)

  @main
  def all(rc: RuntimeETLContext, batch: Batch): Unit = {
    snv(rc, batch)
    snv_somatic(rc, batch)
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
