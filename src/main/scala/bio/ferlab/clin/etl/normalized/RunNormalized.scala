package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
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
  def snv_somatic_tumor_only(rc: RuntimeETLContext, batch: Batch): Unit = SNVSomaticTumorOnly.run(rc, batch)

  @main
  def cnv(rc: RuntimeETLContext, batch: Batch): Unit = CNV.run(rc, batch)

  @main
  def cnv_somatic_tumor_only(rc: RuntimeETLContext, batch: Batch): Unit = CNVSomaticTumorOnly.run(rc, batch)

  @main
  def panels(rc: RuntimeETLContext): Unit = Panels.run(rc)

  @main
  def exomiser(rc: RuntimeETLContext, batch: Batch): Unit = Exomiser.run(rc, batch)

  @main
  def all(rc: RuntimeETLContext, batch: Batch): Unit = {
    snv(rc, batch)
    snv_somatic_tumor_only(rc, batch)
    cnv(rc, batch)
    cnv_somatic_tumor_only(rc, batch)
    variants(rc, batch)
    consequences(rc, batch)
    panels(rc)
    exomiser(rc, batch)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}
