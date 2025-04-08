package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.mainutils.{OptionalBatch, OptionalChromosome}
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import mainargs.{ParserForMethods, main}

object RunEnriched {

  @main
  def variants(rc: RuntimeETLContext, chromosome: OptionalChromosome): Unit = Variants.run(rc, chromosome)

  @main
  def consequences(rc: RuntimeETLContext): Unit = Consequences.run(rc)

  @main
  def cnv(rc: RuntimeETLContext, batch: OptionalBatch): Unit = CNV.run(rc, batch)

  @main
  def snv(rc: RuntimeETLContext): Unit = SNV.run(rc)

  @main
  def snv_somatic(rc: RuntimeETLContext, batch: OptionalBatch): Unit = SNVSomatic.run(rc, batch)

  @main
  def coverage_by_gene(rc: RuntimeETLContext): Unit = CoverageByGene.run(rc)
  @main
  def all(rc: RuntimeETLContext, batch: OptionalBatch, chromosome: OptionalChromosome): Unit = {
    variants(rc, chromosome)
    consequences(rc)
    cnv(rc, batch)
    snv(rc)
    snv_somatic(rc, batch)
    coverage_by_gene(rc)
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)
}