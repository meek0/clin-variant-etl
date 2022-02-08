package bio.ferlab.clin.etl.vcf

sealed trait VarsomeJobType
case class ForBatch(batchId:String) extends VarsomeJobType
case object Reload extends VarsomeJobType
