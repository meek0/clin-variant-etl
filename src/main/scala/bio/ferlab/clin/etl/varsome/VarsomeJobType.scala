package bio.ferlab.clin.etl.varsome

sealed trait VarsomeJobType
case class ForBatch(batchId:String) extends VarsomeJobType
case object Reload extends VarsomeJobType
