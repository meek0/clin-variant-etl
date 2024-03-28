package bio.ferlab.clin.etl.fhir

object GenomicFile {

  case object COVGENE extends GenomicFile {
    override val dataType: String = "COVGENE"
    override val format: String = "CSV"
    override val urlColumn: String = "covgene_urls"
  }

  case object EXOMISER extends GenomicFile {
    override val dataType: String = "EXOMISER"
    override val format: String = "TSV"
    override val urlColumn: String = "exomiser_urls"
  }
}

sealed trait GenomicFile {
  /**
   * Name of the data type as specified in FHIR.
   */
  val dataType: String

  /**
   * Name of the file format as specified in FHIR.
   */
  val format: String

  /**
   * Name of the column containing urls of this file type in the [[EnrichedClinical]] table.
   */
  val urlColumn: String
}
