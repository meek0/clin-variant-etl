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
    override val urlColumn: String = "exomiser_snv_urls"
  }

  case object EXOMISER_CNV extends GenomicFile {
    override val dataType: String = "EXOMISER_CNV"
    override val format: String = "TSV"
    override val urlColumn: String = "exomiser_cnv_urls"
  }

  case object SNV extends GenomicFile {
    override val dataType: String = "SNV"
    override val format: String = "VCF"
    override val urlColumn: String = "snv_vcf_germline_urls"
  }

  case object SSNV extends GenomicFile {
    override val dataType: String = "SSNV"
    override val format: String = "VCF"
    override val urlColumn: String = "snv_vcf_somatic_urls"
  }

  case object GCNV extends GenomicFile {
    override val dataType: String = "GCNV"
    override val format: String = "VCF"
    override val urlColumn: String = "cnv_vcf_germline_urls"
  }

  case object SCNV extends GenomicFile {
    override val dataType: String = "SCNV"
    override val format: String = "VCF"
    override val urlColumn: String = "cnv_vcf_somatic_urls"
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
