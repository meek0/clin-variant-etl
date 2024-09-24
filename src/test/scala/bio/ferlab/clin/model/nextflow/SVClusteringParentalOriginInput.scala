package bio.ferlab.clin.model.nextflow

case class SVClusteringParentalOriginInput(`sample`: String = "1",
                                           `familyId`: String = "SRA0001",
                                           `vcf`: String = "s3a://1.vcf.gz")
