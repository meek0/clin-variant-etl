package bio.ferlab.clin.model.nextflow

case class SVClusteringInput(`sample`: String = "1",
                             `familyId`: String = "SRA0001",
                             `vcf`: String = "s3://1.vcf.gz")
