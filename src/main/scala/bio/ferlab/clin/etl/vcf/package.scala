package bio.ferlab.clin.etl

package object vcf {

  val validContigNames: List[String] = List("chrX", "chrY") ++ (1 to 22).map(n => s"chr$n")

}
