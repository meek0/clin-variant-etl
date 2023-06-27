package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.model.raw.VCF_SNV_Input
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.ClassTag

package object normalized {

  val validContigNames: List[String] = List("chrX", "chrY") ++ (1 to 22).map(n => s"chr$n")

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  def loadOptionalVCFDataFrame[T <: Product : ClassTag : TypeTag](location: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      vcf(location, referenceGenomePath = None).where(col("contigName").isin(validContigNames: _*))
    } catch {
      case e: AnalysisException if e.message.contains("Path does not exist") => {
        log.warn(s"No VCF files found at location: $location returning empty DataFrame of type: ${typeOf[T]}")
        Seq.empty[T].toDF
      }
      case e: Exception => throw e
    }
  }

}
