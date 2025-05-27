package bio.ferlab.clin.etl.utils.transformation

import bio.ferlab.datalake.spark3.transformation.Transformation

trait DatasetTransformationMapping { self =>

  val mapping: Map[String, List[Transformation]]

  /**
   * Utility method allowing to concat multiple [[DatasetTransformationMapping]] together
   * @param other another [[DatasetTransformationMapping]] to merge with
   * @return merged [[DatasetTransformationMapping]] containing both mappings joined together
   */
  def +(other: DatasetTransformationMapping): DatasetTransformationMapping = new DatasetTransformationMapping {
    override val mapping: Map[String, List[Transformation]] = self.mapping ++ other.mapping
  }
}
