package bio.ferlab.clin.etl.utils.transformation

import bio.ferlab.datalake.spark3.transformation.{Rename, Transformation, UpperCase}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetTransformationMappingSpec extends AnyFlatSpec with Matchers {

  "DatasetTransformationMapping" should "allow merging multiple mappings" in {
    val mapping1 = new DatasetTransformationMapping {
      override val mapping: Map[String, List[Transformation]] = Map(
        "dataset1" -> List(Rename(Map("col1" -> "new_col1")))
      )
    }

    val mapping2 = new DatasetTransformationMapping {
      override val mapping: Map[String, List[Transformation]] = Map(
        "dataset2" -> List(Rename(Map("col2" -> "new_col2")))
      )
    }

    val mergedMapping = mapping1 + mapping2

    mergedMapping.mapping shouldBe Map(
        "dataset1" -> List(Rename(Map("col1" -> "new_col1"))),
        "dataset2" -> List(Rename(Map("col2" -> "new_col2")))
    )
  }

  "DatasetTransformationMapping" should "use the transformations from the right mapping when merging duplicate dataset ids" in {
    val mapping1 = new DatasetTransformationMapping {
      override val mapping: Map[String, List[Transformation]] = Map(
        "dataset1" -> List(Rename(Map("col1" -> "new_col1")))
      )
    }

    val mapping2 = new DatasetTransformationMapping {
      override val mapping: Map[String, List[Transformation]] = Map(
        "dataset1" -> List(UpperCase("new_col1"))
      )
    }

    val chainedMapping = mapping1 + mapping2

    chainedMapping.mapping shouldBe Map(
      "dataset1" -> List(UpperCase("new_col1"))
    )
  }
}
