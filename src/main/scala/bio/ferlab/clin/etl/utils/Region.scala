package bio.ferlab.clin.etl.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

case class Region(chromosome: Column, start: Column, end: Column) {

  val nbBases: Column = end - start + 1

  def isOverlapping(other: Region): Column = {
    this.chromosome === other.chromosome and (
      (this.start between(other.start, other.end) or (this.end between(other.start, other.end))) or
        (other.start between(this.start, this.end) or (other.end between(this.start, this.end)))
      )
  }

  def overlap(other: Region): Column = {
    when(this.start <= other.start and this.end >= other.end, other.nbBases)
      .when(this.start >= other.start and this.end <= other.end, this.nbBases)
      .when(this.start <= other.start and (this.end between(other.start, other.end)), this.end - other.start + 1)
      .when(this.start >= other.start and (this.start between(other.start, other.end)), other.end - this.start + 1)
      .otherwise(0)
  }
}