package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.model.Track
import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RegionSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "overlap" should "return T1.end - T2.start" in {
    println(
      """
        |   T1 : |---------|
        |   T2 :        |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 22, 15, 30))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 8
  }

  it should "return T2.end - T1.start" in {
    println(
      """
        |   T1:         |---------|
        |   T2 :  |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(15, 30, 10, 22))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 8
  }

  it should "return T1.end - T1.start" in {
    println(
      """
        |   T1:      |---|
        |   T2 :  |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(15, 22, 10, 30))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 8
  }

  it should "return T2.end - T2.start" in {
    println(
      """
        |   T1:   |--------|
        |   T2 :    |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 15, 22))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 8
  }

  it should "return 0" in {
    println(
      """
        |   T1:         |---|
        |   T2 : |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 40, 50))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 0
  }

  it should "return 1" in {
    println(
      """
        |   T1:      |---|
        |   T2 : |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 30, 50))
    val res: Array[Long] = overlapTrack(data)
    res.head shouldBe 1
  }

  "isOverlapping" should "return true (T1 before T2)" in {
    println(
      """
        |   T1 : |---------|
        |   T2 :        |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 22, 15, 30))
    val res = isOverlappingTrack(data)
    res.head shouldBe true
  }

  it should "return true (T2 before T1)" in {
    println(
      """
        |   T1:         |---------|
        |   T2 :  |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(15, 30, 10, 22))
    val res = isOverlappingTrack(data)
    res.head shouldBe true
  }

  it should "return true (T1 in T2)" in {
    println(
      """
        |   T1:      |---|
        |   T2 :  |--------|
        |""".stripMargin)
    val data = Seq(sameChrTrack(15, 22, 10, 30))
    val res = isOverlappingTrack(data)
    res.head shouldBe true
  }

  it should "return (T2 in T1)" in {
    println(
      """
        |   T1:   |--------|
        |   T2 :    |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 15, 22))
    val res = isOverlappingTrack(data)
    res.head shouldBe true
  }

  it should "return false" in {
    println(
      """
        |   T1:         |---|
        |   T2 : |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 40, 50))
    val res = isOverlappingTrack(data)
    res.head shouldBe false
  }

  it should "return tue (1 base intersection)" in {
    println(
      """
        |   T1:      |---|
        |   T2 : |---|
        |""".stripMargin)
    val data = Seq(sameChrTrack(10, 30, 30, 50))
    val res = isOverlappingTrack(data)
    res.head shouldBe true
  }

  it should "return flase (not same chromosome)" in {
    val data = Seq(Track(t1_chr = "1", 15, 30, t2_chr = "2", 10, 22))
    val res = isOverlappingTrack(data)
    res.head shouldBe false
  }


  def sameChrTrack(t1_start: Long, t1_end: Long, t2_start: Long, t2_end: Long, chr: String = "1"): Track = Track(chr, t1_start, t1_end, chr, t2_start, t2_end)


  private def overlapTrack(data: Seq[Track]) = {
    import spark.implicits._
    val track = data.toDF()
    val res = track
      .select(Region(col("t1_chr"), col("t1_start"), col("t1_end")).overlap(Region(col("t2_chr"), col("t2_start"), col("t2_end"))) as "overlap")
      .as[Long].collect()
    res
  }

  private def isOverlappingTrack(data: Seq[Track]) = {
    import spark.implicits._
    val track = data.toDF()
    val res = track
      .select(Region(col("t1_chr"), col("t1_start"), col("t1_end")).isOverlapping(Region(col("t2_chr"), col("t2_start"), col("t2_end"))) as "overlap")
      .as[Boolean].collect()
    res
  }

}
