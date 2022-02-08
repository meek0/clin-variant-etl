package bio.ferlab.clin.etl.varsome

import bio.ferlab.clin.etl.vcf.ForBatch
import bio.ferlab.clin.model.{VariantRawOutput, VarsomeExtractOutput, VarsomeOutput}
import bio.ferlab.clin.testutils.HttpServerUtils.{resourceHandler, withHttpServer}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

class VarsomeSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {
  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  import spark.implicits._

  private val current = LocalDateTime.now()
  private val ts = Timestamp.valueOf(current)
  private val sevenDaysAgo = Timestamp.valueOf(current.minusDays(8))
  private val yesterday = Timestamp.valueOf(current.minusDays(1))
  val normalized_variantsDF: DataFrame = Seq(
    VariantRawOutput(start = 1000, `reference` = "A", `alternate` = "T"),
    VariantRawOutput(start = 1001, `reference` = "A", `alternate` = "T", `batch_id` = "BAT2"),
    VariantRawOutput(start = 1002, `reference` = "A", `alternate` = "T")
  ).toDF()
  val normalized_varsomeDF: DataFrame = Seq(
    VarsomeOutput("1", 1000, "A", "T", "1234", sevenDaysAgo),
    VarsomeOutput("1", 1002, "A", "T", "5678", yesterday)
  ).toDF()
  val normalized_varsome: DatasetConf = conf.getDataset("normalized_varsome")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val defaultData = Map(
    normalized_variants.id -> normalized_variantsDF,
    normalized_varsome.id -> normalized_varsomeDF
  )

  def withVarsomeServer[T](resource: String = "varsome_minimal.json")(block: String => T): T = withHttpServer("/lookup/batch/hg38", resourceHandler(resource, "application/json"))(block)

  def withData[T](data: Map[String, DataFrame] = defaultData)(block: Map[String, DataFrame] => T): T = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(normalized_varsome.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_normalized")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)
      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
    block(data)
  }


  "extract" should "return a dataframe that contains only variants not in varsome table or older than 7 days in varsome table" in {
    withData() { _ =>
      val dataframes = new Varsome(ForBatch("BAT1"), "url", "").extract(currentRunDateTime = current)
      dataframes(normalized_variants.id).as[VarsomeExtractOutput].collect() should contain theSameElementsAs Seq(
        VarsomeExtractOutput("1", 1000, "A", "T")
      )
    }

  }

  "transform" should "return a dataframe representing Varsome response" in {
    withData() { data =>
      withVarsomeServer() { url =>
        val df = new Varsome(ForBatch("BAT1"), url, "").transform(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts)
        )
      }

    }
  }

  it should "return a dataframe representing complete Varsome response" in {
    withData() { data =>

      withVarsomeServer("varsome_full.json") { url =>
        val df = new Varsome(ForBatch("BAT1"), url, "").transform(data, currentRunDateTime = current)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("15", 73027478, "T", "C", "10190150730274780002", ts,
            publications = Some(VariantPublication(
              publications = Seq(
                Publication(pub_med_id = "12016587", referenced_by = Seq("gene2phenotype", "ClinVar", "CGD")),
                Publication(pub_med_id = "15666242", referenced_by = Seq("UniProt Variants"))
              ),
              genes = Seq(GenePublication(
                publications = Seq(
                  Publication(pub_med_id = "7711739", referenced_by = Seq("CGD")),
                  Publication(pub_med_id = "11381270", referenced_by = Seq("gene2phenotype", "CGD"))
                ),
                gene_id = Some("1959"),
                gene_symbol = Some("BBS4")
              ))
            )),
            acmg_annotation = Some(
              ACMGAnnotation(
                verdict = Some(
                  Verdict(classifications = Seq("BA1", "BP6_Very Strong"),
                    ACMG_rules = Some(ACMGRules(
                      clinical_score = Some(1.242361132330188),
                      verdict = Some("Benign"),
                      approx_score = Some(-11),
                      pathogenic_subscore = Some("Uncertain Significance"),
                      benign_subscore = Some("Benign")
                    )))
                ),
                classifications = Seq(
                  Classification(
                    met_criteria = Some(true),
                    user_explain = Seq("GnomAD exomes allele frequency = 0.573 is greater than 0.05 threshold (good gnomAD exomes coverage = 75.8)."),
                    name = "BA1",
                    strength = None
                  ),
                  Classification(
                    met_criteria = Some(true),
                    user_explain = Seq(
                      "UniProt Variants classifies this variant as Benign, citing 3 articles (%%PUBMED:15770229%%, %%PUBMED:15666242%% and %%PUBMED:14702039%%), associated with Bardet-Biedl syndrome, Bardet-Biedl syndrome 1 and Bardet-Biedl syndrome 4.",
                      "Using strength Strong because ClinVar classifies this variant as Benign, 2 stars (multiple consistent, 9 submissions), citing %%PUBMED:12016587%%, associated with Allhighlypenetrant, Bardet-Biedl Syndrome, Bardet-Biedl Syndrome 1 and Bardet-Biedl Syndrome 4.",
                      "Using strength Very Strong because of the combined evidence from ClinVar and UniProt Variants."
                    ),
                    name = "BP6",
                    strength = Some("Very Strong")
                  ),
                ),
                transcript = Some("NM_033028.5"),
                gene_symbol = Some("BBS4"),
                transcript_reason = Some("canonical"),
                version_name = Some("11.1.6"),
                coding_impact = Some("missense"),
                gene_id = Some("1959"),
              )
            )
          )
        )
      }

    }
  }


  "run" should "create varsome table" in {
    withData() { _ =>

      withVarsomeServer() { url =>
        new Varsome(ForBatch("BAT1"), url, "").run(currentRunDateTime = Some(current))
        val df = spark.table(normalized_varsome.table.get.fullName)
        df.as[VarsomeOutput].collect() should contain theSameElementsAs Seq(
          VarsomeOutput("1", 1000, "A", "T", "1234", ts),
          VarsomeOutput("1", 1002, "A", "T", "5678", yesterday)
        )
      }

    }
  }

}