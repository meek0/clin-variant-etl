package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
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
import java.sql.Date

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val normalized_variants: DatasetConf = conf.getDataset("normalized_variants")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_genomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_2_1_1")
  val gnomad_exomes_2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_2_1_1")
  val gnomad_genomes_3_0: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_0")
  val gnomad_genomes_3_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_3_1_1")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val varsome: DatasetConf = conf.getDataset("normalized_varsome")

  val normalized_occurrencesDf: DataFrame = Seq(
    NormalizedSNV(`patient_id` = "PA0001", `transmission` = Some("AD"), `organization_id` = "OR00201", `parental_origin` = Some("mother")),
    NormalizedSNV(`patient_id` = "PA0002", `transmission` = Some("AR"), `organization_id` = "OR00202", `parental_origin` = Some("father")),
    NormalizedSNV(`patient_id` = "PA0003", `has_alt` = false, `zygosity` = "UNK", `calls` = List(0, 0))
  ).toDF
  val normalized_variantsDf: DataFrame = Seq(NormalizedVariants()).toDF()
  val genomesDf: DataFrame = Seq(OneKGenomesOutput()).toDF
  val topmed_bravoDf: DataFrame = Seq(Topmed_bravoOutput()).toDF
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(GnomadGenomes211Output()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(GnomadExomes211Output()).toDF
  val gnomad_genomes_3_0Df: DataFrame = Seq(GnomadGenomes30Output()).toDF
  val gnomad_genomes_3_1_1Df: DataFrame = Seq(GnomadGenomes311Output()).toDF
  val dbsnpDf: DataFrame = Seq(DbsnpOutput()).toDF
  val clinvarDf: DataFrame = Seq(ClinvarOutput()).toDF
  val genesDf: DataFrame = Seq(GenesOutput()).toDF()
  val normalized_panelsDf: DataFrame = Seq(PanelOutput()).toDF()
  val varsomeDf: DataFrame = Seq(VarsomeOutput()).toDF()

  val data = Map(
    normalized_variants.id -> normalized_variantsDf,
    normalized_snv.id -> normalized_occurrencesDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_3_0.id -> gnomad_genomes_3_0Df,
    gnomad_genomes_3_1_1.id -> gnomad_genomes_3_1_1Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf,
    normalized_panels.id -> normalized_panelsDf,
    varsome.id -> varsomeDf
  )

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(enriched_variants.location))
    spark.sql("CREATE DATABASE IF NOT EXISTS clin_normalized")
    spark.sql("CREATE DATABASE IF NOT EXISTS clin")

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  val expectedDonors =
    List(
      DONORS(1, 30, List(0, 1), 8.07, true,List("PASS"),0,30,30,1.0,"HET","chr1:g.69897T>C","SNV","BAT1","SR0095","14-696","SP_696",Date.valueOf("2022-04-06"),"germline","PA0001","FM00001","PPR00101","OR00201","WXS","11111","MM_PG","Maladies musculaires (Panel global)","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("mother"),Some("AD")),
      DONORS(1, 30, List(0, 1), 8.07, true,List("PASS"),0,30,30,1.0,"HET","chr1:g.69897T>C","SNV","BAT1","SR0095","14-696","SP_696",Date.valueOf("2022-04-06"),"germline","PA0002","FM00001","PPR00101","OR00202","WXS","11111","MM_PG","Maladies musculaires (Panel global)","PA0003","PA0002",Some(List(0, 1)),Some(List(0, 0)),Some(true),Some(false),Some("HET"),Some("WT"),Some("father"),Some("AR"))
  )

  val expectedFrequencies = Map("MN-PG" -> Map("affected" -> Frequency(), "total" -> Frequency()))
/*
  "variants job" should "aggregate frequencies from normalized_variants" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = true , `patient_id` = "1", `ad_alt`=30),
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = false, `patient_id` = "2", `ad_alt`=30),
      NormalizedSNV(`analysis_code` = "ACHO" ,`affected_status` = true , `patient_id` = "3", `ad_alt`=30),
      NormalizedSNV(`analysis_code` = "ACHO" ,`affected_status` = true , `patient_id` = "4", `ad_alt`=30),
      NormalizedSNV(`analysis_code` = "GEAN" ,`affected_status` = true , `patient_id` = "5", `ad_alt`=30)
    ).toDF
occurrencesDf.show(false)
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
|chromosome|start|reference|alternate|patient_id|aliquot_id|end  |name       |dp |gq |calls |qd  |has_alt|is_multi_allelic|old_multi_allelic|filters|ad_ref|ad_alt|ad_total|ad_ratio|hgvsg          |variant_class|batch_id|last_update|variant_type|service_request_id|analysis_service_request_id|sequencing_strategy|genome_build|analysis_code|analysis_display_name              |family_id|is_proband|gender|practitioner_role_id|organization_id|affected_status|mother_id|father_id|specimen_id|sample_id|mother_calls|father_calls|mother_affected_status|father_affected_status|zygosity|mother_zygosity|father_zygosity|parental_origin|transmission      |is_hc|hc_complement |possibly_hc_complement|is_possibly_hc|
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
|1         |69897|T        |C        |1         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |MM_PG        |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |true           |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
|1         |69897|T        |C        |2         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |MM_PG        |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |false          |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
|1         |69897|T        |C        |3         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |ACHO         |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |true           |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
|1         |69897|T        |C        |4         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |ACHO         |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |true           |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
|1         |69897|T        |C        |5         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |GEAN         |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |true           |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "MM_PG",
            `analysis_display_name` = "Maladies musculaires (Panel global)",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            `analysis_code` = "GEAN",
            `analysis_display_name` = "General analysis",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(2, 4, 0.5, 2, 2, 1.0, 0),
          `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          `total` =        Frequency(2, 4, 0.5, 2, 2, 1.0, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "MM_PG",
            `analysis_display_name` = "Maladies musculaires (Panel global)",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
            `total` =        Frequency(2, 2, 1.0, 1, 1, 1.0, 1)),
          AnalysisCodeFrequencies(
            `analysis_code` = "ACHO",
            `analysis_display_name` = "Achondroplasia",
            `affected` =     Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
            `non_affected` = Frequency(0, 0, 0.0,  0, 0, 0.0, 0),
            `total` =        Frequency(1, 4, 0.25, 1, 2, 0.5, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 4, 0.25, 1, 2, 0.5,         0),
          `non_affected` = Frequency(2, 2, 1.0 , 1, 1, 1.0,         1),
          `total` =        Frequency(3, 6, 0.5 , 2, 3, 0.666666667, 1))),
    ).toDF()
variantDf.show(false)
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------+--------+--------------------------+
|chromosome|start|end  |reference|alternate|name       |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|frequencies_by_analysis                                                                                                                                                                                                                   |frequency_RQDM                                                                            |batch_id|created_on                |
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------+--------+--------------------------+
|1         |69897|69898|T        |C        |rs200676709|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |[{MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}, {GEAN, General analysis, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{2, 4, 0.5, 2, 2, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}}         |BAT1    |2022-04-06 13:21:08.019096|
|1         |69897|69898|T        |C        |rs200676709|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |[{MM_PG, Maladies musculaires (Panel global), {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 2, 1.0, 1, 1, 1.0, 1}}, {ACHO, Achondroplasia, {1, 4, 0.25, 1, 2, 0.5, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}]|{{1, 4, 0.25, 1, 2, 0.5, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {3, 6, 0.5, 2, 3, 0.666666667, 1}}|BAT2    |2022-04-06 13:21:08.019096|
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------+--------+--------------------------+
    val expectedFrequencies = List(
      AnalysisCodeFrequencies(
        `analysis_code` = "MM_PG",
        `analysis_display_name` = "Maladies musculaires (Panel global)",
        `affected` =     Frequency(1, 2, 0.5,  1, 1, 1.0, 0),
        `non_affected` = Frequency(2, 2, 1.0,  1, 1, 1.0, 1),
        `total` =        Frequency(3, 4, 0.75, 2, 2, 1.0, 1)),
      AnalysisCodeFrequencies(
        `analysis_code` = "GEAN",
        `analysis_display_name` = "General analysis",
        `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
      AnalysisCodeFrequencies(
        `analysis_code` = "ACHO",
        `analysis_display_name` = "Achondroplasia",
        `affected` =     Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
        `non_affected` = Frequency(0, 0, 0.0 , 0, 0, 0.0, 0),
        `total` =        Frequency(1, 4, 0.25, 1, 2, 0.5, 0))
    )

    val expectedRQDMFreq = AnalysisFrequencies(
      `affected` =     Frequency(3, 8,  0.375, 3, 4, 0.75, 0),
      `non_affected` = Frequency(2, 2,  1.0  , 1, 1, 1.0 , 1),
      `total` =        Frequency(5, 10, 0.5  , 4, 5, 0.8 , 1))

    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))
    val result = resultDf.as[VariantEnrichedOutput].collect().head
resultDf.show(false)
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+--------------------------+-----------+----------------------------------------+--------------------------+
|chromosome|start|reference|alternate|panels       |frequencies_by_analysis                                                                                                                                                                                                                                                                                                                               |frequency_RQDM                                                                       |end  |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|updated_on                |created_on                |assembly_version|last_annotation_update|dna_change|donors                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |external_frequencies                                                                                                                                |rsnumber   |clinvar                                                                 |genes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |omim    |varsome                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |gene_external_reference|variant_external_reference|locus      |hash                                    |variants_oid              |
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+--------------------------+-----------+----------------------------------------+--------------------------+
|1         |69897|T        |C        |[DYSTM, MITN]|[{ACHO, Achondroplasia, {1, 4, 0.25, 1, 2, 0.5, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}, {GEAN, General analysis, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}, {MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {3, 4, 0.75, 2, 2, 1.0, 1}}]|{{3, 8, 0.375, 3, 4, 0.75, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {5, 10, 0.5, 4, 5, 0.8, 1}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{1, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, MM_PG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {2, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, MM_PG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {3, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, ACHO, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {4, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, ACHO, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {5, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GEAN, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{{3446, 0.688099, 5008}, {2, 1.59276E-5, 125568, 0, 2}, {1, 3.7962189659099535E-5, 26342, 0}, {0, 0.0, 2, 0}, {0, 0.0, 53780, 0}, {10, 0.5, 20, 10}}|rs200676709|{445750, [Likely_benign], [not provided], [germline], [, Likely_benign]}|[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|{10190150730274780002, [{[gene2phenotype, ClinVar, CGD], 12016587}, {[UniProt Variants], 15666242}], true, {{1.242361132330188, Benign, -11, Uncertain Significance, Benign}, [{true, [GnomAD exomes allele frequency = 0.573 is greater than 0.05 threshold (good gnomAD exomes coverage = 75.8).], null, BA1}, {true, [UniProt Variants classifies this variant as Benign, citing 3 articles (%%PUBMED:15770229%%, %%PUBMED:15666242%% and %%PUBMED:14702039%%), associated with Bardet-Biedl syndrome, Bardet-Biedl syndrome 1 and Bardet-Biedl syndrome 4., Using strength Strong because ClinVar classifies this variant as Benign, 2 stars (multiple consistent, 9 submissions), citing %%PUBMED:12016587%%, associated with Allhighlypenetrant, Bardet-Biedl Syndrome, Bardet-Biedl Syndrome 1 and Bardet-Biedl Syndrome 4., Using strength Very Strong because of the combined evidence from ClinVar and UniProt Variants.], Very Strong, BP6}], NM_033028.5, canonical, BBS4, missense}}|[HPO, Orphanet, OMIM]  |[DBSNP, Clinvar, Pubmed]  |1-69897-T-C|314c8a3ce0334eab1a9358bcaf8c6f4206971d92|2022-04-06 13:21:08.019096|
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------+--------------------------+-----------+----------------------------------------+--------------------------+
    result.`frequencies_by_analysis` should contain allElementsOf expectedFrequencies
    result.`frequency_RQDM` shouldBe expectedRQDMFreq
  }
*/

/*
  "variants job" should "aggregate frequencies from 2 variants from normalized_variants (frequency_RQDM PASS)" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = true , `patient_id` = "1", `ad_alt`=30, `batch_id` = "BAT1", `start` = 101),
      NormalizedSNV(`analysis_code` = "GEAN" ,`affected_status` = false, `patient_id` = "2", `ad_alt`=30, `batch_id` = "BAT2", `start` = 101)
    ).toDF

    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "MM_PG",
            `analysis_display_name` = "Maladies musculaires (Panel global)",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 2, 0.5 , 1, 1, 1.0, 0),
          `non_affected` = Frequency(0, 0, 0.0 , 0, 0, 0.0, 0),
          `total` =        Frequency(1, 2, 0.5 , 1, 1, 1.0, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "GEAN",
            `analysis_display_name` = "General analysis",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
            `total` =        Frequency(2, 2, 1.0, 1, 1, 1.0, 1))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
          `non_affected` = Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
          `total` =        Frequency(2, 2, 1.0, 1, 1, 1.0, 1)))
    ).toDF()

    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))

resultDf.orderBy("start").show(false)
/*
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|chromosome|start|reference|alternate|panels       |frequencies_by_analysis                                                                                                                                                                                                                   |frequency_RQDM                                                                    |end  |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|updated_on                |created_on                |assembly_version|last_annotation_update|dna_change|donors                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |external_frequencies                |rsnumber   |clinvar|genes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |omim    |varsome|gene_external_reference|variant_external_reference|locus    |hash                                    |variants_oid              |
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|1         |101  |T        |C        |[DYSTM, MITN]|[{GEAN, General analysis, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 2, 1.0, 1, 1, 1.0, 1}}, {MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{1, 2, 0.5, 1, 1, 1.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {3, 4, 0.75, 2, 2, 1.0, 1}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-05            |T>C       |[{1, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, MM_PG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {2, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GEAN, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-101-T-C|899607a56ee98f319eff50dba3ff0dfff6fd6912|2022-04-06 13:21:08.019096|
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
*/
    val result = resultDf.as[VariantEnrichedOutput].collect()
    result.find(_.`start` == 101).head.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 2, 0.5 , 1, 1, 1.0, 0),
      Frequency(2, 2, 1.0 , 1, 1, 1.0, 1),
      Frequency(3, 4, 0.75, 2, 2, 1.0, 1))
  }
*/







  "variants job" should "aggregate frequencies from 2 variants from normalized_variants (frequency_RQDM FAIL)" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = true , `patient_id` = "1", `ad_alt`=30, `batch_id` = "BAT1", `start` = 101),
      NormalizedSNV(`analysis_code` = "GEAN" ,`affected_status` = false, `patient_id` = "2", `ad_alt`=30, `batch_id` = "BAT2", `start` = 102)
    ).toDF

    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "MM_PG",
            `analysis_display_name` = "Maladies musculaires (Panel global)",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 2, 0.5 , 1, 1, 1.0, 0),
          `non_affected` = Frequency(0, 2, 0.0 , 0, 1, 0.0, 0),
          `total` =        Frequency(1, 4, 0.25, 1, 2, 0.5, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 102,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "GEAN",
            `analysis_display_name` = "General analysis",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 1.0, 1, 1, 1.0, 1),
            `total` =        Frequency(1, 2, 1.0, 1, 1, 1.0, 1))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
          `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
          `total` =        Frequency(1, 4, 0.25, 1, 2, 0.5, 1)))
    ).toDF()

    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))

//resultDf.orderBy("start").show(false)
/*
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|chromosome|start|reference|alternate|panels       |frequencies_by_analysis                                                                                                        |frequency_RQDM                                                                   |end  |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|updated_on                |created_on                |assembly_version|last_annotation_update|dna_change|donors                                                                                                                                                                                                                                                                                                                                                                                        |external_frequencies                |rsnumber   |clinvar|genes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |omim    |varsome|gene_external_reference|variant_external_reference|locus    |hash                                    |variants_oid              |
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|1         |101  |T        |C        |[DYSTM, MITN]|[{MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-05            |T>C       |[{1, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, MM_PG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-101-T-C|899607a56ee98f319eff50dba3ff0dfff6fd6912|2022-04-06 13:21:08.019096|
|1         |102  |T        |C        |[DYSTM, MITN]|[{GEAN, General analysis, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 1}, {1, 2, 0.5, 1, 1, 1.0, 1}}]                    |{{0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 1}, {1, 2, 0.5, 1, 1, 1.0, 1}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-05            |T>C       |[{2, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GEAN, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-102-T-C|c8a0f834533e786e148dc9e24a75bfc66794332b|2022-04-06 13:21:08.019096|
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
*/
    val result = resultDf.as[VariantEnrichedOutput].collect()
    result.find(_.`start` == 101).head.`frequency_RQDM` shouldBe AnalysisFrequencies(
      affected = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
      non_affected = Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
      total = Frequency(1, 4, 0.25, 1, 2, 0.5, 0),
    )
    result.find(_.`start` == 102).head.`frequency_RQDM` shouldBe AnalysisFrequencies(
      affected = Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
      non_affected = Frequency(1, 2, 0.5, 1, 1, 1.0, 1),
      total = Frequency(1, 4, 0.25, 1, 2, 0.5, 1))
  }












/*

  "variants job" should "Test (frequency_RQDM FAIL)" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "MM_PG",`affected_status` = true , `patient_id` = "1", `ad_alt`=30, `batch_id` = "BAT1", `start` = 101),
      NormalizedSNV(`analysis_code` = "GEAN" ,`affected_status` = false, `patient_id` = "2", `ad_alt`=30, `batch_id` = "BAT1", `start` = 102)
    ).toDF
occurrencesDf.orderBy("start").show(false)
/*
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
|chromosome|start|reference|alternate|patient_id|aliquot_id|end  |name       |dp |gq |calls |qd  |has_alt|is_multi_allelic|old_multi_allelic|filters|ad_ref|ad_alt|ad_total|ad_ratio|hgvsg          |variant_class|batch_id|last_update|variant_type|service_request_id|analysis_service_request_id|sequencing_strategy|genome_build|analysis_code|analysis_display_name              |family_id|is_proband|gender|practitioner_role_id|organization_id|affected_status|mother_id|father_id|specimen_id|sample_id|mother_calls|father_calls|mother_affected_status|father_affected_status|zygosity|mother_zygosity|father_zygosity|parental_origin|transmission      |is_hc|hc_complement |possibly_hc_complement|is_possibly_hc|
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
|1         |101  |T        |C        |1         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |MM_PG        |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |true           |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
|1         |102  |T        |C        |2         |11111     |69898|rs200676709|1  |30 |[0, 1]|8.07|true   |false           |null             |[PASS] |0     |30    |30      |1.0     |chr1:g.69897T>C|SNV          |BAT1    |2022-04-06 |germline    |SR0095            |SRA0001                    |WXS                |GRCh38      |GEAN         |Maladies musculaires (Panel global)|FM00001  |true      |Male  |PPR00101            |OR00201        |false          |PA0003   |PA0002   |SP_696     |14-696   |[0, 1]      |[0, 0]      |true                  |false                 |HET     |HET            |WT             |mother         |autosomal_dominant|false|[{null, null}]|[{null, null}]        |false         |
+----------+-----+---------+---------+----------+----------+-----+-----------+---+---+------+----+-------+----------------+-----------------+-------+------+------+--------+--------+---------------+-------------+--------+-----------+------------+------------------+---------------------------+-------------------+------------+-------------+-----------------------------------+---------+----------+------+--------------------+---------------+---------------+---------+---------+-----------+---------+------------+------------+----------------------+----------------------+--------+---------------+---------------+---------------+------------------+-----+--------------+----------------------+--------------+
*/
    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "MM_PG",
            `analysis_display_name` = "Maladies musculaires (Panel global)",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 2, 0.5 , 1, 1, 1.0, 0),
          `non_affected` = Frequency(0, 2, 0.0 , 0, 1, 0.0, 0),
          `total` =        Frequency(1, 4, 0.25, 1, 2, 0.5, 0))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 102,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "GEAN",
            `analysis_display_name` = "General analysis",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
            `total` =        Frequency(2, 2, 1.0, 1, 1, 1.0, 1))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
          `non_affected` = Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
          `total` =        Frequency(2, 4, 0.5, 1, 2, 0.5, 1)))
    ).toDF()
variantDf.orderBy("start").show(false)
/*
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+-------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------+--------------------------+
|chromosome|start|end  |reference|alternate|name       |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|frequencies_by_analysis                                                                                                        |frequency_RQDM                                                                     |batch_id|created_on                |
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+-------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------+--------------------------+
|1         |101  |69898|T        |C        |rs200676709|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |[{MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 2, 0.0, 0, 1, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.05, 0}}|BAT1    |2022-04-06 13:21:08.019096|
|1         |102  |69898|T        |C        |rs200676709|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |[{GEAN, General analysis, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 2, 1.0, 1, 1, 1.0, 1}}]                    |{{0, 2, 0.0, 0, 1, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 4, 0.5, 1, 2, 0.5, 1}}  |BAT1    |2022-04-06 13:21:08.019096|
+----------+-----+-----+---------+---------+-----------+------------+---------------+-------------+----------+------------+-------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------+--------------------------+
*/
    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))

resultDf.orderBy("start").show(false)
/*
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|chromosome|start|reference|alternate|panels       |frequencies_by_analysis                                                                                                        |frequency_RQDM                                                                   |end  |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|updated_on                |created_on                |assembly_version|last_annotation_update|dna_change|donors                                                                                                                                                                                                                                                                                                                                                                                        |external_frequencies                |rsnumber   |clinvar|genes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |omim    |varsome|gene_external_reference|variant_external_reference|locus    |hash                                    |variants_oid              |
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|1         |101  |T        |C        |[DYSTM, MITN]|[{MM_PG, Maladies musculaires (Panel global), {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-05            |T>C       |[{1, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, MM_PG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-101-T-C|899607a56ee98f319eff50dba3ff0dfff6fd6912|2022-04-06 13:21:08.019096|
|1         |102  |T        |C        |[DYSTM, MITN]|[{GEAN, General analysis, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 2, 1.0, 1, 1, 1.0, 1}}]                    |{{0, 0, 0.0, 0, 0, 0.0, 0}, {2, 2, 1.0, 1, 1, 1.0, 1}, {2, 2, 1.0, 1, 1, 1.0, 1}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-05            |T>C       |[{2, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GEAN, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-102-T-C|c8a0f834533e786e148dc9e24a75bfc66794332b|2022-04-06 13:21:08.019096|
+----------+-----+---------+---------+-------------+-------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
*/
    val result = resultDf.as[VariantEnrichedOutput].collect()
    result.find(_.`start` == 101).head.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
      Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
      Frequency(2, 4, 1.0, 1, 2, 1.0, 1))
    result.find(_.`start` == 102).head.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(0, 2, 0.0, 0, 1, 0.0, 0),
      Frequency(2, 2, 1.0, 1, 1, 1.0, 1),
      Frequency(2, 4, 1.0, 1, 2, 1.0, 1))
  }
*/
/*
  "variants job" should "aggregate frequencies from normalized_variants *** Karine ***" in {

    val occurrencesDf: DataFrame = Seq(
      NormalizedSNV(`analysis_code` = "AAAA",`affected_status` = true , `patient_id` = "PA01", `ad_alt`=30, `batch_id` = "BAT1", `start` = 101),
      NormalizedSNV(`analysis_code` = "BBBB",`affected_status` = true , `patient_id` = "PA02", `ad_alt`=30, `batch_id` = "BAT2", `start` = 102),
      NormalizedSNV(`analysis_code` = "CCCC",`affected_status` = true , `patient_id` = "PA03", `ad_alt`=30, `batch_id` = "BAT1", `start` = 103),
      NormalizedSNV(`analysis_code` = "CCCC",`affected_status` = true , `patient_id` = "PA04", `ad_alt`=30, `batch_id` = "BAT2", `start` = 103),
      NormalizedSNV(`analysis_code` = "DDDD",`affected_status` = true , `patient_id` = "PA05", `ad_alt`=30, `batch_id` = "BAT1", `start` = 104),
      NormalizedSNV(`analysis_code` = "DDDD",`affected_status` = false, `patient_id` = "PA06", `ad_alt`=30, `batch_id` = "BAT2", `start` = 105),
      NormalizedSNV(`analysis_code` = "EEEE",`affected_status` = true , `patient_id` = "PA05", `ad_alt`=30, `batch_id` = "BAT1", `start` = 106),
      NormalizedSNV(`analysis_code` = "FFFF",`affected_status` = false, `patient_id` = "PA06", `ad_alt`=30, `batch_id` = "BAT2", `start` = 106),
      NormalizedSNV(`analysis_code` = "GGGG",`affected_status` = true , `patient_id` = "PA07", `ad_alt`=30, `batch_id` = "BAT2", `start` = 107),
      NormalizedSNV(`analysis_code` = "GGGG",`affected_status` = false, `patient_id` = "PA08", `ad_alt`=30, `batch_id` = "BAT2", `start` = 107),
      NormalizedSNV(`analysis_code` = "HHHH",`affected_status` = true , `patient_id` = "PA09", `ad_alt`=30, `batch_id` = "BAT2", `start` = 108),
      NormalizedSNV(`analysis_code` = "IIII",`affected_status` = false, `patient_id` = "PA10", `ad_alt`=30, `batch_id` = "BAT2", `start` = 108),
      NormalizedSNV(`analysis_code` = "JJJJ",`affected_status` = true , `patient_id` = "PA11", `ad_alt`=30, `batch_id` = "BAT2", `start` = 109),
      NormalizedSNV(`analysis_code` = "JJJJ",`affected_status` = true , `patient_id` = "PA11", `ad_alt`=30, `batch_id` = "BAT2", `start` = 110),
      NormalizedSNV(`analysis_code` = "KKKK",`affected_status` = true , `patient_id` = "PA12", `ad_alt`=30, `batch_id` = "BAT2", `start` = 111),
      NormalizedSNV(`analysis_code` = "LLLL",`affected_status` = true , `patient_id` = "PA13", `ad_alt`=30, `batch_id` = "BAT2", `start` = 111)
    ).toDF

    val variantDf = Seq(
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 101,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "AAAA",
            `analysis_display_name` = "Analyse de test A",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 6, 0.166, 1, 3, 0.333, 0),
          `non_affected` = Frequency(0, 0, 0.0  , 0, 0, 0.0  , 0),
          `total` =        Frequency(1, 6, 0.166, 1, 3, 0.333, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 102,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "BBBB",
            `analysis_display_name` = "Analyse de test B",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0 , 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 103,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "CCCC",
            `analysis_display_name` = "Analyse de test C",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 6, 0.166, 1, 3, 0.333, 0),
          `non_affected` = Frequency(0, 0, 0.0  , 0, 0, 0.0  , 0),
          `total` =        Frequency(1, 6, 0.166, 1, 3, 0.333, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 103,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "CCCC",
            `analysis_display_name` = "Analyse de test C",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0 , 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 104,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "DDDD",
            `analysis_display_name` = "Analyse de test D",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 6, 0.166, 1, 3, 0.333, 0),
          `non_affected` = Frequency(0, 0, 0.0  , 0, 0, 0.0  , 0),
          `total` =        Frequency(1, 6, 0.166, 1, 3, 0.333, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 105,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "DDDD",
            `analysis_display_name` = "Analyse de test D",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0 , 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT1",
        `start` = 106,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "EEEE",
            `analysis_display_name` = "Analyse de test E",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 6, 0.166, 1, 3, 0.333, 0),
          `non_affected` = Frequency(0, 0, 0.0  , 0, 0, 0.0  , 0),
          `total` =        Frequency(1, 6, 0.166, 1, 3, 0.333, 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 106,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "FFFF",
            `analysis_display_name` = "Analyse de test F",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(0, 14, 0.0  , 0, 7 , 0.0  , 0),
          `non_affected` = Frequency(1, 6 , 0.166, 1, 3 , 0.333, 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 107,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "GGGG",
            `analysis_display_name` = "Analyse de test G",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `total` =        Frequency(2, 4, 0.5, 2, 2, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(1, 6 , 0.166, 1, 3 , 0.333, 0),
          `total` =        Frequency(2, 20, 0.1  , 2, 10, 0.2  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 108,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "HHHH",
            `analysis_display_name` = "Analyse de test H",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            `analysis_code` = "IIII",
            `analysis_display_name` = "Analyse de test I",
            `affected` =     Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `non_affected` = Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(1, 6 , 0.166, 1, 3 , 0.333, 0),
          `total` =        Frequency(2, 20, 0.1  , 2, 10, 0.2  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 109,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "JJJJ",
            `analysis_display_name` = "Analyse de test J",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 110,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "JJJJ",
            `analysis_display_name` = "Analyse de test J",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(1, 14, 0.071, 1, 7 , 0.143, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(1, 20, 0.05 , 1, 10, 0.1  , 0))),
      NormalizedVariants(
        `batch_id` = "BAT2",
        `start` = 111,
        `frequencies_by_analysis` = List(
          AnalysisCodeFrequencies(
            `analysis_code` = "KKKK",
            `analysis_display_name` = "Analyse de test K",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)),
          AnalysisCodeFrequencies(
            `analysis_code` = "LLLL",
            `analysis_display_name` = "Analyse de test L",
            `affected` =     Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
            `non_affected` = Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
            `total` =        Frequency(1, 2, 0.5, 1, 1, 1.0, 0))),
        `frequency_RQDM` = AnalysisFrequencies(
          `affected` =     Frequency(2, 14, 0.143, 2, 7 , 0.286, 0),
          `non_affected` = Frequency(0, 6 , 0.0  , 0, 3 , 0.0  , 0),
          `total` =        Frequency(2, 20, 0.1  , 2, 10, 0.2  , 0)))
    ).toDF()

    val resultDf = new Variants().transform(data ++ Map(normalized_variants.id -> variantDf, normalized_snv.id -> occurrencesDf))

resultDf.orderBy("start").show(false)
/*
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|chromosome|start|reference|alternate|panels       |frequencies_by_analysis                                                                                                                                                                                                 |frequency_RQDM                                                                    |end  |genes_symbol|hgvsg          |variant_class|pubmed    |variant_type|updated_on                |created_on                |assembly_version|last_annotation_update|dna_change|donors                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |external_frequencies                |rsnumber   |clinvar|genes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |omim    |varsome|gene_external_reference|variant_external_reference|locus    |hash                                    |variants_oid              |
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
|1         |101  |T        |C        |[DYSTM, MITN]|[{AAAA, Analyse de test A, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]                                                                                                            |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA01, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, AAAA, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                 |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-101-T-C|899607a56ee98f319eff50dba3ff0dfff6fd6912|2022-04-06 13:21:08.019096|
|1         |102  |T        |C        |[DYSTM, MITN]|[{BBBB, Analyse de test B, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]                                                                                                            |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA02, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, BBBB, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                 |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-102-T-C|c8a0f834533e786e148dc9e24a75bfc66794332b|2022-04-06 13:21:08.019096|
|1         |103  |T        |C        |[DYSTM, MITN]|[{CCCC, Analyse de test C, {2, 4, 0.5, 2, 2, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}}]                                                                                                            |{{2, 4, 0.5, 2, 2, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA03, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, CCCC, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {PA04, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, CCCC, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}] |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-103-T-C|9b307b28d3e3d3faf923646568b7a19f827d5312|2022-04-06 13:21:08.019096|
|1         |104  |T        |C        |[DYSTM, MITN]|[{DDDD, Analyse de test D, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 2, 0.0, 0, 1, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}]                                                                                                           |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 2, 0.0, 0, 1, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA05, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, DDDD, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                 |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-104-T-C|41de4b59b171706678b18fdab5b8cdc7d23d307d|2022-04-06 13:21:08.019096|
|1         |105  |T        |C        |[DYSTM, MITN]|[{DDDD, Analyse de test D, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 2, 0.0, 0, 1, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}]                                                                                                           |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 2, 0.0, 0, 1, 0.0, 0}, {1, 4, 0.25, 1, 2, 0.5, 0}}|69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA06, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, DDDD, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-105-T-C|ed857253d642d9afd7b4d432337f5f32e6d7a05d|2022-04-06 13:21:08.019096|
|1         |106  |T        |C        |[DYSTM, MITN]|[{FFFF, Analyse de test F, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}, {EEEE, Analyse de test E, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA05, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT1, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, EEEE, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {PA06, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, FFFF, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-106-T-C|6629228d210a293ef7f8867ef33856b48382c413|2022-04-06 13:21:08.019096|
|1         |107  |T        |C        |[DYSTM, MITN]|[{GGGG, Analyse de test G, {1, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}}]                                                                                                            |{{1, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA07, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GGGG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {PA08, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, GGGG, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-107-T-C|e8bd039de08294be77e78717ce5cf980940458c6|2022-04-06 13:21:08.019096|
|1         |108  |T        |C        |[DYSTM, MITN]|[{IIII, Analyse de test I, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}, {HHHH, Analyse de test H, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{1, 2, 0.5, 1, 1, 1.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA09, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, HHHH, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {PA10, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, IIII, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, false, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]|{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-108-T-C|e5996d385f1b724636e0a4d15373cbe0a447ff5e|2022-04-06 13:21:08.019096|
|1         |109  |T        |C        |[DYSTM, MITN]|[{JJJJ, Analyse de test J, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]                                                                                                            |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA11, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, JJJJ, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                 |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-109-T-C|dcc44974d289114fb1761bd87a8ce508729e1672|2022-04-06 13:21:08.019096|
|1         |110  |T        |C        |[DYSTM, MITN]|[{JJJJ, Analyse de test J, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]                                                                                                            |{{1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA11, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, JJJJ, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}]                                                                                                                                                                                                                                                                                                                                                                                                 |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-110-T-C|372e10fe659681bb7aef40746d4ed3bba3ff6625|2022-04-06 13:21:08.019096|
|1         |111  |T        |C        |[DYSTM, MITN]|[{LLLL, Analyse de test L, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}, {KKKK, Analyse de test K, {1, 2, 0.5, 1, 1, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {1, 2, 0.5, 1, 1, 1.0, 0}}]|{{2, 4, 0.5, 2, 2, 1.0, 0}, {0, 0, 0.0, 0, 0, 0.0, 0}, {2, 4, 0.5, 2, 2, 1.0, 0}} |69898|[OR4F5]     |chr1:g.69897T>C|SNV          |[29135816]|germline    |2022-04-06 13:21:08.019096|2022-04-06 13:21:08.019096|GRCh38          |2022-08-04            |T>C       |[{PA12, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, KKKK, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}, {PA13, 11111, 1, 30, [0, 1], 8.07, true, [PASS], 0, 30, 30, 1.0, chr1:g.69897T>C, SNV, BAT2, 2022-04-06, germline, SR0095, SRA0001, WXS, GRCh38, LLLL, Maladies musculaires (Panel global), FM00001, true, Male, PPR00101, OR00201, true, PA0003, PA0002, SP_696, 14-696, [0, 1], [0, 0], true, false, HET, HET, WT, mother, autosomal_dominant, false, [{null, null}], [{null, null}], false}] |{null, null, null, null, null, null}|rs200676709|null   |[{OR4F5, 777, 601013, HGNC:1392, ENSG00000198216, 1q25.3, calcium voltage-gated channel subunit alpha1 E, [BII, CACH6, CACNL1A6, Cav2.3, EIEE69, gm139], protein_coding, [{17827, Immunodeficiency due to a classical component pathway complement deficiency, [Autosomal recessive]}], [{HP:0001347, Hyperreflexia, Hyperreflexia (HP:0001347)}], [{Epileptic encephalopathy, early infantile, 69, 618285, [Autosomal dominant], [AD]}], [{OCULOAURICULAR SYNDROME}], [{[breast, colon, endometrial cancer under age 50]}]}]|[618285]|null   |[HPO, Orphanet, OMIM]  |[DBSNP, Pubmed]           |1-111-T-C|367dc794e56ba0f55701c0758c173313caa9a2be|2022-04-06 13:21:08.019096|
+----------+-----+---------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+-----+------------+---------------+-------------+----------+------------+--------------------------+--------------------------+----------------+----------------------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+-----------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+-------+-----------------------+--------------------------+---------+----------------------------------------+--------------------------+
*/
    val result = resultDf.as[VariantEnrichedOutput].collect()
/*
    // Use case #1: Un variant est présent dans la batch #1 et absent de la batch #2
    val result01 = result.find(_.`start` == 101).head
    result01.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "AAAA", "Analyse de test A",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result01.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 20, 0.05             , 1, 10, 0.1              , 0),
      Frequency(0, 6 , 0.0              , 0, 3 , 0.0              , 0),
      Frequency(1, 26, 0.038461538461538, 1, 13, 0.076923076923077, 0))
*/
/*
    // Use case #2: Un variant est absent de la batch #1 et présent dans la batch #2
    val result02 = result.find(_.`start` == 102).head
    result02.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "BBBB", "Analyse de test B",
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(1, 2, 0.5, 1, 1, 1.0, 0)))
    result02.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 20, 0.05             , 1, 10, 0.1              , 0),
      Frequency(0, 6 , 0.0              , 0, 3 , 0.0              , 0),
      Frequency(1, 26, 0.038461538461538, 1, 13, 0.076923076923077, 0))
*/
/*
    // Use case #3: Un variant est présent dans la batch #1 et présent dans la batch #2
    val result03 = result.find(_.`start` == 103).head
    result03.`frequencies_by_analysis` should contain allElementsOf List(
      AnalysisCodeFrequencies(
        "CCCC", "Analyse de test C",
        Frequency(2, 4, 0.5, 2, 2, 1.0, 0),
        Frequency(0, 0, 0.0, 0, 0, 0.0, 0),
        Frequency(2, 4, 0.5, 2, 2, 1.0, 0)))
    result03.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(2, 20, 0.1              , 2, 10, 0.2              , 0),
      Frequency(0, 6 , 0.0              , 0, 3 , 0.0              , 0),
      Frequency(2, 26, 0.076923076923077, 2, 13, 0.153846153846154, 0))
*/

/*
// Glitch d'affichage
    val result06 = result.find(_.`start` == 106).head
    result06.`frequency_RQDM` shouldBe AnalysisFrequencies(
      Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
      Frequency(1, 2, 0.5, 1, 1, 1.0, 0),
      Frequency(2, 4, 0.5, 2, 2, 1.0, 0))
*/
  }
*/

/*
  "variants job" should "run" in {
    new Variants().run(RunStep.initial_load)

    val resultDf = spark.table("clin.variants")
    val result = resultDf.as[VariantEnrichedOutput].collect().head

    result.`donors` should contain allElementsOf expectedDonors
    result.`frequencies_by_analysis` should contain allElementsOf List(AnalysisCodeFrequencies(
      `affected` = Frequency(4,6,0.6666666666666666,2,3,0.6666666666666666,2),
      `total` = Frequency(4,6,0.6666666666666666,2,3,0.6666666666666666,2)))

    result.copy(
      `donors` = List(),
      `frequencies_by_analysis` = List()
    ) shouldBe VariantEnrichedOutput(
      `pubmed` = Some(List("29135816")),
      `donors` = List(),
      `frequencies_by_analysis` = List(),
      `frequency_RQDM` = AnalysisFrequencies(
        `affected` = Frequency(4,6,0.6666666666666666,2,3,0.6666666666666666,2),
        `total` = Frequency(4,6,0.6666666666666666,2,3,0.6666666666666666,2)),
      `created_on` = result.`created_on`,
      `updated_on` = result.`updated_on`
    )
  }*/
}

