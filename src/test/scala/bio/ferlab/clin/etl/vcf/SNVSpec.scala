package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class SNVSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    ))

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val group: DatasetConf = conf.getDataset("normalized_group")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")

  val patientDf: DataFrame = Seq(
    PatientOutput(
      `id` = "PA0001",
      `family_id` = "FM00001",
      `gender` = "male",
      `practitioner_role_id` = "PPR00101",
      `organization_id` = Some("OR00201"),
      `family_relationship` = List(FAMILY_RELATIONSHIP("PA0002", "FTH"), FAMILY_RELATIONSHIP("PA0003", "MTH")),
      `is_proband` = true
    ),
    PatientOutput(
      `id` = "PA0002",
      `family_id` = "FM00001",
      `gender` = "male",
      `family_relationship` = List(),
      `is_proband` = false
    ),
    PatientOutput(
      `id` = "PA0003",
      `family_id` = "FM00001",
      `gender` = "female",
      `family_relationship` = List(),
      `is_proband` = false
    )
  ).toDF()

  val groupDf: DataFrame = Seq(
    GroupOutput(
      `id` = "FM00000",
      `members` = List(
        MEMBERS("PA0001", `affected_status` = true),
        MEMBERS("PA0002", `affected_status` = true),
        MEMBERS("PA0003", `affected_status` = true)
      ),
      `version_id` = "1"
    ),
    GroupOutput(
      `id` = "FM00001",
      `members` = List(
        MEMBERS("PA0001", `affected_status` = true),
        MEMBERS("PA0002", `affected_status` = false),
        MEMBERS("PA0003", `affected_status` = true)
      ),
      `version_id` = "4"
    )
  ).toDF()

  val taskDf: DataFrame = Seq(
    TaskOutput(
      `id` = "73254",
      `patient_id` = "PA0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0002",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "22222")
    ),
    TaskOutput(
      `id` = "73257",
      `patient_id` = "PA0003",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "33333")
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA00095",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111")
    )
  ).toDF

  val serviceRequestDf: DataFrame = Seq(
    ServiceRequestOutput(),
    ServiceRequestOutput(`id` = "111")
  ).toDF()

  val specimenDf: DataFrame = Seq(
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SR0095", `sample_id` = Some("14-696"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SR0095", `sample_id` = None          , `specimen_id` =  Some("SP_696"))
  ).toDF

  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput(
      `genotypes` = List(
        GENOTYPES(),                                          //proband
        GENOTYPES(`sampleId` = "22222", `calls` = List(0, 0)),//father
        GENOTYPES(`sampleId` = "33333"))                      //mother
    )).toDF(),
    patient.id -> patientDf,
    group.id -> groupDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf,
    specimen.id -> specimenDf
  )


  "occurrences transform" should "transform data in expected format" in {
    val result = new SNV("BAT1").transform(data)
    result.as[NormalizedSNV].collect() should contain allElementsOf Seq(
      NormalizedSNV(
        `hc_complement` = List(),
        `possibly_hc_complement` = List(),
        `last_update` = Date.valueOf(LocalDate.now()))
    )

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedSNV", result, "src/test/scala/")
  }

  "getCompoundHet" should "return compound het for one patient and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), "father")
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )
  }
  it should "return compound het for one patient and multiple genes" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2"), "father"),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1", "BRAF2"), null),
      CompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2"), "father")
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1030-C-G", "1-1070-C-G")), HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")), HCComplement("BRAF2", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA001", "1", 1070, "C", "G", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1000-A-T"))))
    )

  }
  it should "return compound het for two patients and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), "father"),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1"), null),
      CompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), "mother"),
      CompoundHetInput("PA002", "1", 1050, "C", "G", Seq("BRAF1"), "father"),
    ).toDF()

    SNV.getCompoundHet(input).as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA002", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1050-C-G")))),
      CompoundHetOutput("PA002", "1", 1050, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )

  }

  "getPossiblyCompoundHet" should "return possibly compound het for many patients" in {
    val input = Seq(
      PossiblyCompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2")),
      PossiblyCompoundHetInput("PA001", "1", 1090, "C", "G", Seq("BRAF3")),
      PossiblyCompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      PossiblyCompoundHetInput("PA002", "1", 1030, "C", "G", Seq("BRAF1"))
    ).toDF()

    val result = SNV.getPossiblyCompoundHet(input).as[PossiblyCompoundHetOutput]
    result.collect() should contain theSameElementsAs Seq(
      PossiblyCompoundHetOutput("PA001", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2),PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2),PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1070, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA002", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      PossiblyCompoundHetOutput("PA002", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
    )


  }



}
