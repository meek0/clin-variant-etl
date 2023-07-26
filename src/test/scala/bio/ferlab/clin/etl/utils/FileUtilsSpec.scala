package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.etl.utils.FileUtils.filesUrlFromDF
import bio.ferlab.clin.model.{Content, DOCUMENTS, DocumentReferenceOutput, EXPERIMENT, TaskOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  "filesUrlFromDF" should "return list of expectd urls for a given batch id" in {
    val tasks = Seq(
      TaskOutput(
        batch_id = "B1",
        experiment = EXPERIMENT(name = "B1"),
        documents = List(
          DOCUMENTS(id = "exo1", document_type = "EXOMISER"), //Should be included
          DOCUMENTS(id = "exo2", document_type = "EXOMISER"), //Should be included
          DOCUMENTS(id = "snv3", document_type = "SNV") //Should be excluded because wrong data type
        )
      ),
      TaskOutput(
        batch_id = "B1",
        experiment = EXPERIMENT(name = "B1"),
        documents = List(
          DOCUMENTS(id = "exo3", document_type = "EXOMISER"), //Should be included
          DOCUMENTS(id = "exo4", document_type = "EXOMISER") //Should be excluded because of missing format in associated document

        )
      ),
      TaskOutput(
        batch_id = "B2",
        experiment = EXPERIMENT(name = "B2"),
        documents = List(
          DOCUMENTS(id = "exo5", document_type = "EXOMISER") //Should be excluded because wrong batch
        )
      ),

    ).toDF

    val documents = Seq(
      DocumentReferenceOutput(id = "exo1", contents = List(
        Content(s3_url = "s3a://file1.tsv", format = "TSV"), //Should be included
        Content(s3_url = "s3a://file1.json", format = "JSON")
      )),
      DocumentReferenceOutput(id = "exo2", contents = List(
        Content(s3_url = "s3a://file2.tsv", format = "TSV"), //Should be included
        Content(s3_url = "s3a://file2b.tsv", format = "TSV") //Should be included
      ))
      ,
      DocumentReferenceOutput(id = "snv3", `type` = "SNV", contents = List(
        Content(s3_url = "s3a://file3snv.tsv", format = "TSV") //Should be excluded, wrong data type (SNV)
      )),
      DocumentReferenceOutput(id = "exo3", contents = List(
        Content(s3_url = "s3a://file3.tsv", format = "TSV") //Should be included
      )),
      DocumentReferenceOutput(id = "exo4", `type` = "SNV", contents = List(
        Content(s3_url = "s3a://file4.json", format = "JSON") //Should be excluded, wrong format
      )),
      DocumentReferenceOutput(id = "exo5", `type` = "SNV", contents = List(
        Content(s3_url = "s3a://file5.tsv", format = "TSV") //Should be excluded, wrong batch
      )),
    ).toDF()

    val results = filesUrlFromDF(tasks = tasks, documentReferences = documents, batchId = "B1", dataType = "EXOMISER", format = "TSV")
    results should contain theSameElementsAs Seq(
      FileInfo("s3a://file1.tsv", "16868", "438787", "440171"),
      FileInfo("s3a://file2.tsv", "16868", "438787", "440171"),
      FileInfo("s3a://file2b.tsv", "16868", "438787", "440171"),
      FileInfo("s3a://file3.tsv", "16868", "438787", "440171")
    )

  }

}
