package bio.ferlab.clin.model.normalized.fhir

import java.sql.Timestamp

case class NormalizedDocumentReference(id: String = "438780",
                                       `type`: String = "EXOMISER",
                                       category: String = "GENO",
                                       patient_id: String = "438787",
                                       specimen_id: String = "440171",
                                       organization_id: String = "LDM-CHUSJ",
                                       master_identifier: String = "blue/71916f68-957b-4263-a6b7-5a91dea456d6.html",
                                       ingestion_file_name: String = "/raw/landing/fhir/DocumentReference/DocumentReference.json",
                                       contents: List[Content] = List(Content(), Content(url = "https://ferload.qa.cqgc.hsj.rtss.qc.ca/blue/71916f68-957b-4263-a6b7-5a91dea456d6.json", s3_url = "s3a://test/blue/71916f68-957b-4263-a6b7-5a91dea456d6.json", format = "JSON"), Content(url = "https://ferload.qa.cqgc.hsj.rtss.qc.ca/blue/71916f68-957b-4263-a6b7-5a91dea456d6.variants.tsv", s3_url = "s3a://test/blue/71916f68-957b-4263-a6b7-5a91dea456d6.variants.tsv", format = "TSV")),
                                       ingested_on: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"),
                                       updated_on: Timestamp = Timestamp.valueOf("2021-11-23 09:40:49.947"),
                                       created_on: Timestamp = Timestamp.valueOf("2021-11-23 09:40:49.947"),
                                      )

case class Content(url: String = "https://ferload.qa.cqgc.hsj.rtss.qc.ca/blue/71916f68-957b-4263-a6b7-5a91dea456d6.html", s3_url: String = "s3a://test/blue/71916f68-957b-4263-a6b7-5a91dea456d6.html", format: String = "HTML")