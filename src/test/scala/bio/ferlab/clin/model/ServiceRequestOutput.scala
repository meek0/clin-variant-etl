package bio.ferlab.clin.model

import java.sql.{Date, Timestamp}

case class ServiceRequestOutput(`category`: String = "MedicalRequest",
                                `service_request_code`: String = "PHME",
                                `service_request_description`: String = "Prédisposition aux hématopathies malignes de l'enfant",
                                `resource_type`: String = "ServiceRequest",
                                `patient_id`: String = "31291",
                                `practitioner_id`: String = "c4becdcf-87e1-4fa7-ae87-9bbf555b1c4f",
                                `ref-clin-impression`: String = "32131",
                                `is-submitted`: Boolean = true,
                                `service_request_id`: String = "32130",
                                `version_id`: String = "2",
                                `medical_record_number`: String = "423180",
                                `authored_on`: Date = Date.valueOf("2021-03-04"),
                                `ingested_on`: Timestamp = Timestamp.valueOf("2021-03-05 08:26:56.0"),
                                `ingestion_file_name`: String = "/raw/landing/fhir/ServiceRequest/ServiceRequest_0_20210310_102715.json",
                                `updated_on`: Timestamp = Timestamp.valueOf("2021-02-15 11:55:04.06"),
                                `created_on`: Timestamp = Timestamp.valueOf("2021-02-15 11:55:04.06"),
                                `note`: List[NOTE] = List(NOTE()))

case class NOTE(practitioner_role_id: String = "PROLE-78a60801-cbab-4064-93c6-d85aeadc1edb",
                time: Timestamp = Timestamp.valueOf("2021-03-04 13:51:09.410"),
                text: String = "Service Request submitted.")
