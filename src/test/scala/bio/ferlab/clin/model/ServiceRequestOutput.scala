package bio.ferlab.clin.model

import java.sql.{Date, Timestamp}

case class ServiceRequestOutput(`category`: String = "MedicalRequest",
                                `serviceRequestCode`: String = "PHME",
                                `serviceRequestDescription`: String = "Prédisposition aux hématopathies malignes de l'enfant",
                                `resourceType`: String = "ServiceRequest",
                                `patientId`: String = "31291",
                                `practitionerId`: String = "c4becdcf-87e1-4fa7-ae87-9bbf555b1c4f",
                                `ref-clin-impression`: String = "32131",
                                `is-submitted`: Boolean = true,
                                `id`: String = "32130",
                                `versionId`: String = "2",
                                `medical_record_number`: String = "423180",
                                `authoredOn`: Date = Date.valueOf("2021-03-04"),
                                `ingestedOn`: Timestamp = Timestamp.valueOf("2021-03-05 08:26:56.0"),
                                `ingestionFileName`: String = "/raw/landing/fhir/ServiceRequest/ServiceRequest_0_20210310_102715.json",
                                `updatedOn`: Timestamp = Timestamp.valueOf("2021-02-15 11:55:04.06"),
                                `createdOn`: Timestamp = Timestamp.valueOf("2021-02-15 11:55:04.06"),
                                `note`: List[NOTE] = List(NOTE()))

case class NOTE(practitionerRoleId: String = "PROLE-78a60801-cbab-4064-93c6-d85aeadc1edb",
                time: Timestamp = Timestamp.valueOf("2021-03-04 13:51:09.410"),
                text: String = "Service Request submitted.")
