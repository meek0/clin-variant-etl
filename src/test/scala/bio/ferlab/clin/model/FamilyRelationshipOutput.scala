package bio.ferlab.clin.model

case class FamilyRelationshipOutput(`family_relationship_id`: String = "FR0001",
                                    `modified_at`: String = "2019-04-08T16:45:13.223277+00:00",
                                    `created_at`: String = "2018-06-12T17:27:48.141263+00:00",
                                    `patient1`: String = "PA0002",
                                    `patient2`: String = "PA0003",
                                    `patient1_to_patient2_relation`: String = "Father",
                                    `patient2_to_patient1_relation`: String = "Child",
                                    `batch_id`: String = "BAT1")
