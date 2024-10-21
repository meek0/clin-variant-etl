/**
 * Generated by [[bio.ferlab.datalake.testutils.ClassGenerator]]
 * on 2024-10-16T16:29:42.061063
 */
package bio.ferlab.clin.model.nextflow


case class SVClusteringParentalOrigin(`analysis_service_request_id`: String = "SRA0001",
                                      `aliquot_id`: String = "11111",
                                      `chromosome`: String = "1",
                                      `start`: Long = 9823629,
                                      `reference`: String = "A",
                                      `alternate`: String = "<DEL>",
                                      `name`: String = "DRAGEN:LOSS:chr1:9823628-9823687",
                                      `calls`: Seq[Int] = Seq(0, 1),
                                      `members`: Seq[String] = Seq("DRAGEN:LOSS:chr1:9823628-9823687"),
                                      `batch_id`: String = "BAT1",
                                      `is_multi_allelic`: Boolean = false,
                                      `service_request_id`: String = "SRS0001",
                                      `patient_id`: String = "PA0001",
                                      `gender`: String = "Male",
                                      `family_id`: String = "FM00001",
                                      `mother_id`: Option[String] = Some("PA0003"),
                                      `father_id`: Option[String] = Some("PA0002"),
                                      `affected_status`: Boolean = true,
                                      `mother_calls`: Option[Seq[Int]] = Some(Seq(0, 1)),
                                      `father_calls`: Option[Seq[Int]] = Some(Seq(0, 0)),
                                      `mother_affected_status`: Option[Boolean] = Some(true),
                                      `father_affected_status`: Option[Boolean] = Some(false),
                                      `zygosity`: String = "HET",
                                      `parental_origin`: Option[String] = Some("mother"),
                                      `transmission`: String = "autosomal_dominant")