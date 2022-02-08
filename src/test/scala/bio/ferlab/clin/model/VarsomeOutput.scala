package bio.ferlab.clin.model

import bio.ferlab.clin.etl.varsome.{ACMGAnnotation, VariantPublication}

import java.sql.Timestamp
import java.time.LocalDateTime

case class VarsomeOutput(
                          chromosome: String,
                          start: Long,
                          reference: String,
                          alternate: String,
                          variant_id: String,
                          `updated_on`: Timestamp = Timestamp.valueOf(LocalDateTime.now()),
                          publications: Option[VariantPublication] = None,
                          acmg_annotation: Option[ACMGAnnotation] = None,
                        )