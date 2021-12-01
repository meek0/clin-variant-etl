package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import bio.ferlab.datalake.commons.config.Format._
import bio.ferlab.datalake.commons.config.LoadType._


object FhirCatalog {

  object Raw {
    val alias = "raw"
    val path = "/raw/landing/fhir"
    val clinicalImpression = DatasetConf("clinicalImpression", alias, s"$path/ClinicalImpression", JSON, OverWrite)
    val group              = DatasetConf("group"             , alias, s"$path/Group"             , JSON, OverWrite)
    val observation        = DatasetConf("observation"       , alias, s"$path/Observation"       , JSON, OverWrite)
    val organization       = DatasetConf("organization"      , alias, s"$path/Organization"      , JSON, OverWrite)
    val patient            = DatasetConf("patient"           , alias, s"$path/Patient"           , JSON, OverWrite)
    val practitioner       = DatasetConf("practitioner"      , alias, s"$path/Practitioner"      , JSON, OverWrite)
    val practitionerRole   = DatasetConf("practitionerRole"  , alias, s"$path/PractitionerRole"  , JSON, OverWrite)
    val serviceRequest     = DatasetConf("serviceRequest"    , alias, s"$path/ServiceRequest"    , JSON, OverWrite)
    val specimen           = DatasetConf("specimen"          , alias, s"$path/Specimen"          , JSON, OverWrite)
    val task               = DatasetConf("task"              , alias, s"$path/Task"              , JSON, OverWrite)

  }

  object Normalized {
    val alias = "normalized"
    val path = "/normalized/fhir"
    val clinical_impression = DatasetConf("clinical_impression", alias, s"$path/ClinicalImpression", DELTA, Upsert, TableConf("clin", "clinical_impression"))
    val group               = DatasetConf("group"              , alias, s"$path/Group"             , DELTA, Upsert, TableConf("clin", "group"))
    val observation         = DatasetConf("observation"        , alias, s"$path/Observation"       , DELTA, Upsert, TableConf("clin", "observation"))
    val organization        = DatasetConf("organization"       , alias, s"$path/Organization"      , DELTA, Upsert, TableConf("clin", "organization"))
    val patient             = DatasetConf("patient"            , alias, s"$path/Patient"           , DELTA, Upsert, TableConf("clin", "patient"))
    val practitioner        = DatasetConf("practitioner"       , alias, s"$path/Practitioner"      , DELTA, Upsert, TableConf("clin", "practitioner"))
    val practitioner_role   = DatasetConf("practitioner_role"  , alias, s"$path/PractitionerRole"  , DELTA, Upsert, TableConf("clin", "practitioner_role"))
    val service_request     = DatasetConf("service_request"    , alias, s"$path/ServiceRequest"    , DELTA, Upsert, TableConf("clin", "service_request"))
    val specimen            = DatasetConf("specimen"           , alias, s"$path/Specimen"          , DELTA, Upsert, TableConf("clin", "specimen"))
    val task                = DatasetConf("task"               , alias, s"$path/Task"              , DELTA, Upsert, TableConf("clin", "task"))
  }

}
