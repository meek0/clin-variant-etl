package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.core.etl.{DataSource, Partitioning}
import bio.ferlab.datalake.core.loader.Formats._
import bio.ferlab.datalake.core.loader.LoadTypes._


object FhirCatalog {

  object Raw {
    val alias = "raw"
    val path = "/raw/landing/fhir"
    val clinicalImpression = DataSource(alias, s"$path/ClinicalImpression", "", "", JSON, OverWrite, Partitioning.default)
    val group              = DataSource(alias, s"$path/Group"             , "", "", JSON, OverWrite, Partitioning.default)
    val observation        = DataSource(alias, s"$path/Observation"       , "", "", JSON, OverWrite, Partitioning.default)
    val organization       = DataSource(alias, s"$path/Organization"      , "", "", JSON, OverWrite, Partitioning.default)
    val patient            = DataSource(alias, s"$path/Patient"           , "", "", JSON, OverWrite, Partitioning.default)
    val practitioner       = DataSource(alias, s"$path/Practitioner"      , "", "", JSON, OverWrite, Partitioning.default)
    val practitionerRole   = DataSource(alias, s"$path/PractitionerRole"  , "", "", JSON, OverWrite, Partitioning.default)
    val serviceRequest     = DataSource(alias, s"$path/ServiceRequest"    , "", "", JSON, OverWrite, Partitioning.default)

  }

  object Normalized {
    val alias = "normalized"
    val path = "/normalized/fhir"
    val clinical_impression = DataSource(alias, s"$path/ClinicalImpression", "clin_normalized", "clinical_impression", DELTA, Upsert, Partitioning.default)
    val group               = DataSource(alias, s"$path/Group"             , "clin_normalized", "group"              , DELTA, Upsert, Partitioning.default)
    val observation         = DataSource(alias, s"$path/Observation"       , "clin_normalized", "observation"        , DELTA, Upsert, Partitioning.default)
    val organization        = DataSource(alias, s"$path/Organization"      , "clin_normalized", "organization"       , DELTA, Upsert, Partitioning.default)
    val patient             = DataSource(alias, s"$path/Patient"           , "clin_normalized", "patient"            , DELTA, Upsert, Partitioning.default)
    val practitioner        = DataSource(alias, s"$path/Practitioner"      , "clin_normalized", "practitioner"       , DELTA, Upsert, Partitioning.default)
    val practitioner_role   = DataSource(alias, s"$path/PractitionerRole"  , "clin_normalized", "practitioner_role"  , DELTA, Upsert, Partitioning.default)
    val service_request     = DataSource(alias, s"$path/ServiceRequest"   , "clin_normalized", "service_request"     , DELTA, Upsert, Partitioning.default)

  }

}
