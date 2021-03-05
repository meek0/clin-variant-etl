package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.etl.Formats.{DELTA, JSON}

object FhirCatalog {



  object Raw {
    val alias = "raw"
    val path = "/raw/landing/fhir"
    val clinicalImpression = DataSource(alias, s"$path/ClinicalImpression", "", "", JSON)
    val group              = DataSource(alias, s"$path/Group"             , "", "", JSON)
    val observation        = DataSource(alias, s"$path/Observation"       , "", "", JSON)
    val organization       = DataSource(alias, s"$path/Organization"      , "", "", JSON)
    val patient            = DataSource(alias, s"$path/Patient"           , "", "", JSON)
    val practitioner       = DataSource(alias, s"$path/Practitioner"      , "", "", JSON)
    val practitionerRole   = DataSource(alias, s"$path/PractitionerRole"  , "", "", JSON)
    val serviceRequest     = DataSource(alias, s"$path/ServiceRequest"    , "", "", JSON)

  }

  object Normalized {
    val alias = "normalized"
    val path = "/normalized/fhir"
    val clinicalImpression = DataSource(alias, s"$path/ClinicalImpression", "clin_normalized", "fhir_ClinicalImpression", DELTA)
    val group              = DataSource(alias, s"$path/Group"             , "clin_normalized", "fhir_Group"             , DELTA)
    val observation        = DataSource(alias, s"$path/Observation"       , "clin_normalized", "fhir_Observation"       , DELTA)
    val organization       = DataSource(alias, s"$path/Organization"      , "clin_normalized", "fhir_Organization"      , DELTA)
    val patient            = DataSource(alias, s"$path/Patient"           , "clin_normalized", "fhir_Patient"           , DELTA)
    val practitioner       = DataSource(alias, s"$path/Practitioner"      , "clin_normalized", "fhir_Practitioner"      , DELTA)
    val practitionerRole   = DataSource(alias, s"$path/PractitionerRole"  , "clin_normalized", "fhir_PractitionerRole"  , DELTA)
    val serviceRequest     = DataSource(alias, s"$path/ServiceRequest"    , "clin_normalized", "fhir_ServiceRequest"    , DELTA)

  }

}
