/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2022-12-05T17:38:33.300234
 */
package bio.ferlab.clin.model

import java.sql.Date


case class CoverageByGeneCentricOutput(`gene`: String = "A1BG",
                                  `size`: Long = 1469,
                                  `average_coverage`: Double = 538.5268890401634,
                                  `coverage5`: Float = 1.0f,
                                  `coverage15`: Float = 0.9f,
                                  `coverage30`: Float = 0.8f,
                                  `coverage50`: Float = 0.7f,
                                  `coverage100`: Float = 0.6f,
                                  `coverage200`: Float = 0.5f,
                                  `coverage300`: Float = 0.9489396868618108f,
                                  `coverage400`: Float = 0.7099923076923078f,
                                  `coverage500`: Float = 0.484001361470388f,
                                  `coverage1000`: Float = 0.0f,
                                  `aliquot_id`: String = "aliquot1",
                                  `batch_id`: String = "BAT1",
                                  `chromosome`: String = "1",
                                  `start`: Long = 10000,
                                  `end`: Long = 10059,
                                  `service_request_id`: String = "SR0095",
                                  `ensembl_gene_id`: String = "ENS1230912",
                                  `omim_gene_id`: String = "365432",
                                  `panels`: Seq[String] = Seq("DYSTM", "MITN"),
                                  `hash`: String = "f0f5be6ab4e10640235ca5529401ade728c94736"
                                 )