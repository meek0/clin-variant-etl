package bio.ferlab.clin.etl.migration

import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.StructType

/**
 * This app convert all public tables from  parquet to delta tables
 */
object ConvertPublicTablesToDelta extends SparkApp {

  implicit val (conf, _, spark) = init(appName = s"Convert Public tables to parquet")

  Seq(
    "normalized_1000_genomes",
    "normalized_clinvar",
    "normalized_cosmic_gene_set",
    "normalized_dbnsfp",
    "normalized_dbnsfp_annovar",
    "enriched_dbnsfp",
    "normalized_dbsnp",
    "normalized_ddd_gene_set",
    "normalized_ensembl_mapping",
    "enriched_genes",
    "normalized_human_genes",
    "normalized_hpo_gene_set",
    "normalized_omim_gene_set",
    "normalized_orphanet_gene_set",
    "normalized_topmed_bravo",
    "normalized_mane_summary",
    "normalized_refseq_annotation"
  ).foreach { d =>
    val ds = conf.getDataset(d)
    val partitionSchema: StructType = ds.read.schema
      .filter(s => ds.partitionby.contains(s.name))
      .foldLeft(new StructType()) { case (struct, f) => struct.add(f) }
    DeltaTable.convertToDelta(spark, ds.table.map(_.fullName).getOrElse(s"parquet.`${ds.location}`"), partitionSchema)
  }
}

