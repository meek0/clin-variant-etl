package bio.ferlab.clin.etl.qc

package object dictionary {

  val DicAffectedStatusCode = Seq(
    "not_affected",
    "affected",
    "unknown",
  )

  val DicChromosome = Seq(
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "X",
    "Y",
    "null",
  )

  val DicClinvar = Seq(
    "Affects",
    "association",
    "association_not_found",
    "Benign",
    "confers_sensitivity",
    "Conflicting_interpretations_of_pathogenicity",
    "drug_response",
    "Likely_benign",
    "Likely_pathogenic",
    "Likely_risk_allele",
    "low_penetrance",
    "not_provided",
    "null",
    "other",
    "Pathogenic",
    "protective",
    "risk_factor",
    "Uncertain_risk_allele",
    "Uncertain_significance",
    "no_classification_for_the_single_variant",
    "Conflicting_classifications_of_pathogenicity",
    "no_classifications_from_unflagged_records",
  )

  val DicCmcTier = Seq(
    "1",
    "2",
    "3",
    "Other",
    "null",
  )

  val DicCNVFilters = Seq(
    "PASS",
    "cnvQual",
    "cnvCopyRatio",
    "LoDFail",
    "binCount",
    "segmentMean",
    "null",
  )

  val DicCNVType = Seq(
    "GAIN",
    "LOSS",
    "GAINLOH",
    "CNLOH",
    "null",
  )

  val DicConsequences = Seq(
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "transcript_amplification",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "splice_region_variant",
    "splice_donor_5th_base_variant",
    "splice_donor_region_variant",
    "splice_polypyrimidine_tract_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "feature_elongation",
    "regulatory_region_variant",
    "feature_truncation",
    "intergenic_variant",
    "null",
  )

  val DicExoACMGClass = Seq(
    "BENIGN",
    "LIKELY_BENIGN",
    "LIKELY_PATHOGENIC",
    "PATHOGENIC",
    "UNCERTAIN_SIGNIFICANCE",
    "null"
  )

  val DicExoACMGEviMax = Seq(
    "PVS1",
    "PS2",
    "PM2",
    "PM3",
    "PM4",
    "PP3",
    "PP4",
    "PP5",
    "BA1",
    "BS4",
    "BP2",
    "BP4",
    "BP6",
    "BP4_VeryStrong",
    "BP4_Moderate",
    "BP4_Strong",
    "BP6_Strong",
    "PP3_Moderate",
    "PP3_Strong",
    "PP5_Strong",
    "PVS1_Strong",
    "null"
  )

  val DicFatherZygosity = Seq(
    "HOM",
    "HEM",
    "HET",
    "WT",
    "UNK",
    "null",
  )

  val DicFathmmPred = Seq(
    "D",
    "T",
    "null",
  )

  val DicFilters = Seq(
    "PASS",
    "DRAGENSnpHardQUAL",
    "DRAGENIndelHardQUAL",
    "LowDepth",
    "PloidyConflict",
    "base_quality",
    "lod_fstar",
    "mapping_quality",
    "weak_evidence",
    "no_reliable_supporting_read",
    "systematic_noise",
    "filtered_reads",
    "fragment_length",
    "too_few_supporting_reads",
    "low_frac_info_reads",
    "read_position",
    "multiallelic",
    "long_indel",
    "low_tlen",
    "non_homref_normal",
    "low_normal_depth",
    "alt_allele_in_normal",
    "noisy_normal",
    "null",
  )

  val DicFraACMGClassMax = Seq(
    "BENIGN",
    "LIKELY_BENIGN",
    "LIKELY_PATHOGENIC",
    "PATHOGENIC",
    "UNCERTAIN_SIGNIFICANCE",
    "POSSIBLY_PATHOGENIC_MODERATE",
    "POSSIBLY_PATHOGENIC_BENIGN",
    "POSSIBLY_PATHOGENIC_LOW",
    "POSSIBLY_BENIGN",
    "null"
  )

  val DicFraACMGEviMax = Seq(
    "PVS1",
    "PS1",
    "PS2",
    "PS3",
    "PS4",
    "PM1",
    "PM2",
    "PM3",
    "PM4",
    "PM5",
    "PP1",
    "PP2",
    "PP3",
    "PP4",
    "PP5",
    "BA1",
    "BS1",
    "BS2",
    "BS3",
    "BS4",
    "BP1",
    "BP3",
    "BP4",
    "BP5",
    "BP6",
    "null"
  )

  val DicGender = Seq(
    "Female",
    "Male",
    "other",
    "unknown",
  )

  val DicGeneExternalReference = Seq(
    "OMIM",
    "HPO",
    "Orphanet",
    "SpliceAI",
    "Cosmic",
    "gnomAD",
    "DDD",
    "null",
  )

  val DicGenesBiotype = Seq(
    "IG_C_gene",
    "IG_D_gene",
    "IG_J_gene",
    "IG_LV_gene",
    "IG_V_gene",
    "TR_C_gene",
    "TR_J_gene",
    "TR_V_gene",
    "TR_D_gene",
    "IG_pseudogene",
    "IG_C_pseudogene",
    "IG_J_pseudogene",
    "IG_V_pseudogene",
    "TR_V_pseudogene",
    "TR_J_pseudogene",
    "Mt_rRNA",
    "Mt_tRNA",
    "miRNA",
    "misc_RNA",
    "rRNA",
    "scRNA",
    "snRNA",
    "snoRNA",
    "ribozyme",
    "sRNA",
    "scaRNA",
    "Non-coding",
    "lncRNA",
    "Mt_tRNA_pseudogene",
    "tRNA_pseudogene",
    "snoRNA_pseudogene",
    "snRNA_pseudogene",
    "scRNA_pseudogene",
    "rRNA_pseudogene",
    "misc_RNA_pseudogene",
    "miRNA_pseudogene",
    "TEC",
    "nonsense_mediated_decay",
    "non_stop_decay",
    "retained_intron",
    "protein_coding",
    "protein_coding_LoF",
    "protein_coding_CDS_not_defined",
    "processed_transcript",
    "non_coding",
    "ambiguous_orf",
    "sense_intronic",
    "sense_overlapping",
    "antisense_RNA",
    "known_ncrna",
    "pseudogene",
    "processed_pseudogene",
    "polymorphic_pseudogene",
    "retrotransposed",
    "transcribed_processed_pseudogene",
    "transcribed_unprocessed_pseudogene",
    "transcribed_unitary_pseudogene",
    "translated_processed_pseudogene",
    "translated_unprocessed_pseudogene",
    "unitary_pseudogene",
    "unprocessed_pseudogene",
    "artifact",
    "lincRNA",
    "macro_lncRNA",
    "3prime_overlapping_ncRNA",
    "disrupted_domain",
    "vault_RNA",
    "bidirectional_promoter_lncRNA",
    "null",
  )

  val DicHotspot = Seq(
    "true",
    "false",
    "null",
  )

  val DicIsHc = Seq(
    "true",
    "false",
  )

  val DicIsPossiblyHc = Seq(
    "true",
    "false",
  )

  val DicLrtPred = Seq(
    "D",
    "N",
    "U",
    "null",
  )

  val DicMotherZygosity = Seq(
    "HOM",
    "HEM",
    "HET",
    "WT",
    "UNK",
    "null",
  )

  val DicOMIM = Seq(
    "AD",
    "AR",
    "DD",
    "DR",
    "IC",
    "Mi",
    "Mu",
    "NRT",
    "SMo",
    "Smu",
    "XL",
    "XLD",
    "XLR",
    "YL",
    "?AD",
    "?AR",
    "?DD",
    "?DR",
    "?IC",
    "?Mi",
    "?Mu",
    "?SMo",
    "?Smu",
    "?XL",
    "?XLD",
    "?XLR",
    "?YL",
    "null"
  )

  val DicPanels = Seq(
    "HYPM",
    "MYOC",
    "RGDI",
    "TUHEM",
    "MYAC",
    "MMG",
    "DYSM",
    "MITN",
    "TUPED",
    "POLYM",
    "RGDI+",
    "RHAB",
    "SSOLID",
    "SHEMA",
    "EXTUM",
    "SCID",
    "THBP",
    "HLH",
    "IEI",
    "VEOIBD",
    "RGDIEP",
    "EPILEP"
  )

  val DicParentalOrigin = Seq(
    "denovo",
    "possible_denovo",
    "both",
    "mother",
    "father",
    "possible_father",
    "possible_mother",
    "ambiguous",
    "unknown",
    "null",
  )

  val DicPolyphen2HvarPred = Seq(
    "B",
    "D",
    "P",
    "null",
  )

  val DicSiftPred = Seq(
    "D",
    "T",
    "null",
  )

  val DicTransmission = Seq(
    "autosomal_dominant_de_novo",
    "autosomal_dominant",
    "autosomal_recessive",
    "x_linked_dominant_de_novo",
    "x_linked_recessive_de_novo",
    "x_linked_dominant",
    "x_linked_recessive",
    "non_carrier_proband",
    "unknown_parents_genotype",
    "unknown_father_genotype",
    "unknown_mother_genotype",
    "unknown_proband_genotype",
    "null",
  )

  val DicVariantClass = Seq(
    "insertion",
    "deletion",
    "SNV",
    "indel",
    "substitution",
    "sequence_alteration",
    "null",
  )

  val DicVariantExternalReference = Seq(
    "DBSNP",
    "Clinvar",
    "PubMed",
    "Cosmic",
    "Franklin",
    "gnomAD",
    "null",
  )

  val DicVepImpact = Seq(
    "HIGH",
    "MODERATE",
    "LOW",
    "MODIFIER",
    "null",
  )

  val DicZygosity = Seq(
    "HOM",
    "HEM",
    "HET",
  )

}
