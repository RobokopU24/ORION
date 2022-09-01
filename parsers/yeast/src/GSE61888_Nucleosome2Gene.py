import pandas as pd

nuc_df = pd.read_csv('/SGD_Data_Storage/GSE61888_nucs_normed.csv')
gene_df = pd.read_csv('/SGD_Data_Storage/SGDAllGenes.csv')

new_nuc_df = nuc_df.merge(gene_df, left_on='acc', right_on='secondaryIdentifier')

new_nuc_df.to_csv('/SGD_Data_Storage/GSE61888_nucs_normed_updated.csv', encoding="utf-8-sig", index=False)