import requests
import polars as pl
import os
import boto3
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

s3_client = boto3.client('s3')

file_name = 'GCST90302052'

manifest = pl.read_csv('metabolite_manifest.tsv', separator='\t')

file_names = manifest['accessionId'].to_list()
failed_urls = []

def process_file(file_name):
    download_url = f'https://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90302001-GCST90303000/{file_name}/{file_name}.tsv'
    response = requests.get(download_url)
    if response.status_code == 200:
        # Write the content to a TSV file
        with open('data.tsv', 'wb') as file:
            file.write(response.content)
        print(f"The {file_name} file has been downloaded successfully.")

        try:
            df = pl.read_csv('data.tsv', separator='\t')
            df = (
                df
                .rename({
                    'effect_allele':'effect_allele',
                    'other_allele':'other_allele',
                    'effect_allele_frequency':'eaf', 
                    'beta':'beta',
                    'standard_error':'se',
                    'p_value':'pval',
                    'chromosome':'chr',
                    'base_pair_location':'pos'
                })
            )

            df = df.select(['chr', 'pos', 'effect_allele', 'other_allele', 'eaf', 'beta', 'se', 'pval'])

            for chrom in df['chr'].unique().to_list():
                partition_df = df.filter(pl.col('chr') == chrom)
                partition_key = f"TER/Metabolite/chr{chrom}/{file_name}.parquet"

                buffer = BytesIO()
                partition_df.write_parquet(buffer)
                buffer.seek(0)
                s3_client.put_object(Bucket='primula-genetic-consulting-datalake', Key=partition_key, Body=buffer)
            print(f"The {file_name} file has been uploaded successfully.")
        except:
            print(f"The {file_name} file failed.")
            failed_urls.append(file_name)

    else:
        failed_urls.append(file_name)
        print('Failed to download the file', response.status_code)


with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(tqdm(executor.map(process_file, file_names)))


print(failed_urls)
    