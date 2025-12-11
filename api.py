import requests
import polars as pl
import polars.selectors as cs
import io

# API Adresse swagger : https://www.data.gouv.fr/dataservices/api-adresse-base-adresse-nationale-ban/

# 2.4
adresse = "88 avenue Verdier"
postcode = ''
url_ban_example = f'https://data.geopf.fr/geocodage/search?q={adresse}'

r = requests.get(url_ban_example, {'postcode': postcode})

# to explore json
import json
print(json.dumps(r.json()['features'], indent=2))
print(r.json()['features'][0].keys())

r.json().get('features')[0].get('properties')

localisation = r.json()['features']

# Turning the response to a dataframe and extracting lat and lon with unnest function
response_df = pl.DataFrame(localisation).select(['geometry', 'properties']).unnest(['properties'])
response_df = response_df.with_columns(
    lat=pl.col("geometry").struct['coordinates'].list.get(0),
    lon=pl.col("geometry").struct['coordinates'].list.get(1)
).drop('geometry')
response_df

import folium

def generate_map(lon, lat, zoom_start=15):
    # Create a map centered at the given coordinates
    m = folium.Map(location=[lat, lon], zoom_start=zoom_start)

    # Add a marker at the given coordinates
    folium.Marker([lat, lon], popup='Your Location').add_to(m)

    # Save the map to an HTML file
    m.save('map.html')

generate_map(lon=response_df[0,'lon'], lat=response_df[0,'lat'])

# 3.1
import duckdb

query = """
FROM read_parquet('https://minio.lab.sspcloud.fr/lgaliana/diffusion/BPE23.parquet')
SELECT NOMRS, NUMVOIE, INDREP, TYPVOIE, LIBVOIE,
       CADR, CODPOS, DEPCOM, DEP, TYPEQU,
       concat_ws(' ', NUMVOIE, INDREP, TYPVOIE, LIBVOIE) AS adresse, SIRET
WHERE DEP = '31'
      AND NOT (starts_with(TYPEQU, 'C6') OR starts_with(TYPEQU, 'C7'))
"""

bpe = duckdb.sql(query)
bpe = pl.from_pandas(bpe.to_df())
bpe = bpe.filter(pl.col('TYPEQU').str.starts_with('C'))

# 3.2
# API Education nationale https://www.data.gouv.fr/dataservices/annuaire-de-leducation-nationale/ 
siren_siret = '21310001900024'
annuaire_en_url = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=siren_siret%3D%27{siren_siret}%27'

r = requests.get(annuaire_en_url)
print(json.dumps(r.json()['records'][0]['record']['fields'], indent=2))

dep = '031'
offset = 0
limit = 100
annuaire_en_url_offset = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=code_departement=\'{dep}\'&limit={limit}&offset={offset}'
response = requests.get(annuaire_en_url_offset)
nb_obs = response.json()['total_count']
annuaire_en_df = pl.DataFrame(response.json()['records']).select('record').unnest('record').select('fields').unnest('fields').cast(pl.String)  # casting all columns to string to avoid errors when concatenating df

while nb_obs > len(annuaire_en_df):
    offset += limit
    annuaire_en_url_offset = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=code_departement=\'{dep}\'&limit={limit}&offset={offset}'
    print(f'fetching from {offset}th row')
    response = requests.get(annuaire_en_url2)
    annuaire_en_df = pl.concat([annuaire_en_df, pl.DataFrame([row['record']['fields'] for row in response.json()['records']], infer_schema_length=None).cast(pl.String)])
    print(f'length of data is now {len(annuaire_en_df)}')

bpe_enriched = (
    annuaire_en_df
        .select(pl.col('code_commune'), pl.col('nom_commune'), pl.col('nom_etablissement'), pl.col('latitude'), pl.col('longitude'), pl.col('siren_siret'))
        .join(bpe, left_on='siren_siret', right_on='SIRET', how='right')
        .with_columns(
            pl.col('adresse').str.strip_chars() 
        )
)

# No comma in the adresse fiedl
bpe_enriched.with_columns(pl.col('adresse').str.contains(',').alias('virg')).filter('virg')

(
bpe_enriched
    .select(pl.col('adresse'), pl.col('DEPCOM'), pl.col('nom_commune'))
    .write_csv('temp.csv')
)



headers = {
    'accept': 'text/csv',
}

files = [
    ('indexes', (None, 'address')),
    ('indexes', (None, 'poi')),
    ('data', ('temp.csv', open('temp.csv', 'rb'), 'text/csv')),
    ('citycode', (None, 'DEPCOM')),
    ('columns', (None, 'adresse'))
]

response = requests.post('https://data.geopf.fr/geocodage/search/csv', headers=headers, files=files)
bpe_loc = pl.read_csv(io.StringIO(response.text))

