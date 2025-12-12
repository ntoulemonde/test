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

# 3.1 - _get.qmd
#| echo: true
#| label: read-bpe-parquet
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
bpe.glimpse()  # Contains other equipment as mairies, etc. Need to filter to C

bpe = bpe.filter(pl.col('TYPEQU').str.starts_with('C'))
bpe.glimpse()  # Now only ~1k equipments

# 3.2 - _exo2_solution
# API Education nationale https://www.data.gouv.fr/dataservices/annuaire-de-leducation-nationale/ 
siren_siret = '21310001900024'
annuaire_en_url = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=siren_siret%3D%27{siren_siret}%27'

r = requests.get(annuaire_en_url)
print(json.dumps(r.json()['records'][0]['record']['fields'], indent=2))

#| echo: true
#| output: false
#| label: exercise2-api-tabular
import requests
import polars as pl

# Initialize the initial API URL
dep = '031'
offset = 0
limit = 100
url_api_datagouv = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=code_departement=\'{dep}\'&limit={limit}&offset={offset}'

# First request to get total obs and first df
response = requests.get(url_api_datagouv)
nb_obs = response.json()['total_count']
schools_dep31 = pl.DataFrame(response.json()['records']).select('record').unnest('record').select('fields').unnest('fields').cast(pl.String)  # casting all columns to string to avoid errors when concatenating df

# Loop on the nb of obs
while nb_obs > len(schools_dep31):
    try:
      # Increase offset for first reply sent and update API url
      offset += limit
      url_api_datagouv = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=code_departement=\'{dep}\'&limit={limit}&offset={offset}'

      # Call API
      print(f'fetching from {offset}th row')
      response = requests.get(url_api_datagouv)
      
      # Concatenate the data from this call to previous data
      page_data = pl.DataFrame(response.json()['records']).select('record').unnest('record').select('fields').unnest('fields').cast(pl.String)
      schools_dep31 = pl.concat([schools_dep31, page_data])
      print(f'length of data is now {len(schools_dep31)}')

    except requests.exceptions.RequestException as e:
      print(f"An error occurred: {e}")
      break

#| echo: true
#| label: exercise2-bpe-enriched

bpe_enriched = (
    schools_dep31
        .select(pl.col('code_commune'), pl.col('nom_commune'), pl.col('nom_etablissement'), pl.col('latitude'), pl.col('longitude'), pl.col('siren_siret'))
        .join(bpe, left_on='siren_siret', right_on='SIRET', how='right')
        .with_columns(
            pl.col('adresse').str.strip_chars() 
        )
)

bpe_enriched.head(2).glimpse()

# No comma in the adresse fiedl
bpe_enriched.with_columns(pl.col('adresse').str.contains(',').alias('virg')).filter('virg')

bpe_enriched = bpe_enriched.with_columns(pl.col('adresse').str.replace(',', ' '))

# Ex 3 solution
#| label: exercise3-q1
import pathlib
output_path = pathlib.Path("data/output")
output_path.mkdir(parents=True, exist_ok=True)
csv_file = output_path / "bpe_before_geoloc.csv"

bpe_enriched = bpe_enriched.with_columns(pl.col('adresse').str.replace(',', ' '))

(
bpe_enriched
    .select(['adresse', 'DEPCOM', 'nom_commune'])
    .write_csv(csv_file)
)

import io

headers = {
    'accept': 'text/csv',
}

files = [
    ('indexes', (None, 'address')),
    ('indexes', (None, 'poi')),
    ('data', (str(csv_file), open(csv_file, 'rb'), 'text/csv')),
    ('citycode', (None, 'DEPCOM')),
    ('columns', (None, 'adresse'))
]

response = requests.post(
        "https://data.geopf.fr/geocodage/search/csv/",
        headers=headers,
        files=files
    )

bpe_loc = pl.read_csv(io.StringIO(response.text))

