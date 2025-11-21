import requests
import polars as pl

# to explore json 
import json
print(json.dumps(r.json()['features'], indent=2))
print(r.json()['features'][0].keys())

adresse = "88 avenue Verdier"
postcode = ''
url_ban_example = f'https://data.geopf.fr/geocodage/search?q={adresse}'

r = requests.get(url_ban_example, {'postcode': postcode})
localisation = r.json()['features']
for row in localisation:
    row['properties'].update({'lon': row['geometry']['coordinates'][0], 'lat': row['geometry']['coordinates'][1]})

response_df = pl.DataFrame([row['properties'] for row in localisation])
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
bpe.filter(pl.col('TYPEQU').str.starts_with('C'))

siren_siret = '21310001900024'
annuaire_en_url = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=siren_siret%3D%27{siren_siret}%27'

r = requests.get(annuaire_en_url)
print(json.dumps(r.json()['records'][0]['record']['fields'], indent=2))

annuaire_en_url2 = f'https://data.education.gouv.fr/api/v2/catalog/datasets/fr-en-annuaire-education/records?where=code_departement%3D%27031%27'
r2 = requests.get(annuaire_en_url2)
print(json.dumps(r2.json()['total_count']))
print(json.dumps(r2.json()['records'], indent=2))
len(r2.json()['records'])
len(r2.json()['records'][0]['record']['fields'])
annuaire_en_31 = r2.json()['records']

limit=100&offset=0
pl.DataFrame([row['record']['fields'] for row in r2.json()['records']])
