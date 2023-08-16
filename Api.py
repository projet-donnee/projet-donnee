import requests
import pandas as pd
from datetime import datetime


# API Weather API pour les prévisions météorologiques
weather_api_key = 'a42386cba08d40f9aab60416230208'
weather_api_url = f'http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q=Antananarivo&aqi=no'

# Appel API pour récupérer les données météorologiques
response = requests.get(weather_api_url)
weather_data = response.json()

# Extraction des données nécessaires
location_data = weather_data['location']
current_data = weather_data['current']

# Extraction des informations de localisation
location_name = location_data['name']
country = location_data['country']
localtime = location_data['localtime']

# Extraction des informations météorologiques actuelles
last_updated = current_data['last_updated']
temperature_c = current_data['temp_c']
temperature_f = current_data['temp_f']
is_day = current_data['is_day']
condition_text = current_data['condition']['text']
uv = current_data['uv']

# Création d'un DataFrame pandas pour stocker les données météorologiques
weather_df = pd.DataFrame({
    'Location': [location_name],
    'Country': [country],
    'LocalTime': [localtime],
    'LastUpdated': [last_updated],
    'TemperatureC': [temperature_c],
    'TemperatureF': [temperature_f],
    'IsDay': [is_day],
    'Condition': [condition_text],
    'UV': [uv]
})

# Transformation et stockage dans un fichier CSV local
weather_csv_filename = 'weather_data3.csv'
weather_df.to_csv(weather_csv_filename, index=False)

print("Données météorologiques extraites et transformées.")