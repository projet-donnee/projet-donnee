import requests 
import pandas as pd
import boto3
from datetime import datetime



# API Weather API pour les prévisions météorologiques
weather_api_key = 'a42386cba08d40f9aab60416230208'
cities = ['Antananarivo', 'Majunga', 'Toliara', 'Foulpointe','Antsirabe', 'Fianarantsoa']

# Création du DataFrame pour les données météorologiques
weather_data_list = []

for city in cities:
    weather_api_url = f'http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q={city}&aqi=no'
    response = requests.get(weather_api_url)
    weather_data = response.json()

    location_data = weather_data['location']
    current_data = weather_data['current']

    location_name = location_data['name']
    country = location_data['country']
    localtime = location_data['localtime']
    last_updated = current_data['last_updated']
    temperature_c = current_data['temp_c']
    temperature_f = current_data['temp_f']
    is_day = current_data['is_day']
    condition_text = current_data['condition']['text']
    uv = current_data['uv']

    weather_data_list.append({
        'Location': location_name,
        'Country': country,
        'LocalTime': localtime,
        'LastUpdated': last_updated,
        'TemperatureC': temperature_c,
        'TemperatureF': temperature_f,
        'IsDay': is_day,
        'Condition': condition_text,
        'UV': uv
    })

weather_df = pd.DataFrame(weather_data_list)

# Sauvegarder le DataFrame au format CSV localement
weather_csv_filename = 'weather_data.csv'
weather_df.to_csv(weather_csv_filename, index=False)

print("Données météorologiques extraites et transformées.")

