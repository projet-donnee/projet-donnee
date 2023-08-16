from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

# ...

def compare_cities():
    # Charger le DataFrame
    weather_df = pd.read_csv('weather_data2.csv')

    # Analyse comparative des températures entre les villes
    city_temperature_comparison = weather_df.groupby('Location')['TemperatureC'].mean()
    
    # Afficher le graphique
    plt.bar(city_temperature_comparison.index, city_temperature_comparison.values)
    plt.xlabel('Villes')
    plt.ylabel('Température moyenne (C)')
    plt.title('Comparaison des températures entre les villes')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def temperature_variations():
    # Charger le DataFrame
    weather_df = pd.read_csv('weather_data2.csv')

    # Analyse des variations de température
    weather_df['LocalTime'] = pd.to_datetime(weather_df['LocalTime'])
    weather_df['Hour'] = weather_df['LocalTime'].dt.hour
    temperature_variation_by_hour = weather_df.groupby('Hour')['TemperatureC'].std()
    
    # Afficher le graphique
    plt.plot(temperature_variation_by_hour.index, temperature_variation_by_hour.values)
    plt.xlabel('Heure de la journée')
    plt.ylabel('Écart-type de la température (C)')
    plt.title('Variations de température par heure de la journée')
    plt.xticks(range(24))
    plt.tight_layout()
    plt.show()

def weather_trends():
    # Charger le DataFrame
    weather_df = pd.read_csv('weather_data2.csv')

    # Analyse des tendances météorologiques
    weather_conditions = weather_df['Condition'].value_counts()
    
    # Afficher le graphique
    plt.bar(weather_conditions.index, weather_conditions.values)
    plt.xlabel('Conditions météorologiques')
    plt.ylabel('Nombre d\'occurrences')
    plt.title('Tendances météorologiques générales')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


default_args = {
    'owner': 'mitsanta_andriah',
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mon_dag',
    default_args=default_args,
    schedule=timedelta(days=1),
)

compare_cities_task = PythonOperator(
    task_id='compare_cities',
    python_callable=compare_cities,
    dag=dag,
)

temperature_variations_task = PythonOperator(
    task_id='temperature_variations',
    python_callable=temperature_variations,
    dag=dag,
)

weather_trends_task = PythonOperator(
    task_id='weather_trends',
    python_callable=weather_trends,
    dag=dag,
)

compare_cities_task >> temperature_variations_task >> weather_trends_task
