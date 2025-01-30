from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Latitude and Longitude of Chicago
LATITUDE = '41.8781'
LONGITUDE = '-87.6298'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Create DAG (Directed Acyclic Graph)
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """
        Extracts weather data from the Open Meteo API using airflow connection
        """
        # Use the HttpHook to connect to the Open Meteo API
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        
        # Build the API and endpoint
        # https://api.open-meteo.com/v1/forecast?latitude=41.8781&longitude=-87.6298&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the request via HTTP hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Failed to fetch weather data: {response.status_code}')

    @task()
    def transform_weather_data(weather_data):
        """
        Transforms the weather data
        """
        # Extract the current weather data
        current_weather = weather_data.get('current_weather', {})

        # Log the full API response to understand what's available
        print("Full API Response:", weather_data)

        # Extract the fields with the correct names based on the API response
        temperature = current_weather.get('temperature', None)
        wind_speed = current_weather.get('windspeed', None)  # Use 'windspeed' as per the response
        weather_code = current_weather.get('weathercode', None)  # Use 'weathercode' as per the response
        observation_time = current_weather.get('time', None)  # Use 'time' as per the response

        # Handle missing data or log warnings for specific fields
        if wind_speed is None:
            print("Warning: 'windspeed' data is missing.")
        if weather_code is None:
            print("Warning: 'weathercode' data is missing.")
        if observation_time is None:
            print("Warning: 'time' data is missing.")
        
        transform_data = {
            'temperature': temperature if temperature is not None else 0,
            'windspeed': wind_speed,
            'weathercode': weather_code,
            'observation_time': observation_time if observation_time is not None else '1970-01-01T00:00:00',
            'latitude': LATITUDE,
            'longitude': LONGITUDE
        }

        print("Transformed Data:", transform_data)  # Log transformed data for further inspection
        
        return transform_data

    @task()
    def load_weather_data(transformed_data):
        """
        Loads the transformed weather data into the Postgres database
        """
        # Connect to the Postgres database
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data(
        temperature FLOAT,
        windspeed FLOAT,
        weathercode INT,
        observation_time TIMESTAMP,
        latitude FLOAT,
        longitude FLOAT
        )
        """)


        # Insert the transformed data into the table
        cursor.execute("""
    INSERT INTO weather_data(temperature, windspeed, weather_code, observation_time, latitude, longitude)
            VALUES(%s, %s, %s, %s, %s, %s)
            """, (
            transformed_data['temperature'], 
            transformed_data['windspeed'], 
            transformed_data['weathercode'],  # Ensure this is 'weather_code'
            transformed_data['observation_time'], 
            transformed_data['latitude'],
            transformed_data['longitude']
            ))

        conn.commit()
        cursor.close()

    # DAG workflow - ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)