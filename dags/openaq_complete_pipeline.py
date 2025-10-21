from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json
import logging
import psycopg2
import psycopg2.extras
import os

# Configuración por defecto del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'openaq_complete_pipeline',
    default_args=default_args,
    description='Pipeline completo de OpenAQ: Países, Ubicaciones y Mediciones',
    schedule_interval=timedelta(hours=6),  # Ejecutar cada 6 horas
    catchup=False,
    tags=['openaq', 'air-quality', 'complete-pipeline', 'postgresql', 'etl'],
)

# Configuración de la API
OPENAQ_API_KEY = '5a8e299d36d1671a4eabb37c3629318095b8221829d5f6feb5157973318a847d'
API_BASE_URL = 'https://api.openaq.org/v3'

def get_api_headers():
    """Retorna los headers necesarios para la API de OpenAQ"""
    return {
        'X-API-Key': OPENAQ_API_KEY,
        'Content-Type': 'application/json'
    }

def get_postgres_connection():
    """
    Obtiene conexión a PostgreSQL usando las variables de entorno
    """
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            port=os.environ.get('POSTGRES_PORT', '5432'),
            database=os.environ.get('POSTGRES_DB', 'openaq_data'),
            user=os.environ.get('POSTGRES_USER', 'openaq_user'),
            password=os.environ.get('POSTGRES_PASSWORD', 'openaq_password')
        )
        conn.autocommit = False  # Usar transacciones manuales
        return conn
    except Exception as e:
        logging.error(f"Error conectando a PostgreSQL: {str(e)}")
        raise

def get_country_iso_codes(**context):
    """
    Obtiene los códigos ISO de los países desde la base de datos PostgreSQL
    Si la tabla está vacía, devuelve una lista predefinida de países prioritarios
    """
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Intentar obtener códigos de la base de datos
        cursor.execute("SELECT DISTINCT code FROM openaq_countries WHERE code IS NOT NULL ORDER BY code;")
        results = cursor.fetchall()
        
        if results:
            iso_codes = [row[0] for row in results]
            logging.info(f"Se obtuvieron {len(iso_codes)} códigos ISO desde la base de datos")
        else:
            # Lista predefinida de países prioritarios si la tabla está vacía
            iso_codes = [
                'CL',  # Chile (como ejemplo)
                'AR',  # Argentina
                'BR',  # Brasil
                'CO',  # Colombia
                'PE',  # Perú
                'US',  # Estados Unidos
                'DE',  # Alemania
                'CN',  # China
                'IN',  # India
                'GB'   # Reino Unido
            ]
            logging.info(f"Usando lista predefinida de {len(iso_codes)} países prioritarios")
        
        conn.close()
        return iso_codes
        
    except Exception as e:
        logging.error(f"Error obteniendo códigos ISO: {str(e)}")
        # En caso de error, usar lista básica
        return ['CL', 'AR', 'BR', 'CO', 'PE']

def create_all_tables(**context):
    """
    Crea todas las tablas necesarias para el pipeline de OpenAQ
    """
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Tabla de países
        countries_table_sql = """
        CREATE TABLE IF NOT EXISTS openaq_countries (
            id SERIAL PRIMARY KEY,
            country_id INTEGER UNIQUE,
            code VARCHAR(10),
            name VARCHAR(255),
            datetimeFirst TIMESTAMP,
            datetimeLast TIMESTAMP,
            parameters JSONB,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Tabla de ubicaciones
        locations_table_sql = """
        CREATE TABLE IF NOT EXISTS openaq_locations (
            id SERIAL PRIMARY KEY,
            location_id INTEGER UNIQUE,
            name VARCHAR(255),
            locality VARCHAR(255),
            timezone VARCHAR(100),
            country_code VARCHAR(10),
            country_name VARCHAR(255),
            owner_name VARCHAR(255),
            provider_name VARCHAR(255),
            is_mobile BOOLEAN,
            is_monitor BOOLEAN,
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            sensors JSONB,
            instruments JSONB,
            datetime_first TIMESTAMP,
            datetime_last TIMESTAMP,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Tabla de parámetros (mediciones disponibles)
        parameters_table_sql = """
        CREATE TABLE IF NOT EXISTS openaq_parameters (
            id SERIAL PRIMARY KEY,
            parameter_id INTEGER UNIQUE,
            name VARCHAR(100),
            display_name VARCHAR(255),
            units VARCHAR(50),
            description TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Tabla de mediciones de sensores
        measurements_table_sql = """
        CREATE TABLE IF NOT EXISTS openaq_measurements (
            id SERIAL PRIMARY KEY,
            sensor_id INTEGER NOT NULL,
            location_id INTEGER,
            value DECIMAL(15, 6),
            parameter_id INTEGER,
            parameter_name VARCHAR(100),
            parameter_units VARCHAR(50),
            datetime_utc TIMESTAMP,
            datetime_local TIMESTAMP,
            period_label VARCHAR(50),
            period_interval VARCHAR(50),
            period_from_utc TIMESTAMP,
            period_to_utc TIMESTAMP,
            period_from_local TIMESTAMP,
            period_to_local TIMESTAMP,
            has_flags BOOLEAN DEFAULT FALSE,
            coordinates_latitude DECIMAL(10, 8),
            coordinates_longitude DECIMAL(11, 8),
            coverage_expected_count INTEGER,
            coverage_observed_count INTEGER,
            coverage_percent_complete DECIMAL(5, 2),
            coverage_percent_coverage DECIMAL(5, 2),
            raw_data JSONB,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sensor_id, period_from_utc, period_to_utc)
        );
        """
        
        # Índices para optimización
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_countries_code ON openaq_countries(code);",
            "CREATE INDEX IF NOT EXISTS idx_countries_country_id ON openaq_countries(country_id);",
            "CREATE INDEX IF NOT EXISTS idx_locations_location_id ON openaq_locations(location_id);",
            "CREATE INDEX IF NOT EXISTS idx_locations_country_code ON openaq_locations(country_code);",
            "CREATE INDEX IF NOT EXISTS idx_locations_coordinates ON openaq_locations(latitude, longitude);",
            "CREATE INDEX IF NOT EXISTS idx_parameters_name ON openaq_parameters(name);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_sensor_id ON openaq_measurements(sensor_id);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_location_id ON openaq_measurements(location_id);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_parameter_name ON openaq_measurements(parameter_name);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_datetime_utc ON openaq_measurements(datetime_utc);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_period_from ON openaq_measurements(period_from_utc);",
            "CREATE INDEX IF NOT EXISTS idx_measurements_extracted_at ON openaq_measurements(extracted_at);"
        ]
        
        logging.info("Creando tablas de OpenAQ...")
        cursor.execute(countries_table_sql)
        cursor.execute(locations_table_sql)
        cursor.execute(parameters_table_sql)
        cursor.execute(measurements_table_sql)
        
        # Crear índices
        for index_sql in indexes_sql:
            cursor.execute(index_sql)
        
        conn.commit()
        
        logging.info("Todas las tablas de OpenAQ e índices creados exitosamente")
        conn.close()
        
    except Exception as e:
        logging.error(f"Error al crear tablas: {str(e)}")
        raise

def extract_countries(**context):
    """Extrae datos de países de la API de OpenAQ con paginación"""
    try:
        # Extraer países por páginas
        all_countries = []
        page = 1
        limit = 100
        
        while True:
            api_url = f"{API_BASE_URL}/countries?page={page}&limit={limit}"
            headers = get_api_headers()
            
            logging.info(f"Extrayendo países - Página {page}: {api_url}")
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            countries = data.get('results', [])
            meta = data.get('meta', {})
            
            logging.info(f"Página {page}: {len(countries)} países obtenidos")
            logging.info(f"Metadatos: Found={meta.get('found', 'N/A')}, Limit={meta.get('limit', 'N/A')}, Page={meta.get('page', 'N/A')}")
            
            if not countries:
                logging.info("No hay más países para extraer")
                break
            
            all_countries.extend(countries)
            
            # Verificar si es la última página
            found = meta.get('found', 0)
            if found > 0 and len(all_countries) >= found:
                logging.info(f"Se alcanzó el total de países disponibles: {found}")
                break
            
            # Si no hay metadatos de paginación y obtuvimos menos del límite, es la última página
            if len(countries) < limit:
                logging.info(f"Última página detectada: se obtuvieron {len(countries)} países (menos que el límite {limit})")
                break
            
            page += 1
            
            # Límite de seguridad para evitar bucle infinito
            if page > 10:
                logging.warning("Límite de seguridad alcanzado (10 páginas)")
                break
        
        logging.info(f"Se extrajeron {len(all_countries)} países en total")
        
        # Guardar datos
        data_file = '/opt/airflow/data/openaq_countries.json'
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        
        with open(data_file, 'w') as f:
            json.dump(all_countries, f, indent=2)
        
        return len(all_countries)
        
    except Exception as e:
        logging.error(f"Error al extraer países: {str(e)}")
        raise

def extract_locations(**context):
    """Extrae ubicaciones de medición de la API de OpenAQ filtrando por país (ISO)"""
    try:
        # Obtener códigos ISO de países
        iso_codes = get_country_iso_codes()
        all_locations = []
        
        # Extraer ubicaciones por país
        for iso_code in iso_codes:
            logging.info(f"Extrayendo ubicaciones para el país: {iso_code}")
            
            # Extraer ubicaciones por páginas para cada país
            page = 1
            limit = 100
            country_locations = []
            
            while True:
                api_url = f"{API_BASE_URL}/locations"
                params = {
                    'iso': iso_code,
                    'page': page, 
                    'limit': limit
                }
                headers = get_api_headers()
                
                logging.info(f"  País {iso_code} - Página {page}")
                response = requests.get(api_url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                locations = data.get('results', [])
                meta = data.get('meta', {})
                
                logging.info(f"  Página {page}: {len(locations)} ubicaciones obtenidas")
                if meta.get('found'):
                    logging.info(f"  Total disponible para {iso_code}: {meta.get('found')}")
                
                if not locations:
                    break
                    
                country_locations.extend(locations)
                
                # Verificar si hay más páginas usando metadatos
                found = meta.get('found', 0)
                if found and str(found).isdigit() and int(found) > 0 and len(country_locations) >= int(found):
                    logging.info(f"  Se obtuvieron todas las ubicaciones para {iso_code}: {len(country_locations)}")
                    break
                
                # Si no hay metadatos y obtuvimos menos del límite, es la última página
                if len(locations) < limit:
                    logging.info(f"  Última página para {iso_code}: {len(locations)} ubicaciones")
                    break
                
                page += 1
                
                # Límite de seguridad por país
                if page > 20:
                    logging.warning(f"  Límite de seguridad alcanzado para {iso_code} (20 páginas)")
                    break
            
            logging.info(f"País {iso_code}: {len(country_locations)} ubicaciones extraídas")
            all_locations.extend(country_locations)
            
            # Límite global para evitar timeout - opcional, se puede ajustar
            if len(all_locations) >= 2000:
                logging.info(f"Límite global alcanzado: {len(all_locations)} ubicaciones")
                break
        
        logging.info(f"Se extrajeron {len(all_locations)} ubicaciones en total de {len(iso_codes)} países")
        
        # Guardar datos
        data_file = '/opt/airflow/data/openaq_locations.json'
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        
        with open(data_file, 'w') as f:
            json.dump(all_locations, f, indent=2)
        
        return len(all_locations)
        
    except Exception as e:
        logging.error(f"Error al extraer ubicaciones: {str(e)}")
        raise

def extract_parameters(**context):
    """Extrae parámetros disponibles de la API de OpenAQ"""
    try:
        api_url = f"{API_BASE_URL}/parameters"
        headers = get_api_headers()
        
        logging.info(f"Extrayendo parámetros de: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        parameters = data.get('results', [])
        
        logging.info(f"Se extrajeron {len(parameters)} parámetros")
        
        # Guardar datos
        data_file = '/opt/airflow/data/openaq_parameters.json'
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        
        with open(data_file, 'w') as f:
            json.dump(parameters, f, indent=2)
        
        return len(parameters)
        
    except Exception as e:
        logging.error(f"Error al extraer parámetros: {str(e)}")
        raise

def load_countries(**context):
    """Carga datos de países en PostgreSQL"""
    try:
        data_file = '/opt/airflow/data/openaq_countries.json'
        
        with open(data_file, 'r') as f:
            countries = json.load(f)
        
        if not countries:
            logging.warning("No hay datos de países para cargar")
            return
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla
        cursor.execute("DELETE FROM openaq_countries;")
        
        # Usar UPSERT para evitar duplicados
        insert_sql = """
        INSERT INTO openaq_countries (country_id, code, name, datetimeFirst, datetimeLast, parameters)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_id) 
        DO UPDATE SET 
            code = EXCLUDED.code,
            name = EXCLUDED.name,
            datetimeFirst = EXCLUDED.datetimeFirst,
            datetimeLast = EXCLUDED.datetimeLast,
            parameters = EXCLUDED.parameters,
            extracted_at = CURRENT_TIMESTAMP;
        """
        
        for country in countries:
            parameters_json = json.dumps(country.get('parameters', []))
            
            cursor.execute(insert_sql, (
                country.get('id'),
                country.get('code'),
                country.get('name'),
                country.get('datetimeFirst'),
                country.get('datetimeLast'),
                parameters_json
            ))
        
        conn.commit()
        logging.info(f"Se cargaron {len(countries)} países exitosamente")
        
        # Verificar carga
        cursor.execute("SELECT COUNT(*) FROM openaq_countries;")
        count = cursor.fetchone()[0]
        logging.info(f"Total de países en la tabla: {count}")
        
        conn.close()
        os.remove(data_file)  # Limpiar archivo temporal
        
    except Exception as e:
        logging.error(f"Error al cargar países: {str(e)}")
        raise

def load_locations(**context):
    """Carga datos de ubicaciones en PostgreSQL"""
    try:
        data_file = '/opt/airflow/data/openaq_locations.json'
        
        with open(data_file, 'r') as f:
            locations = json.load(f)
        
        if not locations:
            logging.warning("No hay datos de ubicaciones para cargar")
            return
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla
        cursor.execute("DELETE FROM openaq_locations;")
        
        # Usar UPSERT para evitar duplicados
        insert_sql = """
        INSERT INTO openaq_locations (
            location_id, name, locality, timezone, country_code, country_name,
            owner_name, provider_name, is_mobile, is_monitor, latitude, longitude,
            sensors, instruments, datetime_first, datetime_last
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            locality = EXCLUDED.locality,
            timezone = EXCLUDED.timezone,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            owner_name = EXCLUDED.owner_name,
            provider_name = EXCLUDED.provider_name,
            is_mobile = EXCLUDED.is_mobile,
            is_monitor = EXCLUDED.is_monitor,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            sensors = EXCLUDED.sensors,
            instruments = EXCLUDED.instruments,
            datetime_first = EXCLUDED.datetime_first,
            datetime_last = EXCLUDED.datetime_last,
            extracted_at = CURRENT_TIMESTAMP;
        """
        
        for location in locations:
            country = location.get('country', {})
            owner = location.get('owner', {})
            provider = location.get('provider', {})
            coordinates = location.get('coordinates', {})
            
            sensors_json = json.dumps(location.get('sensors', []))
            instruments_json = json.dumps(location.get('instruments', []))
            
            # Manejar fechas que pueden venir como diccionarios o strings
            datetime_first = location.get('datetimeFirst')
            datetime_last = location.get('datetimeLast')
            
            # Si las fechas son diccionarios, extraer el valor 'utc' o convertir a string
            if isinstance(datetime_first, dict):
                datetime_first = datetime_first.get('utc') or str(datetime_first)
            if isinstance(datetime_last, dict):
                datetime_last = datetime_last.get('utc') or str(datetime_last)
            
            cursor.execute(insert_sql, (
                location.get('id'),
                location.get('name'),
                location.get('locality'),
                location.get('timezone'),
                country.get('code'),
                country.get('name'),
                owner.get('name'),
                provider.get('name'),
                location.get('isMobile', False),
                location.get('isMonitor', False),
                coordinates.get('latitude'),
                coordinates.get('longitude'),
                sensors_json,
                instruments_json,
                datetime_first,
                datetime_last
            ))
        
        conn.commit()
        logging.info(f"Se cargaron {len(locations)} ubicaciones exitosamente")
        
        # Verificar carga
        cursor.execute("SELECT COUNT(*) FROM openaq_locations;")
        count = cursor.fetchone()[0]
        logging.info(f"Total de ubicaciones en la tabla: {count}")
        
        conn.close()
        os.remove(data_file)  # Limpiar archivo temporal
        
    except Exception as e:
        logging.error(f"Error al cargar ubicaciones: {str(e)}")
        raise

def load_parameters(**context):
    """Carga datos de parámetros en PostgreSQL"""
    try:
        data_file = '/opt/airflow/data/openaq_parameters.json'
        
        with open(data_file, 'r') as f:
            parameters = json.load(f)
        
        if not parameters:
            logging.warning("No hay datos de parámetros para cargar")
            return
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Limpiar tabla
        cursor.execute("DELETE FROM openaq_parameters;")
        
        # Usar UPSERT para evitar duplicados
        insert_sql = """
        INSERT INTO openaq_parameters (parameter_id, name, display_name, units, description)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (parameter_id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            units = EXCLUDED.units,
            description = EXCLUDED.description,
            extracted_at = CURRENT_TIMESTAMP;
        """
        
        for param in parameters:
            cursor.execute(insert_sql, (
                param.get('id'),
                param.get('name'),
                param.get('displayName'),
                param.get('units'),
                param.get('description')
            ))
        
        conn.commit()
        logging.info(f"Se cargaron {len(parameters)} parámetros exitosamente")
        
        # Verificar carga
        cursor.execute("SELECT COUNT(*) FROM openaq_parameters;")
        count = cursor.fetchone()[0]
        logging.info(f"Total de parámetros en la tabla: {count}")
        
        conn.close()
        os.remove(data_file)  # Limpiar archivo temporal
        
    except Exception as e:
        logging.error(f"Error al cargar parámetros: {str(e)}")
        raise

def validate_complete_pipeline(**context):
    """Valida que todo el pipeline se ejecutó correctamente"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Estadísticas de países
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT code) FROM openaq_countries;")
        countries_stats = cursor.fetchone()
        
        # Estadísticas de ubicaciones
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT country_code) FROM openaq_locations;")
        locations_stats = cursor.fetchone()
        
        # Estadísticas de parámetros
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT name) FROM openaq_parameters;")
        parameters_stats = cursor.fetchone()
        
        # Top países por ubicaciones
        cursor.execute("""
        SELECT country_name, COUNT(*) as locations_count 
        FROM openaq_locations 
        WHERE country_name IS NOT NULL 
        GROUP BY country_name 
        ORDER BY locations_count DESC 
        LIMIT 5;
        """)
        top_countries = cursor.fetchall()
        
        logging.info("=== VALIDACIÓN PIPELINE COMPLETO ===")
        logging.info(f"Países: {countries_stats[0]} total, {countries_stats[1]} únicos")
        logging.info(f"Ubicaciones: {locations_stats[0]} total, {locations_stats[1]} países")
        logging.info(f"Parámetros: {parameters_stats[0]} total, {parameters_stats[1]} únicos")
        
        logging.info("Top 5 países por número de ubicaciones:")
        for country, count in top_countries:
            logging.info(f"  - {country}: {count} ubicaciones")
        
        conn.close()
        
        if countries_stats[0] == 0 or locations_stats[0] == 0 or parameters_stats[0] == 0:
            raise ValueError("Una o más tablas están vacías")
        
        return {
            'countries_count': countries_stats[0],
            'locations_count': locations_stats[0],
            'parameters_count': parameters_stats[0],
            'validation_status': 'SUCCESS'
        }
        
    except Exception as e:
        logging.error(f"Error en validación: {str(e)}")
        raise

def get_santiago_sensors(**context):
    """Obtiene los IDs de sensores para Santiago de Chile desde la base de datos"""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Buscar ubicaciones en Santiago
        cursor.execute("""
            SELECT 
                location_id, 
                name, 
                locality,
                sensors
            FROM openaq_locations 
            WHERE country_code = 'CL' 
            AND (
                LOWER(locality) LIKE '%stemuco%' 
                --OR LOWER(name) LIKE '%santiago%'
            );
        """)
        
        santiago_locations = cursor.fetchall()
        all_sensors = []
        
        for location in santiago_locations:
            location_id, name, locality, sensors_json = location
            logging.info(f"Procesando ubicación: {name} (ID: {location_id})")
            
            if sensors_json:
                try:
                    sensors = json.loads(sensors_json) if isinstance(sensors_json, str) else sensors_json
                    if sensors and isinstance(sensors, list):
                        for sensor in sensors:
                            sensor_id = sensor.get('id')
                            parameter = sensor.get('parameter', {})
                            if sensor_id:
                                all_sensors.append({
                                    'sensor_id': sensor_id,
                                    'location_id': location_id,
                                    'location_name': name,
                                    'parameter_name': parameter.get('name', 'N/A'),
                                    'parameter_units': parameter.get('units', 'N/A')
                                })
                except Exception as e:
                    logging.error(f"Error procesando sensores para {name}: {str(e)}")
        
        conn.close()
        
        logging.info(f"Se encontraron {len(all_sensors)} sensores en Santiago")
        
        # Guardar información de sensores
        sensors_file = '/opt/airflow/data/santiago_sensors.json'
        os.makedirs(os.path.dirname(sensors_file), exist_ok=True)
        
        with open(sensors_file, 'w') as f:
            json.dump(all_sensors, f, indent=2)
        
        return len(all_sensors)
        
    except Exception as e:
        logging.error(f"Error obteniendo sensores de Santiago: {str(e)}")
        raise

def extract_measurements(**context):
    """Extrae mediciones de sensores de Santiago de Chile"""
    try:
        # Cargar información de sensores
        sensors_file = '/opt/airflow/data/santiago_sensors.json'
        
        with open(sensors_file, 'r') as f:
            sensors = json.load(f)
        
        if not sensors:
            logging.warning("No hay sensores para procesar")
            return 0
        
        all_measurements = []
        
        # Parámetros de tiempo - usar período con datos disponibles
        # Según la información del sensor, los datos van hasta agosto 2021
        from datetime import datetime, timedelta
        
        # Usar fechas del período donde hay datos disponibles (última semana de agosto 2021)
        datetime_to = '2021-08-20'
        datetime_from = '2021-08-13'  # 7 días antes
        
        logging.info(f"Extrayendo mediciones del {datetime_from} al {datetime_to} (período con datos disponibles)")
        
        for sensor in sensors:
            sensor_id = sensor['sensor_id']
            location_id = sensor['location_id']
            location_name = sensor['location_name']
            parameter_name = sensor['parameter_name']
            
            logging.info(f"Extrayendo mediciones para sensor {sensor_id} ({parameter_name}) en {location_name}")
            
            # Extraer mediciones por páginas
            page = 1
            limit = 1000  # Límite por página
            sensor_measurements = []
            
            while True:
                api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
                params = {
                    'limit': limit,
                    'page': page,
                    'datetime_from': datetime_from,
                    'datetime_to': datetime_to
                }
                headers = get_api_headers()
                
                logging.info(f"  Página {page} para sensor {sensor_id}")
                logging.info(f"  URL: {api_url}?datetime_from={datetime_from}&datetime_to={datetime_to}&page={page}&limit={limit}")
                
                try:
                    response = requests.get(api_url, headers=headers, params=params, timeout=60)
                    response.raise_for_status()
                    
                    data = response.json()
                    measurements = data.get('results', [])
                    meta = data.get('meta', {})
                    
                    logging.info(f"  Página {page}: {len(measurements)} mediciones obtenidas")
                    
                    if not measurements:
                        break
                    
                    # Agregar información adicional a cada medición
                    for measurement in measurements:
                        measurement['sensor_id'] = sensor_id
                        measurement['location_id'] = location_id
                        measurement['location_name'] = location_name
                    
                    sensor_measurements.extend(measurements)
                    
                    # Verificar si hay más páginas
                    found = meta.get('found')
                    if found and str(found).isdigit() and int(found) > 0 and len(sensor_measurements) >= int(found):
                        logging.info(f"  Se obtuvieron todas las mediciones para sensor {sensor_id}: {len(sensor_measurements)}")
                        break
                    
                    if len(measurements) < limit:
                        logging.info(f"  Última página para sensor {sensor_id}")
                        break
                    
                    page += 1
                    
                    # Límite de seguridad
                    if page > 100:
                        logging.warning(f"  Límite de seguridad alcanzado para sensor {sensor_id}")
                        break
                        
                except Exception as e:
                    logging.error(f"  Error en página {page} para sensor {sensor_id}: {str(e)}")
                    break
            
            logging.info(f"Sensor {sensor_id}: {len(sensor_measurements)} mediciones extraídas")
            all_measurements.extend(sensor_measurements)
            
            # Límite global para evitar sobrecarga
            if len(all_measurements) >= 50000:
                logging.info(f"Límite global alcanzado: {len(all_measurements)} mediciones")
                break
        
        logging.info(f"Se extrajeron {len(all_measurements)} mediciones en total")
        
        # Guardar datos
        data_file = '/opt/airflow/data/openaq_measurements.json'
        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        
        with open(data_file, 'w') as f:
            json.dump(all_measurements, f, indent=2)
        
        return len(all_measurements)
        
    except Exception as e:
        logging.error(f"Error al extraer mediciones: {str(e)}")
        raise

def load_measurements(**context):
    """Carga mediciones de sensores en PostgreSQL"""
    try:
        data_file = '/opt/airflow/data/openaq_measurements.json'
        
        with open(data_file, 'r') as f:
            measurements = json.load(f)
        
        if not measurements:
            logging.warning("No hay mediciones para cargar")
            return 0
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Usar UPSERT para evitar duplicados
        insert_sql = """
        INSERT INTO openaq_measurements (
            sensor_id, location_id, value, parameter_id, parameter_name, parameter_units,
            datetime_utc, datetime_local, period_label, period_interval,
            period_from_utc, period_to_utc, period_from_local, period_to_local,
            has_flags, coordinates_latitude, coordinates_longitude,
            coverage_expected_count, coverage_observed_count, 
            coverage_percent_complete, coverage_percent_coverage,
            raw_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (sensor_id, period_from_utc, period_to_utc)
        DO UPDATE SET 
            value = EXCLUDED.value,
            parameter_id = EXCLUDED.parameter_id,
            parameter_name = EXCLUDED.parameter_name,
            parameter_units = EXCLUDED.parameter_units,
            datetime_utc = EXCLUDED.datetime_utc,
            datetime_local = EXCLUDED.datetime_local,
            has_flags = EXCLUDED.has_flags,
            coverage_expected_count = EXCLUDED.coverage_expected_count,
            coverage_observed_count = EXCLUDED.coverage_observed_count,
            coverage_percent_complete = EXCLUDED.coverage_percent_complete,
            coverage_percent_coverage = EXCLUDED.coverage_percent_coverage,
            raw_data = EXCLUDED.raw_data,
            extracted_at = CURRENT_TIMESTAMP;
        """
        
        successful_inserts = 0
        
        for measurement in measurements:
            try:
                # Extraer datos del período
                period = measurement.get('period', {})
                period_from = period.get('datetimeFrom', {})
                period_to = period.get('datetimeTo', {})
                
                # Extraer datos del parámetro
                parameter = measurement.get('parameter', {})
                
                # Extraer datos de cobertura
                coverage = measurement.get('coverage', {})
                
                # Extraer coordenadas si están disponibles
                coordinates = measurement.get('coordinates')
                coords_lat = coordinates.get('latitude') if coordinates else None
                coords_lon = coordinates.get('longitude') if coordinates else None
                
                # Extraer información de flags
                flag_info = measurement.get('flagInfo', {})
                
                cursor.execute(insert_sql, (
                    measurement.get('sensor_id'),
                    measurement.get('location_id'),
                    measurement.get('value'),
                    parameter.get('id'),
                    parameter.get('name'),
                    parameter.get('units'),
                    None,  # datetime_utc - no disponible directamente
                    None,  # datetime_local - no disponible directamente
                    period.get('label'),
                    period.get('interval'),
                    period_from.get('utc'),
                    period_to.get('utc'),
                    period_from.get('local'),
                    period_to.get('local'),
                    flag_info.get('hasFlags', False),
                    coords_lat,
                    coords_lon,
                    coverage.get('expectedCount'),
                    coverage.get('observedCount'),
                    coverage.get('percentComplete'),
                    coverage.get('percentCoverage'),
                    json.dumps(measurement)
                ))
                
                successful_inserts += 1
                
            except Exception as e:
                logging.error(f"Error insertando medición: {str(e)}")
                continue
        
        conn.commit()
        logging.info(f"Se cargaron {successful_inserts} mediciones exitosamente")
        
        # Verificar carga
        cursor.execute("SELECT COUNT(*) FROM openaq_measurements;")
        count = cursor.fetchone()[0]
        logging.info(f"Total de mediciones en la tabla: {count}")
        
        # Estadísticas por sensor
        cursor.execute("""
            SELECT sensor_id, parameter_name, COUNT(*) as measurement_count
            FROM openaq_measurements
            GROUP BY sensor_id, parameter_name
            ORDER BY measurement_count DESC;
        """)
        
        sensor_stats = cursor.fetchall()
        logging.info("Estadísticas por sensor:")
        for sensor_id, param_name, count in sensor_stats:
            logging.info(f"  Sensor {sensor_id} ({param_name}): {count} mediciones")
        
        conn.close()
        os.remove(data_file)  # Limpiar archivo temporal
        
        return successful_inserts
        
    except Exception as e:
        logging.error(f"Error al cargar mediciones: {str(e)}")
        raise

# Definir tareas
create_tables_task = PythonOperator(
    task_id='create_all_tables',
    python_callable=create_all_tables,
    dag=dag,
)

# Tareas de extracción (pueden ejecutarse en paralelo)
extract_countries_task = PythonOperator(
    task_id='extract_countries',
    python_callable=extract_countries,
    dag=dag,
)

extract_locations_task = PythonOperator(
    task_id='extract_locations',
    python_callable=extract_locations,
    dag=dag,
)

extract_parameters_task = PythonOperator(
    task_id='extract_parameters',
    python_callable=extract_parameters,
    dag=dag,
)

# Tareas de carga (deben ejecutarse después de la extracción)
load_countries_task = PythonOperator(
    task_id='load_countries',
    python_callable=load_countries,
    dag=dag,
)

load_locations_task = PythonOperator(
    task_id='load_locations',
    python_callable=load_locations,
    dag=dag,
)

load_parameters_task = PythonOperator(
    task_id='load_parameters',
    python_callable=load_parameters,
    dag=dag,
)

# Tareas de mediciones (Santiago de Chile)
get_sensors_task = PythonOperator(
    task_id='get_santiago_sensors',
    python_callable=get_santiago_sensors,
    dag=dag,
)

extract_measurements_task = PythonOperator(
    task_id='extract_measurements',
    python_callable=extract_measurements,
    dag=dag,
)

load_measurements_task = PythonOperator(
    task_id='load_measurements',
    python_callable=load_measurements,
    dag=dag,
)

# Tarea de validación final
validate_task = PythonOperator(
    task_id='validate_complete_pipeline',
    python_callable=validate_complete_pipeline,
    dag=dag,
)

# Definir dependencias del pipeline
create_tables_task >> [extract_countries_task, extract_locations_task, extract_parameters_task]

extract_countries_task >> load_countries_task
extract_locations_task >> load_locations_task
extract_parameters_task >> load_parameters_task

# Las mediciones dependen de que las locations estén cargadas (para obtener sensores)
load_locations_task >> get_sensors_task >> extract_measurements_task >> load_measurements_task

[load_countries_task, load_locations_task, load_parameters_task, load_measurements_task] >> validate_task