from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import logging

# Configuración por defecto del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'api_to_postgres_pipeline',
    default_args=default_args,
    description='Extraer datos de API pública e insertar en PostgreSQL',
    schedule_interval=timedelta(hours=1),  # Ejecutar cada hora
    catchup=False,
    tags=['api', 'postgres', 'etl'],
)

def extract_data_from_api(**context):
    """
    Extrae datos de una API pública (JSONPlaceholder como ejemplo)
    """
    try:
        # API pública de ejemplo - puedes cambiar por cualquier otra API
        api_url = "https://jsonplaceholder.typicode.com/posts"
        
        logging.info(f"Haciendo petición a: {api_url}")
        
        # Realizar petición GET
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Lanza excepción si hay error HTTP
        
        # Convertir a JSON
        data = response.json()
        
        logging.info(f"Se extrajeron {len(data)} registros de la API")
        
        # Guardar datos en XCom para la siguiente tarea
        return data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con la API: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {str(e)}")
        raise

def create_table_if_not_exists(**context):
    """
    Crea la tabla en PostgreSQL si no existe
    """
    try:
        # Conectar a PostgreSQL usando el hook de Airflow
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # SQL para crear la tabla
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS api_posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            body TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        logging.info("Creando tabla api_posts si no existe")
        postgres_hook.run(create_table_sql)
        logging.info("Tabla creada o ya existía")
        
    except Exception as e:
        logging.error(f"Error al crear la tabla: {str(e)}")
        raise

def insert_data_to_postgres(**context):
    """
    Inserta los datos extraídos de la API en PostgreSQL
    """
    try:
        # Obtener datos de la tarea anterior usando XCom
        task_instance = context['task_instance']
        data = task_instance.xcom_pull(task_ids='extract_api_data')
        
        if not data:
            logging.warning("No se recibieron datos de la tarea anterior")
            return
        
        # Conectar a PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Preparar datos para inserción
        logging.info(f"Preparando {len(data)} registros para inserción")
        
        # Limpiar tabla antes de insertar (opcional)
        postgres_hook.run("TRUNCATE TABLE api_posts;")
        
        # Insertar datos
        insert_sql = """
        INSERT INTO api_posts (id, user_id, title, body)
        VALUES (%(id)s, %(userId)s, %(title)s, %(body)s);
        """
        
        # Insertar cada registro
        for record in data:
            postgres_hook.run(insert_sql, parameters={
                'id': record['id'],
                'userId': record['userId'],
                'title': record['title'],
                'body': record['body']
            })
        
        logging.info(f"Se insertaron {len(data)} registros exitosamente")
        
        # Verificar inserción
        count_result = postgres_hook.get_first("SELECT COUNT(*) FROM api_posts;")
        logging.info(f"Total de registros en la tabla: {count_result[0]}")
        
    except Exception as e:
        logging.error(f"Error al insertar datos en PostgreSQL: {str(e)}")
        raise

def validate_data(**context):
    """
    Valida que los datos se insertaron correctamente
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Obtener estadísticas de la tabla
        stats_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT user_id) as unique_users,
            MIN(extracted_at) as first_extraction,
            MAX(extracted_at) as last_extraction
        FROM api_posts;
        """
        
        result = postgres_hook.get_first(stats_sql)
        
        logging.info(f"Validación completada:")
        logging.info(f"- Total de registros: {result[0]}")
        logging.info(f"- Usuarios únicos: {result[1]}")
        logging.info(f"- Primera extracción: {result[2]}")
        logging.info(f"- Última extracción: {result[3]}")
        
        # Validar que hay datos
        if result[0] == 0:
            raise ValueError("No se encontraron datos en la tabla")
        
        return {
            'total_records': result[0],
            'unique_users': result[1],
            'validation_status': 'SUCCESS'
        }
        
    except Exception as e:
        logging.error(f"Error en la validación: {str(e)}")
        raise

# Definir tareas
extract_task = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_data_from_api,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_data_postgres',
    python_callable=insert_data_to_postgres,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Definir dependencias
create_table_task >> extract_task >> insert_task >> validate_task