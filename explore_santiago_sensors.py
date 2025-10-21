#!/usr/bin/env python3
"""
Script para identificar sensores en Santiago de Chile y probar el endpoint de measurements
"""

import psycopg2
import requests
import json

# Configuración de la API
API_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_API_KEY = '5a8e299d36d1671a4eabb37c3629318095b8221829d5f6feb5157973318a847d'

def get_api_headers():
    """Retorna los headers necesarios para la API de OpenAQ"""
    return {
        'X-API-Key': OPENAQ_API_KEY,
        'User-Agent': 'openaq-python-client/1.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

def get_postgres_connection():
    """Establece conexión con PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='openaq_data',
            user='openaq_user',
            password='openaq_password'
        )
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {str(e)}")
        raise

def find_santiago_locations():
    """Encuentra ubicaciones en Santiago de Chile desde la base de datos"""
    print("=== Buscando ubicaciones en Santiago de Chile ===")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Buscar ubicaciones en Santiago
    cursor.execute("""
        SELECT 
            location_id, 
            name, 
            locality,
            latitude, 
            longitude,
            sensors
        FROM openaq_locations 
        WHERE country_code = 'CL' 
        AND (
            LOWER(locality) LIKE '%santiago%' 
            OR LOWER(name) LIKE '%santiago%'
        )
        ORDER BY name;
    """)
    
    santiago_locations = cursor.fetchall()
    print(f"Encontradas {len(santiago_locations)} ubicaciones en Santiago:")
    
    all_sensors = []
    
    for location in santiago_locations:
        location_id, name, locality, lat, lon, sensors_json = location
        print(f"\n  Ubicación: {name}")
        print(f"    ID: {location_id}")
        print(f"    Localidad: {locality}")
        print(f"    Coordenadas: ({lat}, {lon})")
        
        # Analizar sensores si están disponibles
        if sensors_json:
            try:
                sensors = json.loads(sensors_json) if isinstance(sensors_json, str) else sensors_json
                if sensors and isinstance(sensors, list):
                    print(f"    Sensores ({len(sensors)}):")
                    for sensor in sensors:
                        sensor_id = sensor.get('id')
                        parameter = sensor.get('parameter', {})
                        if sensor_id:
                            print(f"      - Sensor ID: {sensor_id}")
                            print(f"        Parámetro: {parameter.get('name', 'N/A')} ({parameter.get('units', 'N/A')})")
                            all_sensors.append({
                                'sensor_id': sensor_id,
                                'location_id': location_id,
                                'location_name': name,
                                'parameter_name': parameter.get('name', 'N/A'),
                                'parameter_units': parameter.get('units', 'N/A')
                            })
                else:
                    print(f"    Sin información de sensores")
            except Exception as e:
                print(f"    Error procesando sensores: {str(e)}")
        else:
            print(f"    No hay datos de sensores")
    
    conn.close()
    
    print(f"\n=== RESUMEN ===")
    print(f"Total de ubicaciones en Santiago: {len(santiago_locations)}")
    print(f"Total de sensores encontrados: {len(all_sensors)}")
    
    return all_sensors

def test_measurements_endpoint(sensor_id, location_name, parameter_name):
    """Prueba el endpoint de measurements para un sensor específico"""
    print(f"\n=== Probando measurements para sensor {sensor_id} ===")
    print(f"Ubicación: {location_name}")
    print(f"Parámetro: {parameter_name}")
    
    api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
    headers = get_api_headers()
    
    # Parámetros para limitar la consulta (últimos 7 días, máximo 100 registros)
    params = {
        'limit': 100,
        'page': 1,
        'date_from': '2025-10-14',  # Últimos 7 días
        'date_to': '2025-10-21'
    }
    
    try:
        print(f"URL: {api_url}")
        print(f"Parámetros: {params}")
        
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        meta = data.get('meta', {})
        
        print(f"Respuesta exitosa:")
        print(f"  Mediciones obtenidas: {len(results)}")
        print(f"  Metadatos: {json.dumps(meta, indent=2)}")
        
        if results:
            print(f"  Ejemplo de medición:")
            measurement = results[0]
            print(f"    Fecha/Hora: {measurement.get('datetime', 'N/A')}")
            print(f"    Valor: {measurement.get('value', 'N/A')}")
            print(f"    Unidades: {measurement.get('parameter', {}).get('units', 'N/A')}")
            print(f"    Parámetro: {measurement.get('parameter', {}).get('name', 'N/A')}")
            
            # Mostrar estructura completa del primer resultado
            print(f"  Estructura completa de la primera medición:")
            print(json.dumps(measurement, indent=4))
        
        return len(results)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 0

def main():
    """Función principal"""
    try:
        # Paso 1: Encontrar sensores en Santiago
        sensors = find_santiago_locations()
        
        if not sensors:
            print("No se encontraron sensores en Santiago de Chile")
            return
        
        # Paso 2: Probar algunos sensores
        print(f"\n=== Probando endpoints de measurements ===")
        successful_tests = 0
        
        # Probar los primeros 3 sensores como ejemplo
        for i, sensor in enumerate(sensors[:3]):
            count = test_measurements_endpoint(
                sensor['sensor_id'],
                sensor['location_name'],
                sensor['parameter_name']
            )
            if count > 0:
                successful_tests += 1
        
        print(f"\n=== RESULTADO FINAL ===")
        print(f"Sensores encontrados en Santiago: {len(sensors)}")
        print(f"Pruebas exitosas de measurements: {successful_tests}/3")
        
        # Mostrar todos los sensores disponibles
        print(f"\nTodos los sensores disponibles:")
        for sensor in sensors:
            print(f"  Sensor ID: {sensor['sensor_id']} - {sensor['location_name']} ({sensor['parameter_name']})")
        
    except Exception as e:
        print(f"Error general: {str(e)}")

if __name__ == "__main__":
    main()