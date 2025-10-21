#!/usr/bin/env python3
"""
Script para probar los parámetros corregidos de la API de measurements
"""

import requests
import json
from datetime import datetime, timedelta

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

def test_measurements_with_correct_params():
    """Prueba la API de measurements con los parámetros corregidos"""
    print("=== Probando API de measurements con parámetros corregidos ===")
    
    # Calcular fechas dinámicas (últimos 7 días)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    datetime_from = start_date.strftime('%Y-%m-%d')
    datetime_to = end_date.strftime('%Y-%m-%d')
    
    print(f"Fechas calculadas dinámicamente:")
    print(f"  datetime_from: {datetime_from}")
    print(f"  datetime_to: {datetime_to}")
    
    # Usar sensor de CO de Parque O'Higgins (conocido)
    sensor_id = 1045  # CO sensor
    
    api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
    params = {
        'datetime_from': datetime_from,
        'datetime_to': datetime_to,
        'limit': 10,
        'page': 1
    }
    headers = get_api_headers()
    
    print(f"\nProbando con sensor {sensor_id} (CO):")
    print(f"URL: {api_url}")
    print(f"Parámetros: {params}")
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        meta = data.get('meta', {})
        
        print(f"\n✅ Respuesta exitosa:")
        print(f"  Mediciones obtenidas: {len(results)}")
        print(f"  Metadatos: {json.dumps(meta, indent=2)}")
        
        if results:
            print(f"\n  Primeras 3 mediciones:")
            for i, measurement in enumerate(results[:3]):
                period = measurement.get('period', {})
                period_from = period.get('datetimeFrom', {})
                print(f"    {i+1}. Valor: {measurement.get('value', 'N/A')}")
                print(f"       Fecha UTC: {period_from.get('utc', 'N/A')}")
                print(f"       Parámetro: {measurement.get('parameter', {}).get('name', 'N/A')}")
        
        return len(results)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return 0

def test_multiple_sensors():
    """Prueba múltiples sensores de Santiago"""
    print(f"\n=== Probando múltiples sensores de Santiago ===")
    
    # Sensores conocidos de Parque O'Higgins
    sensors_to_test = [
        {'id': 1045, 'name': 'CO'},
        {'id': 1046, 'name': 'NO2'},
        {'id': 114, 'name': 'O3'}
    ]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    datetime_from = start_date.strftime('%Y-%m-%d')
    datetime_to = end_date.strftime('%Y-%m-%d')
    
    total_measurements = 0
    
    for sensor in sensors_to_test:
        sensor_id = sensor['id']
        sensor_name = sensor['name']
        
        print(f"\nProbando sensor {sensor_id} ({sensor_name}):")
        
        api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
        params = {
            'datetime_from': datetime_from,
            'datetime_to': datetime_to,
            'limit': 100,
            'page': 1
        }
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            meta = data.get('meta', {})
            
            print(f"  ✅ {len(results)} mediciones obtenidas")
            print(f"  Total disponible: {meta.get('found', 'N/A')}")
            
            total_measurements += len(results)
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
    
    print(f"\n=== RESUMEN ===")
    print(f"Total de mediciones obtenidas: {total_measurements}")
    print(f"Período: {datetime_from} al {datetime_to}")
    
    return total_measurements

if __name__ == "__main__":
    try:
        # Probar parámetros corregidos
        count1 = test_measurements_with_correct_params()
        
        # Probar múltiples sensores
        count2 = test_multiple_sensors()
        
        print(f"\n=== RESULTADO FINAL ===")
        if count1 > 0 or count2 > 0:
            print(f"✅ Los parámetros corregidos funcionan correctamente")
            print(f"✅ Las fechas dinámicas se calculan correctamente")
        else:
            print(f"❌ Problemas con los parámetros de la API")
            
    except Exception as e:
        print(f"Error general: {str(e)}")