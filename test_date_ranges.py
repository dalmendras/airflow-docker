#!/usr/bin/env python3
"""
Script para probar diferentes rangos de fechas y encontrar datos disponibles
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

def test_different_date_ranges():
    """Prueba diferentes rangos de fechas para encontrar datos disponibles"""
    print("=== Probando diferentes rangos de fechas ===")
    
    sensor_id = 1045  # CO sensor
    
    # Diferentes rangos de fechas para probar
    date_ranges = [
        {'days_back': 7, 'label': 'Últimos 7 días'},
        {'days_back': 30, 'label': 'Últimos 30 días'},
        {'days_back': 90, 'label': 'Últimos 3 meses'},
        {'days_back': 365, 'label': 'Último año'}
    ]
    
    for date_range in date_ranges:
        print(f"\n--- {date_range['label']} ---")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range['days_back'])
        
        datetime_from = start_date.strftime('%Y-%m-%d')
        datetime_to = end_date.strftime('%Y-%m-%d')
        
        print(f"Período: {datetime_from} al {datetime_to}")
        
        api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
        params = {
            'datetime_from': datetime_from,
            'datetime_to': datetime_to,
            'limit': 5,
            'page': 1
        }
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            meta = data.get('meta', {})
            
            print(f"  Mediciones encontradas: {len(results)}")
            print(f"  Total disponible: {meta.get('found', 'N/A')}")
            
            if results:
                print(f"  ✅ ¡Datos encontrados!")
                measurement = results[0]
                period = measurement.get('period', {})
                period_from = period.get('datetimeFrom', {})
                print(f"  Última medición: {period_from.get('utc', 'N/A')}")
                print(f"  Valor: {measurement.get('value', 'N/A')}")
                return datetime_from, datetime_to, len(results)
            else:
                print(f"  ❌ No hay datos en este período")
                
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
    
    return None, None, 0

def test_recent_measurements():
    """Prueba con fechas específicas recientes"""
    print(f"\n=== Probando fechas específicas recientes ===")
    
    sensor_id = 1045
    
    # Probar diferentes fechas específicas
    specific_dates = [
        {'from': '2025-10-01', 'to': '2025-10-21'},
        {'from': '2025-09-01', 'to': '2025-09-30'},
        {'from': '2025-08-01', 'to': '2025-08-31'},
        {'from': '2025-01-01', 'to': '2025-01-31'},
        {'from': '2024-12-01', 'to': '2024-12-31'}
    ]
    
    for date_range in specific_dates:
        datetime_from = date_range['from']
        datetime_to = date_range['to']
        
        print(f"\nProbando período: {datetime_from} al {datetime_to}")
        
        api_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"
        params = {
            'datetime_from': datetime_from,
            'datetime_to': datetime_to,
            'limit': 5,
            'page': 1
        }
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            meta = data.get('meta', {})
            
            print(f"  Mediciones: {len(results)}, Total: {meta.get('found', 'N/A')}")
            
            if results:
                print(f"  ✅ ¡Datos encontrados!")
                measurement = results[0]
                period = measurement.get('period', {})
                period_from = period.get('datetimeFrom', {})
                print(f"  Fecha medición: {period_from.get('utc', 'N/A')}")
                return datetime_from, datetime_to, len(results)
                
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
    
    return None, None, 0

def get_sensor_info():
    """Obtiene información básica del sensor"""
    print(f"\n=== Información del sensor ===")
    
    sensor_id = 1045
    api_url = f"{API_BASE_URL}/sensors/{sensor_id}"
    headers = get_api_headers()
    
    try:
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Información del sensor {sensor_id}:")
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"Error obteniendo info del sensor: {str(e)}")

if __name__ == "__main__":
    try:
        # Obtener información del sensor
        get_sensor_info()
        
        # Probar diferentes rangos
        date_from, date_to, count = test_different_date_ranges()
        
        if not count:
            # Si no encontramos datos, probar fechas específicas
            date_from, date_to, count = test_recent_measurements()
        
        print(f"\n=== RECOMENDACIÓN ===")
        if count > 0:
            print(f"✅ Usar período: {date_from} al {date_to}")
            print(f"✅ Este período tiene {count} mediciones disponibles")
        else:
            print(f"❌ No se encontraron datos recientes")
            print(f"💡 Puede que el sensor no esté activo o tenga retraso en datos")
            
    except Exception as e:
        print(f"Error general: {str(e)}")