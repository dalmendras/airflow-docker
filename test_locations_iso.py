#!/usr/bin/env python3
"""
Script para probar la nueva función de extracción de locations por ISO
"""

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

def test_locations_by_iso(iso_code):
    """Prueba la extracción de locations para un país específico"""
    print(f"\n=== Probando ubicaciones para {iso_code} ===")
    
    page = 1
    limit = 100
    all_locations = []
    
    while True:
        api_url = f"{API_BASE_URL}/locations"
        params = {
            'iso': iso_code,
            'page': page,
            'limit': limit
        }
        headers = get_api_headers()
        
        print(f"Página {page}: {api_url}?iso={iso_code}&page={page}&limit={limit}")
        
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            locations = data.get('results', [])
            meta = data.get('meta', {})
            
            print(f"  Ubicaciones en esta página: {len(locations)}")
            print(f"  Metadatos: {json.dumps(meta, indent=2)}")
            
            if not locations:
                print("  No hay más ubicaciones")
                break
            
            all_locations.extend(locations)
            
            # Mostrar algunas ubicaciones de ejemplo
            if page == 1 and locations:
                print(f"  Ejemplo de ubicación:")
                loc = locations[0]
                print(f"    ID: {loc.get('id', 'N/A')}")
                print(f"    Nombre: {loc.get('name', 'N/A')}")
                print(f"    Ciudad: {loc.get('locality', 'N/A')}")
                print(f"    País: {loc.get('country', {}).get('name', 'N/A')} ({loc.get('country', {}).get('code', 'N/A')})")
                print(f"    Coordenadas: {loc.get('coordinates', {}).get('latitude', 'N/A')}, {loc.get('coordinates', {}).get('longitude', 'N/A')}")
            
            # Verificar si hay más páginas
            found = meta.get('found', 0)
            if found > 0 and len(all_locations) >= found:
                print(f"  Se obtuvieron todas las ubicaciones: {found}")
                break
            
            if len(locations) < limit:
                print(f"  Última página detectada")
                break
            
            page += 1
            
            # Límite de seguridad
            if page > 10:
                print(f"  Límite de seguridad alcanzado")
                break
                
        except Exception as e:
            print(f"  Error en página {page}: {str(e)}")
            break
    
    print(f"Total ubicaciones para {iso_code}: {len(all_locations)}")
    return len(all_locations)

def test_multiple_countries():
    """Prueba múltiples países"""
    countries_to_test = ['CL', 'AR', 'BR', 'CO', 'PE']
    total_locations = 0
    
    print("=== Probando múltiples países ===")
    
    for iso_code in countries_to_test:
        try:
            count = test_locations_by_iso(iso_code)
            total_locations += count
        except Exception as e:
            print(f"Error con país {iso_code}: {str(e)}")
    
    print(f"\n=== RESUMEN ===")
    print(f"Países probados: {countries_to_test}")
    print(f"Total de ubicaciones: {total_locations}")
    
    return total_locations

if __name__ == "__main__":
    try:
        # Probar primero solo Chile como ejemplo
        test_locations_by_iso('CL')
        
        # Luego probar múltiples países
        test_multiple_countries()
        
    except Exception as e:
        print(f"Error general: {str(e)}")