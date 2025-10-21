#!/usr/bin/env python3
"""
Script para probar diferentes límites en la API de países OpenAQ
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

def test_different_limits():
    """Prueba diferentes límites para ver si hay más países"""
    limits_to_test = [100, 200, 500, 1000]
    
    for limit in limits_to_test:
        print(f"\n=== Probando límite: {limit} ===")
        api_url = f"{API_BASE_URL}/countries?limit={limit}"
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            print(f"URL: {api_url}")
            print(f"Países obtenidos: {len(results)}")
            print(f"Metadatos: {json.dumps({k: v for k, v in data.items() if k != 'results'}, indent=2)}")
            
            if len(results) > 0:
                print(f"Primer país: {results[0].get('name', 'N/A')} ({results[0].get('code', 'N/A')})")
                print(f"Último país: {results[-1].get('name', 'N/A')} ({results[-1].get('code', 'N/A')})")
            
        except Exception as e:
            print(f"Error con límite {limit}: {str(e)}")

def test_pagination_with_offsets():
    """Prueba usando offset para ver si hay más países"""
    print(f"\n=== Probando con offsets ===")
    
    offsets_to_test = [0, 100, 200, 300]
    limit = 100
    
    all_countries = []
    
    for offset in offsets_to_test:
        print(f"\nOffset: {offset}")
        api_url = f"{API_BASE_URL}/countries?limit={limit}&offset={offset}"
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            print(f"URL: {api_url}")
            print(f"Países obtenidos: {len(results)}")
            
            if len(results) > 0:
                print(f"Primer país: {results[0].get('name', 'N/A')} ({results[0].get('code', 'N/A')})")
                print(f"Último país: {results[-1].get('name', 'N/A')} ({results[-1].get('code', 'N/A')})")
                
                # Verificar si son países únicos
                new_countries = [c for c in results if c.get('code') not in [ac.get('code') for ac in all_countries]]
                print(f"Países nuevos: {len(new_countries)}")
                
                all_countries.extend(new_countries)
            else:
                print("No hay países en este offset")
                break
                
        except Exception as e:
            print(f"Error con offset {offset}: {str(e)}")
    
    print(f"\nTotal de países únicos encontrados: {len(all_countries)}")
    
    # Mostrar algunos códigos únicos
    unique_codes = sorted(set([c.get('code') for c in all_countries if c.get('code')]))
    print(f"Códigos únicos (primeros 20): {unique_codes[:20]}")
    if len(unique_codes) > 20:
        print(f"... y {len(unique_codes) - 20} más")

def test_page_parameter():
    """Prueba usando el parámetro page"""
    print(f"\n=== Probando con parámetro 'page' ===")
    
    pages_to_test = [1, 2, 3, 4, 5]
    limit = 100
    
    for page in pages_to_test:
        print(f"\nPágina: {page}")
        api_url = f"{API_BASE_URL}/countries?page={page}&limit={limit}"
        headers = get_api_headers()
        
        try:
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            print(f"URL: {api_url}")
            print(f"Países obtenidos: {len(results)}")
            
            if len(results) > 0:
                print(f"Primer país: {results[0].get('name', 'N/A')} ({results[0].get('code', 'N/A')})")
                print(f"Último país: {results[-1].get('name', 'N/A')} ({results[-1].get('code', 'N/A')})")
            else:
                print("No hay países en esta página")
                break
                
        except Exception as e:
            print(f"Error con página {page}: {str(e)}")

if __name__ == "__main__":
    try:
        test_different_limits()
        test_pagination_with_offsets()
        test_page_parameter()
        
    except Exception as e:
        print(f"Error general: {str(e)}")