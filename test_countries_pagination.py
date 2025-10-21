#!/usr/bin/env python3
"""
Script para probar la paginación de países en la API de OpenAQ
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

def test_countries_pagination():
    """Prueba la paginación de países"""
    print("=== Probando paginación de países ===")
    
    # Primera consulta sin parámetros
    api_url = f"{API_BASE_URL}/countries"
    headers = get_api_headers()
    
    print(f"Consultando: {api_url}")
    response = requests.get(api_url, headers=headers, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    print(f"\nRespuesta inicial:")
    print(f"- Results: {len(data.get('results', []))}")
    print(f"- Found: {data.get('found', 'N/A')}")
    print(f"- Limit: {data.get('limit', 'N/A')}")
    print(f"- Page: {data.get('page', 'N/A')}")
    print(f"- Pages: {data.get('pages', 'N/A')}")
    
    # Si hay más páginas, probar con límite más alto
    if data.get('pages', 1) > 1:
        print(f"\n=== Hay {data.get('pages')} páginas. Probando con límite más alto ===")
        
        # Intentar obtener todos los países con un límite alto
        api_url_all = f"{API_BASE_URL}/countries?limit=1000"
        print(f"Consultando: {api_url_all}")
        
        response_all = requests.get(api_url_all, headers=headers, timeout=30)
        response_all.raise_for_status()
        
        data_all = response_all.json()
        print(f"\nRespuesta con límite 1000:")
        print(f"- Results: {len(data_all.get('results', []))}")
        print(f"- Found: {data_all.get('found', 'N/A')}")
        print(f"- Limit: {data_all.get('limit', 'N/A')}")
        print(f"- Page: {data_all.get('page', 'N/A')}")
        print(f"- Pages: {data_all.get('pages', 'N/A')}")
        
        # Mostrar algunos ejemplos de países
        countries = data_all.get('results', [])
        print(f"\nPrimeros 5 países:")
        for i, country in enumerate(countries[:5]):
            print(f"  {i+1}. {country.get('name', 'N/A')} ({country.get('code', 'N/A')})")
        
        print(f"\nÚltimos 5 países:")
        for i, country in enumerate(countries[-5:]):
            print(f"  {len(countries)-4+i}. {country.get('name', 'N/A')} ({country.get('code', 'N/A')})")
        
        return len(countries)
    else:
        countries = data.get('results', [])
        print(f"\nTodos los países están en una sola página: {len(countries)}")
        return len(countries)

def test_pagination_manually():
    """Prueba la paginación manualmente página por página"""
    print(f"\n=== Probando paginación manual ===")
    
    all_countries = []
    page = 1
    limit = 100  # Límite por página
    
    while True:
        api_url = f"{API_BASE_URL}/countries?page={page}&limit={limit}"
        headers = get_api_headers()
        
        print(f"Página {page}: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        countries = data.get('results', [])
        
        print(f"  - Países en esta página: {len(countries)}")
        print(f"  - Total encontrado: {data.get('found', 'N/A')}")
        print(f"  - Páginas totales: {data.get('pages', 'N/A')}")
        
        if not countries:
            print("  - No hay más países")
            break
            
        all_countries.extend(countries)
        
        # Verificar si hay más páginas
        if page >= data.get('pages', 1):
            print("  - Última página alcanzada")
            break
            
        page += 1
        
        # Límite de seguridad
        if page > 10:
            print("  - Límite de seguridad alcanzado (10 páginas)")
            break
    
    print(f"\nTotal de países extraídos manualmente: {len(all_countries)}")
    return len(all_countries)

if __name__ == "__main__":
    try:
        total1 = test_countries_pagination()
        total2 = test_pagination_manually()
        
        print(f"\n=== RESUMEN ===")
        print(f"Método 1 (límite alto): {total1} países")
        print(f"Método 2 (paginación manual): {total2} países")
        
    except Exception as e:
        print(f"Error: {str(e)}")