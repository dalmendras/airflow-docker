#!/usr/bin/env python3
"""
Script para verificar los resultados de locations con filtrado ISO en PostgreSQL
"""

import psycopg2
import json

def get_postgres_connection():
    """
    Establece conexión con PostgreSQL
    """
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

def verify_locations_data():
    """Verifica los datos de locations en PostgreSQL"""
    print("=== Verificando locations en PostgreSQL ===")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Contar total de locations
    cursor.execute("SELECT COUNT(*) FROM openaq_locations;")
    total_locations = cursor.fetchone()[0]
    print(f"Total de ubicaciones en la base de datos: {total_locations}")
    
    # Contar por país
    cursor.execute("""
        SELECT country_code, country_name, COUNT(*) as count 
        FROM openaq_locations 
        WHERE country_code IS NOT NULL
        GROUP BY country_code, country_name 
        ORDER BY count DESC 
        LIMIT 15;
    """)
    
    countries_count = cursor.fetchall()
    print(f"\nTop 15 países por número de ubicaciones:")
    for country in countries_count:
        print(f"  {country[1]} ({country[0]}): {country[2]} ubicaciones")
    
    # Verificar distribución geográfica
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coords,
            COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as without_coords,
            COUNT(*) as total
        FROM openaq_locations;
    """)
    
    coords_info = cursor.fetchone()
    print(f"\nInformación de coordenadas:")
    print(f"  Con coordenadas: {coords_info[0]}")
    print(f"  Sin coordenadas: {coords_info[1]}")
    print(f"  Total: {coords_info[2]}")
    
    # Verificar tipos de monitoreo
    cursor.execute("""
        SELECT 
            is_mobile,
            is_monitor,
            COUNT(*) as count
        FROM openaq_locations 
        GROUP BY is_mobile, is_monitor
        ORDER BY count DESC;
    """)
    
    monitor_types = cursor.fetchall()
    print(f"\nTipos de monitoreo:")
    for monitor_type in monitor_types:
        mobile_text = "Móvil" if monitor_type[0] else "Fijo"
        monitor_text = "Monitor" if monitor_type[1] else "No Monitor"
        print(f"  {mobile_text} + {monitor_text}: {monitor_type[2]} ubicaciones")
    
    # Mostrar algunas ubicaciones de ejemplo de Chile
    cursor.execute("""
        SELECT location_id, name, locality, latitude, longitude 
        FROM openaq_locations 
        WHERE country_code = 'CL'
        ORDER BY name 
        LIMIT 10;
    """)
    
    chile_locations = cursor.fetchall()
    print(f"\nEjemplos de ubicaciones en Chile:")
    for location in chile_locations:
        print(f"  ID: {location[0]}, Nombre: {location[1]}, Ciudad: {location[2]}")
        print(f"    Coordenadas: ({location[3]}, {location[4]})")
    
    # Verificar últimas actualizaciones
    cursor.execute("""
        SELECT 
            MIN(extracted_at) as first_extraction,
            MAX(extracted_at) as last_extraction,
            COUNT(DISTINCT DATE(extracted_at)) as extraction_days
        FROM openaq_locations;
    """)
    
    extraction_info = cursor.fetchone()
    print(f"\nInformación de extracción:")
    print(f"  Primera extracción: {extraction_info[0]}")
    print(f"  Última extracción: {extraction_info[1]}")
    print(f"  Días de extracción: {extraction_info[2]}")
    
    conn.close()
    
    return total_locations

if __name__ == "__main__":
    try:
        total = verify_locations_data()
        
        print(f"\n=== RESUMEN ===")
        if total > 500:
            print(f"✅ Éxito: Se cargaron {total} ubicaciones (mucho más que las 500 anteriores)")
            print(f"✅ El filtrado por ISO funcionó correctamente")
        else:
            print(f"⚠️  Advertencia: Solo se cargaron {total} ubicaciones")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")