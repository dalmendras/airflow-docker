#!/usr/bin/env python3
"""
Script para verificar la calidad de los datos cargados en PostgreSQL
"""
import sys
import os
sys.path.append('/opt/airflow/dags')

from openaq_complete_pipeline import get_postgres_connection

def verify_data_quality():
    """Verifica la calidad de los datos cargados"""
    try:
        print("🔍 Verificando calidad de datos en PostgreSQL...")
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        print("\n📊 ESTADÍSTICAS GENERALES:")
        print("=" * 50)
        
        # Estadísticas de países
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT code) FROM openaq_countries WHERE code IS NOT NULL;")
        countries_stats = cursor.fetchone()
        print(f"Países: {countries_stats[0]} total, {countries_stats[1]} códigos únicos")
        
        # Top 5 países con más ubicaciones
        cursor.execute("""
        SELECT country_name, COUNT(*) as locations_count 
        FROM openaq_locations 
        WHERE country_name IS NOT NULL 
        GROUP BY country_name 
        ORDER BY locations_count DESC 
        LIMIT 5;
        """)
        top_countries = cursor.fetchall()
        
        print(f"\n🌍 TOP 5 PAÍSES POR UBICACIONES:")
        print("=" * 40)
        for country, count in top_countries:
            print(f"  {country}: {count} ubicaciones")
        
        # Estadísticas de parámetros más monitoreados
        cursor.execute("""
        SELECT name, display_name, units, COUNT(*) as usage_count
        FROM openaq_parameters 
        WHERE name IS NOT NULL
        GROUP BY name, display_name, units
        ORDER BY name
        LIMIT 10;
        """)
        parameters = cursor.fetchall()
        
        print(f"\n🧪 PARÁMETROS MONITOREADOS (primeros 10):")
        print("=" * 50)
        for name, display_name, units, count in parameters:
            print(f"  {name} ({display_name}): {units}")
        
        # Verificar distribución temporal
        cursor.execute("""
        SELECT 
            COUNT(CASE WHEN datetime_first IS NOT NULL THEN 1 END) as with_start_date,
            COUNT(CASE WHEN datetime_last IS NOT NULL THEN 1 END) as with_end_date,
            COUNT(*) as total
        FROM openaq_locations;
        """)
        temporal_stats = cursor.fetchone()
        
        print(f"\n📅 DISTRIBUCIÓN TEMPORAL:")
        print("=" * 30)
        print(f"  Ubicaciones con fecha inicio: {temporal_stats[0]}/{temporal_stats[2]}")
        print(f"  Ubicaciones con fecha fin: {temporal_stats[1]}/{temporal_stats[2]}")
        
        # Verificar coordenadas geográficas
        cursor.execute("""
        SELECT 
            COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates,
            COUNT(*) as total,
            MIN(latitude) as min_lat,
            MAX(latitude) as max_lat,
            MIN(longitude) as min_lon,
            MAX(longitude) as max_lon
        FROM openaq_locations;
        """)
        geo_stats = cursor.fetchone()
        
        print(f"\n🗺️  DISTRIBUCIÓN GEOGRÁFICA:")
        print("=" * 35)
        print(f"  Ubicaciones con coordenadas: {geo_stats[0]}/{geo_stats[1]}")
        if geo_stats[2] is not None:
            print(f"  Rango latitud: {geo_stats[2]:.2f} a {geo_stats[3]:.2f}")
            print(f"  Rango longitud: {geo_stats[4]:.2f} a {geo_stats[5]:.2f}")
        
        # Ejemplo de consulta con JOIN
        cursor.execute("""
        SELECT 
            c.name as country_name,
            c.code as country_code,
            COUNT(l.id) as location_count
        FROM openaq_countries c
        LEFT JOIN openaq_locations l ON l.country_code = c.code
        GROUP BY c.name, c.code
        HAVING COUNT(l.id) > 0
        ORDER BY location_count DESC
        LIMIT 3;
        """)
        country_location_stats = cursor.fetchall()
        
        print(f"\n🔗 RELACIÓN PAÍSES-UBICACIONES (TOP 3):")
        print("=" * 45)
        for country_name, country_code, location_count in country_location_stats:
            print(f"  {country_name} ({country_code}): {location_count} ubicaciones")
        
        conn.close()
        print(f"\n✅ Verificación de calidad completada exitosamente!")
        print(f"🎯 Los datos están listos para análisis y consultas.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en la verificación: {str(e)}")
        return False

if __name__ == "__main__":
    success = verify_data_quality()
    sys.exit(0 if success else 1)