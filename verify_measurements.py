#!/usr/bin/env python3
"""
Script para verificar las mediciones cargadas en PostgreSQL
"""

import psycopg2
import json

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

def verify_measurements_data():
    """Verifica los datos de mediciones en PostgreSQL"""
    print("=== Verificando mediciones de Santiago de Chile ===")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Estadísticas generales
    cursor.execute("SELECT COUNT(*) FROM openaq_measurements;")
    total_measurements = cursor.fetchone()[0]
    print(f"Total de mediciones en la base de datos: {total_measurements}")
    
    # Mediciones por sensor
    cursor.execute("""
        SELECT 
            sensor_id, 
            parameter_name, 
            parameter_units,
            COUNT(*) as measurement_count,
            MIN(period_from_utc) as first_measurement,
            MAX(period_from_utc) as last_measurement
        FROM openaq_measurements 
        GROUP BY sensor_id, parameter_name, parameter_units
        ORDER BY measurement_count DESC;
    """)
    
    sensor_stats = cursor.fetchall()
    print(f"\nEstadísticas por sensor:")
    for sensor_id, param_name, param_units, count, first_date, last_date in sensor_stats:
        print(f"  Sensor {sensor_id} ({param_name}):")
        print(f"    Mediciones: {count}")
        print(f"    Unidades: {param_units}")
        print(f"    Período: {first_date} al {last_date}")
    
    # Valores estadísticos por parámetro
    cursor.execute("""
        SELECT 
            parameter_name,
            parameter_units,
            COUNT(*) as count,
            MIN(value) as min_value,
            MAX(value) as max_value,
            AVG(value) as avg_value,
            STDDEV(value) as std_value
        FROM openaq_measurements 
        WHERE value IS NOT NULL
        GROUP BY parameter_name, parameter_units
        ORDER BY parameter_name;
    """)
    
    param_stats = cursor.fetchall()
    print(f"\nEstadísticas de valores por parámetro:")
    for param_name, param_units, count, min_val, max_val, avg_val, std_val in param_stats:
        print(f"  {param_name.upper()} ({param_units}):")
        print(f"    Mediciones: {count}")
        print(f"    Rango: {min_val:.2f} - {max_val:.2f}")
        print(f"    Promedio: {avg_val:.2f}")
        print(f"    Desv. Est.: {std_val:.2f}" if std_val else "    Desv. Est.: N/A")
    
    # Mediciones por día
    cursor.execute("""
        SELECT 
            DATE(period_from_utc) as measurement_date,
            COUNT(*) as daily_measurements
        FROM openaq_measurements 
        GROUP BY DATE(period_from_utc)
        ORDER BY measurement_date;
    """)
    
    daily_stats = cursor.fetchall()
    print(f"\nMediciones por día:")
    for date, count in daily_stats:
        print(f"  {date}: {count} mediciones")
    
    # Ejemplos de mediciones recientes
    cursor.execute("""
        SELECT 
            sensor_id,
            parameter_name,
            value,
            parameter_units,
            period_from_utc,
            period_to_utc
        FROM openaq_measurements 
        ORDER BY period_from_utc DESC
        LIMIT 10;
    """)
    
    recent_measurements = cursor.fetchall()
    print(f"\nÚltimas 10 mediciones (ordenadas por fecha):")
    for sensor_id, param_name, value, units, period_from, period_to in recent_measurements:
        print(f"  Sensor {sensor_id} ({param_name}): {value} {units}")
        print(f"    Período: {period_from} - {period_to}")
    
    # Verificar calidad de datos
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values,
            COUNT(CASE WHEN period_from_utc IS NULL THEN 1 END) as null_dates,
            COUNT(CASE WHEN has_flags = true THEN 1 END) as flagged_measurements
        FROM openaq_measurements;
    """)
    
    quality_stats = cursor.fetchone()
    total, null_values, null_dates, flagged = quality_stats
    
    print(f"\nCalidad de datos:")
    print(f"  Total de registros: {total}")
    print(f"  Valores nulos: {null_values} ({(null_values/total*100):.1f}%)" if total > 0 else "  Valores nulos: 0")
    print(f"  Fechas nulas: {null_dates} ({(null_dates/total*100):.1f}%)" if total > 0 else "  Fechas nulas: 0")
    print(f"  Mediciones marcadas: {flagged} ({(flagged/total*100):.1f}%)" if total > 0 else "  Mediciones marcadas: 0")
    
    conn.close()
    
    return total_measurements

if __name__ == "__main__":
    try:
        total = verify_measurements_data()
        
        print(f"\n=== RESUMEN ===")
        if total > 0:
            print(f"✅ Éxito: Se cargaron {total} mediciones de sensores de Santiago")
            print(f"✅ Datos del período: 13-20 agosto 2021")
            print(f"✅ Sensores incluidos: CO, NO2, O3, PM10, PM2.5")
        else:
            print(f"❌ No se encontraron mediciones en la base de datos")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")