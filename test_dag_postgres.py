#!/usr/bin/env python3
"""
Script para probar la conexión PostgreSQL desde el DAG
"""
import sys
import os
sys.path.append('/opt/airflow/dags')

# Importar la función de conexión del DAG
from openaq_complete_pipeline import get_postgres_connection

def test_postgres_connection():
    """Prueba la conexión a PostgreSQL"""
    try:
        print("🔍 Probando conexión a PostgreSQL...")
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Probar una consulta básica
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Conexión exitosa! PostgreSQL Version: {version}")
        
        # Verificar si las tablas existen
        cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('openaq_countries', 'openaq_locations', 'openaq_parameters');
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"📋 Tablas existentes: {[table[0] for table in tables]}")
            
            # Verificar datos en las tablas
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                print(f"📊 {table_name}: {count} registros")
        else:
            print("📋 No se encontraron tablas del DAG. Esto es normal si es la primera vez.")
        
        conn.close()
        print("🎉 Test de conexión completado exitosamente!")
        return True
        
    except Exception as e:
        print(f"❌ Error en la conexión: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_postgres_connection()
    sys.exit(0 if success else 1)