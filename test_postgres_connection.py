#!/usr/bin/env python3
"""
Script de prueba para verificar la conexión a PostgreSQL
"""

import psycopg2
import os
import sys

def test_postgres_connection():
    """Prueba la conexión a PostgreSQL"""
    try:
        # Configuración de conexión
        conn_params = {
            'host': os.environ.get('POSTGRES_HOST', 'localhost'),
            'port': os.environ.get('POSTGRES_PORT', '5432'),
            'database': os.environ.get('POSTGRES_DB', 'openaq_data'),
            'user': os.environ.get('POSTGRES_USER', 'openaq_user'),
            'password': os.environ.get('POSTGRES_PASSWORD', 'openaq_password')
        }
        
        print("Probando conexión a PostgreSQL...")
        print(f"Host: {conn_params['host']}")
        print(f"Puerto: {conn_params['port']}")
        print(f"Base de datos: {conn_params['database']}")
        print(f"Usuario: {conn_params['user']}")
        
        # Conectar
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Probar consulta simple
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Conexión exitosa!")
        print(f"Versión PostgreSQL: {version[0]}")
        
        # Verificar si existen las tablas
        cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('openaq_countries', 'openaq_locations', 'openaq_parameters');
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"✅ Tablas encontradas: {[table[0] for table in tables]}")
        else:
            print("⚠️ No se encontraron tablas del proyecto OpenAQ")
        
        # Verificar permisos
        cursor.execute("SELECT current_user, current_database();")
        user_info = cursor.fetchone()
        print(f"Usuario actual: {user_info[0]}")
        print(f"Base de datos actual: {user_info[1]}")
        
        conn.close()
        print("✅ Prueba completada exitosamente")
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_postgres_connection()
    sys.exit(0 if success else 1)