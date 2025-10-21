-- Script de inicialización para PostgreSQL
-- Crea la base de datos adicional para datos de OpenAQ

-- Crear usuario y base de datos para los datos de OpenAQ
CREATE USER openaq_user WITH PASSWORD 'openaq_password';
CREATE DATABASE openaq_data OWNER openaq_user;

-- Otorgar permisos
GRANT ALL PRIVILEGES ON DATABASE openaq_data TO openaq_user;

-- Conectar a la base de datos de OpenAQ para configuraciones adicionales
\c openaq_data;

-- Configurar esquema por defecto
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO openaq_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO openaq_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO openaq_user;

-- Permitir que el usuario cree tablas en el futuro
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO openaq_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO openaq_user;