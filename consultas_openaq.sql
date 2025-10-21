-- CONSULTAS SQL ÚTILES PARA EXPLORAR DATOS DE OPENAQ
-- Base de datos: airflow_final.db
-- Tabla principal: openaq_countries

-- ==========================================
-- 1. VISTA GENERAL DE DATOS
-- ==========================================

-- Contar total de países
SELECT COUNT(*) as total_countries FROM openaq_countries;

-- Ver estructura de la tabla
.schema openaq_countries

-- ==========================================
-- 2. EXPLORACIÓN BÁSICA
-- ==========================================

-- Ver todos los países con sus códigos
SELECT code, name, datetimeFirst, datetimeLast 
FROM openaq_countries 
ORDER BY name;

-- Top 10 países con datos más recientes
SELECT code, name, datetimeLast
FROM openaq_countries 
WHERE datetimeLast IS NOT NULL
ORDER BY datetimeLast DESC 
LIMIT 10;

-- Países con datos históricos más antiguos
SELECT code, name, datetimeFirst
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL
ORDER BY datetimeFirst ASC 
LIMIT 10;

-- ==========================================
-- 3. ANÁLISIS DE PARÁMETROS
-- ==========================================

-- Países con más parámetros monitoreados (requiere extensión JSON)
-- Nota: Esta consulta funciona si SQLite tiene soporte JSON
SELECT code, name, 
       json_array_length(parameters) as param_count,
       datetimeFirst
FROM openaq_countries 
WHERE json_array_length(parameters) > 0
ORDER BY param_count DESC 
LIMIT 15;

-- Ver parámetros de un país específico (ejemplo: Estados Unidos)
SELECT name, parameters 
FROM openaq_countries 
WHERE code = 'US';

-- ==========================================
-- 4. ANÁLISIS TEMPORAL
-- ==========================================

-- Países agrupados por año de inicio de monitoreo
SELECT 
    substr(datetimeFirst, 1, 4) as year_started,
    COUNT(*) as countries_count
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL
GROUP BY substr(datetimeFirst, 1, 4)
ORDER BY year_started;

-- Duración del monitoreo por país (en días aproximados)
SELECT code, name,
       ROUND(julianday(datetimeLast) - julianday(datetimeFirst)) as monitoring_days
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL AND datetimeLast IS NOT NULL
ORDER BY monitoring_days DESC
LIMIT 10;

-- ==========================================
-- 5. BÚSQUEDAS ESPECÍFICAS
-- ==========================================

-- Buscar países por región/continente
SELECT code, name, datetimeFirst
FROM openaq_countries 
WHERE code IN ('US', 'CA', 'MX', 'BR', 'AR', 'CL') -- América
ORDER BY name;

-- Países europeos (códigos comunes)
SELECT code, name, datetimeFirst
FROM openaq_countries 
WHERE code IN ('GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'SE', 'NO', 'FI', 'DK')
ORDER BY name;

-- Países asiáticos
SELECT code, name, datetimeFirst
FROM openaq_countries 
WHERE code IN ('CN', 'JP', 'KR', 'IN', 'TH', 'VN', 'MY', 'SG', 'ID', 'PH')
ORDER BY name;

-- ==========================================
-- 6. REPORTES AVANZADOS
-- ==========================================

-- Reporte de cobertura temporal
SELECT 
    'Total países' as metric,
    COUNT(*) as value
FROM openaq_countries
UNION ALL
SELECT 
    'Con fecha inicio' as metric,
    COUNT(*) as value
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL
UNION ALL
SELECT 
    'Con fecha última' as metric,
    COUNT(*) as value
FROM openaq_countries 
WHERE datetimeLast IS NOT NULL
UNION ALL
SELECT 
    'Activos hoy' as metric,
    COUNT(*) as value
FROM openaq_countries 
WHERE date(datetimeLast) = date('now');

-- Países que actualizaron datos en las últimas 24 horas
SELECT code, name, datetimeLast
FROM openaq_countries 
WHERE datetime(datetimeLast) > datetime('now', '-1 day')
ORDER BY datetimeLast DESC;

-- ==========================================
-- 7. ESTADÍSTICAS DE EXTRACCIÓN
-- ==========================================

-- Información sobre la última extracción
SELECT 
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction,
    COUNT(*) as total_records
FROM openaq_countries;

-- Verificar duplicados por código de país
SELECT code, COUNT(*) as duplicates
FROM openaq_countries 
GROUP BY code 
HAVING COUNT(*) > 1;

-- ==========================================
-- 8. CONSULTAS PARA ANÁLISIS DE CALIDAD DEL AIRE
-- ==========================================

-- Buscar países que monitorean PM2.5 y PM10
SELECT code, name, parameters
FROM openaq_countries 
WHERE parameters LIKE '%pm25%' AND parameters LIKE '%pm10%';

-- Buscar países que monitorean ozono
SELECT code, name, parameters
FROM openaq_countries 
WHERE parameters LIKE '%o3%';

-- Buscar países con monitoreo de NO2
SELECT code, name, parameters
FROM openaq_countries 
WHERE parameters LIKE '%no2%';

-- ==========================================
-- COMANDOS ÚTILES DE SQLITE
-- ==========================================

-- Mostrar todas las tablas
.tables

-- Mostrar información de la base de datos
.dbinfo

-- Exportar resultados a CSV
.mode csv
.output paises_openaq.csv
SELECT * FROM openaq_countries;
.output stdout

-- Cambiar formato de salida
.mode column
.headers on

-- ==========================================
-- NOTAS IMPORTANTES
-- ==========================================

/*
1. La tabla openaq_countries contiene 100 países con datos de calidad del aire
2. Los campos de fecha están en formato ISO 8601 (UTC)
3. El campo 'parameters' contiene JSON con información de sensores disponibles
4. Usar LIKE para búsquedas de texto en campos JSON
5. Las consultas JSON funcionan mejor en versiones recientes de SQLite

Para DBeaver:
- Usar airflow_final.db como fuente de datos
- Habilitar "Show row numbers" para mejor navegación
- Usar el editor SQL para ejecutar estas consultas
- Exportar resultados usando Ctrl+Shift+E
*/