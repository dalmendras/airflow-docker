-- ================================================================
-- CONSULTAS ESTACIONES CHILE - BASE DE DATOS POSTGRESQL
-- Conjunto de consultas optimizadas para explorar los datos 
-- de estaciones de Chile extraídos de la API de OpenAQ v3
-- ================================================================

-- 1. INFORMACIÓN GENERAL DE ESTACIONES
-- Obtener estadísticas básicas de la tabla de estaciones
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT id) as estaciones_unicas,
    COUNT(DISTINCT sensor_id) as sensores_unicos,
    COUNT(DISTINCT provider_id) as proveedores_unicos,
    COUNT(DISTINCT locality) as localidades_unicas,
    MIN(extracted_at) as primera_extraccion,
    MAX(extracted_at) as ultima_extraccion
FROM station;

-- 2. TOP ESTACIONES POR NÚMERO DE SENSORES
-- Estaciones que tienen más variedad de sensores
SELECT 
    id,
    name,
    locality,
    provider_name,
    COUNT(DISTINCT sensor_id) as num_sensores,
    STRING_AGG(DISTINCT sensor_name, ', ' ORDER BY sensor_name) as tipos_sensores
FROM station 
GROUP BY id, name, locality, provider_name
ORDER BY num_sensores DESC
LIMIT 15;

-- 3. DISTRIBUCIÓN DE ESTACIONES POR PROVEEDOR
-- Análisis de qué organizaciones proporcionan más datos
SELECT 
    provider_name,
    provider_id,
    COUNT(DISTINCT id) as num_estaciones,
    COUNT(*) as total_sensores,
    COUNT(DISTINCT sensor_name) as tipos_sensores_unicos
FROM station 
WHERE provider_name IS NOT NULL
GROUP BY provider_name, provider_id
ORDER BY num_estaciones DESC;

-- 4. TIPOS DE SENSORES MÁS COMUNES
-- Análisis de qué parámetros se monitorean más frecuentemente
SELECT 
    sensor_name,
    COUNT(*) as frecuencia,
    COUNT(DISTINCT id) as estaciones_diferentes,
    COUNT(DISTINCT provider_id) as proveedores_diferentes
FROM station 
WHERE sensor_name IS NOT NULL
GROUP BY sensor_name
ORDER BY frecuencia DESC;

-- 5. MAPA DE ESTACIONES POR LOCALIDAD
-- Distribución geográfica de estaciones en Chile
SELECT 
    locality,
    COUNT(DISTINCT id) as num_estaciones,
    COUNT(*) as total_sensores,
    COUNT(DISTINCT provider_name) as num_proveedores,
    ROUND(AVG(latitude)::numeric, 6) as lat_promedio,
    ROUND(AVG(longitude)::numeric, 6) as lon_promedio
FROM station 
WHERE locality IS NOT NULL
GROUP BY locality
ORDER BY num_estaciones DESC;

-- 6. ESTACIONES CON COORDENADAS COMPLETAS
-- Listado de estaciones con ubicación geográfica precisa
SELECT 
    id,
    name,
    locality,
    latitude,
    longitude,
    provider_name,
    COUNT(sensor_id) as num_sensores,
    STRING_AGG(sensor_name, ', ' ORDER BY sensor_name) as sensores
FROM station 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY id, name, locality, latitude, longitude, provider_name
ORDER BY locality, name;

-- 7. ANÁLISIS DE ESTACIONES MÓVILES VS FIJAS
-- Comparación entre estaciones móviles y fijas
SELECT 
    is_mobile,
    is_monitor,
    COUNT(DISTINCT id) as num_estaciones,
    COUNT(*) as total_registros,
    COUNT(DISTINCT sensor_name) as tipos_sensores
FROM station 
GROUP BY is_mobile, is_monitor
ORDER BY is_mobile, is_monitor;

-- 8. ESTACIONES POR ZONA HORARIA
-- Distribución de estaciones por zona horaria
SELECT 
    timezone,
    COUNT(DISTINCT id) as num_estaciones,
    COUNT(*) as total_sensores,
    STRING_AGG(DISTINCT locality, ', ' ORDER BY locality) as localidades
FROM station 
WHERE timezone IS NOT NULL
GROUP BY timezone
ORDER BY num_estaciones DESC;

-- 9. DETALLE COMPLETO DE UNA ESTACIÓN ESPECÍFICA
-- Ejemplo: Ver todos los sensores de una estación particular
-- (Cambiar el ID por el que desees consultar)
SELECT 
    s.id,
    s.name,
    s.locality,
    s.latitude,
    s.longitude,
    s.timezone,
    s.provider_name,
    s.sensor_id,
    s.sensor_name,
    s.is_mobile,
    s.is_monitor,
    s.extracted_at
FROM station s 
WHERE s.id = 2158  -- Cambiar por ID de interés
ORDER BY s.sensor_name;

-- 10. BÚSQUEDA POR LOCALIDAD ESPECÍFICA
-- Ejemplo: Todas las estaciones de Santiago
SELECT 
    id,
    name,
    locality,
    provider_name,
    COUNT(sensor_id) as num_sensores,
    STRING_AGG(sensor_name, ', ' ORDER BY sensor_name) as tipos_sensores,
    latitude,
    longitude
FROM station 
WHERE locality ILIKE '%santiago%'
GROUP BY id, name, locality, provider_name, latitude, longitude
ORDER BY name;

-- 11. RELACIÓN CON TABLA DE PAÍSES
-- Verificar la relación con los datos de países
SELECT 
    c.code as codigo_pais,
    c.name as nombre_pais,
    COUNT(DISTINCT s.id) as num_estaciones,
    COUNT(s.id) as total_sensores,
    COUNT(DISTINCT s.provider_name) as num_proveedores
FROM station s
JOIN openaq_countries c ON s.country_id = c.country_id
GROUP BY c.code, c.name
ORDER BY num_estaciones DESC;

-- 12. ESTACIONES CON MÁS VARIEDAD DE PARÁMETROS
-- Top estaciones por diversidad de mediciones
WITH estacion_parametros AS (
    SELECT 
        id,
        name,
        locality,
        provider_name,
        COUNT(DISTINCT sensor_name) as variedad_parametros,
        COUNT(sensor_id) as total_sensores
    FROM station 
    GROUP BY id, name, locality, provider_name
)
SELECT 
    id,
    name,
    locality,
    provider_name,
    variedad_parametros,
    total_sensores,
    ROUND((variedad_parametros::decimal / total_sensores), 2) as ratio_variedad
FROM estacion_parametros
WHERE total_sensores > 1
ORDER BY variedad_parametros DESC, total_sensores DESC
LIMIT 20;

-- 13. ANÁLISIS TEMPORAL DE EXTRACCIÓN
-- Información sobre cuándo fueron extraídos los datos
SELECT 
    DATE(extracted_at) as fecha_extraccion,
    COUNT(*) as registros_extraidos,
    COUNT(DISTINCT id) as estaciones_unicas,
    COUNT(DISTINCT sensor_id) as sensores_unicos,
    MIN(extracted_at) as primera_extraccion_dia,
    MAX(extracted_at) as ultima_extraccion_dia
FROM station 
GROUP BY DATE(extracted_at)
ORDER BY fecha_extraccion DESC;

-- 14. BÚSQUEDA DE SENSORES ESPECÍFICOS
-- Ejemplo: Buscar estaciones que miden PM2.5
SELECT 
    s.id,
    s.name,
    s.locality,
    s.latitude,
    s.longitude,
    s.provider_name,
    s.sensor_name,
    s.sensor_id
FROM station s 
WHERE s.sensor_name ILIKE '%pm2.5%' 
   OR s.sensor_name ILIKE '%pm25%'
ORDER BY s.locality, s.name;

-- 15. VERIFICACIÓN DE INTEGRIDAD DE DATOS
-- Detectar posibles inconsistencias en los datos
SELECT 
    'Registros sin ID de estación' as tipo_problema,
    COUNT(*) as cantidad
FROM station 
WHERE id IS NULL
UNION ALL
SELECT 
    'Registros sin ID de sensor' as tipo_problema,
    COUNT(*) as cantidad
FROM station 
WHERE sensor_id IS NULL
UNION ALL
SELECT 
    'Registros sin nombre de estación' as tipo_problema,
    COUNT(*) as cantidad
FROM station 
WHERE name IS NULL
UNION ALL
SELECT 
    'Registros sin coordenadas' as tipo_problema,
    COUNT(*) as cantidad
FROM station 
WHERE latitude IS NULL OR longitude IS NULL
UNION ALL
SELECT 
    'Registros sin proveedor' as tipo_problema,
    COUNT(*) as cantidad
FROM station 
WHERE provider_name IS NULL;

-- ================================================================
-- CONSULTAS GEOESPACIALES AVANZADAS
-- ================================================================

-- 16. DENSIDAD DE ESTACIONES POR ÁREA
-- Calcular densidad aproximada usando bounding box
WITH chile_bbox AS (
    SELECT 
        MIN(latitude) as min_lat,
        MAX(latitude) as max_lat,
        MIN(longitude) as min_lon,
        MAX(longitude) as max_lon,
        COUNT(DISTINCT id) as total_estaciones
    FROM station 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)
SELECT 
    min_lat,
    max_lat,
    min_lon,
    max_lon,
    total_estaciones,
    ROUND((max_lat - min_lat) * (max_lon - min_lon), 4) as area_aproximada,
    ROUND(total_estaciones / ((max_lat - min_lat) * (max_lon - min_lon)), 2) as densidad_estaciones
FROM chile_bbox;

-- 17. ESTACIONES POR CUADRANTE GEOGRÁFICO
-- Dividir Chile en cuadrantes y ver distribución
WITH cuadrantes AS (
    SELECT 
        id,
        name,
        locality,
        latitude,
        longitude,
        CASE 
            WHEN latitude >= -30 AND longitude >= -70 THEN 'Norte-Este'
            WHEN latitude >= -30 AND longitude < -70 THEN 'Norte-Oeste'
            WHEN latitude < -30 AND longitude >= -70 THEN 'Sur-Este'
            ELSE 'Sur-Oeste'
        END as cuadrante
    FROM station 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)
SELECT 
    cuadrante,
    COUNT(DISTINCT id) as num_estaciones,
    STRING_AGG(DISTINCT locality, ', ' ORDER BY locality) as localidades_principales
FROM cuadrantes
GROUP BY cuadrante
ORDER BY num_estaciones DESC;

-- ================================================================
-- NOTAS DE USO:
-- - Estas consultas están optimizadas para PostgreSQL
-- - Usan funciones de agregación y análisis geoespacial básico
-- - Incluyen análisis de calidad de datos y consistencia
-- - Aprovechan los índices creados en el DAG para mejor rendimiento
-- - Para consultas geoespaciales avanzadas, considerar PostGIS
-- ================================================================