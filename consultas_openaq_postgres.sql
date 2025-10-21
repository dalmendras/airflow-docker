-- ================================================================
-- CONSULTAS OPENAQ - BASE DE DATOS POSTGRESQL
-- Conjunto de consultas optimizadas para explorar los datos 
-- de países extraídos de la API de OpenAQ v3
-- ================================================================

-- 1. INFORMACIÓN GENERAL DE PAÍSES
-- Obtener estadísticas básicas de la tabla de países
SELECT 
    COUNT(*) as total_paises,
    COUNT(DISTINCT code) as codigos_unicos,
    COUNT(DISTINCT country_id) as ids_unicos,
    SUM(locations) as total_ubicaciones,
    SUM(sources) as total_fuentes,
    MIN(extracted_at) as primera_extraccion,
    MAX(extracted_at) as ultima_extraccion
FROM openaq_countries;

-- 2. TOP 10 PAÍSES CON MÁS UBICACIONES
-- Países que tienen más estaciones de monitoreo
SELECT 
    code,
    name,
    locations as estaciones,
    sources as fuentes,
    jsonb_array_length(parameters) as parametros_monitoreados,
    datetimeFirst as primer_dato,
    datetimeLast as ultimo_dato
FROM openaq_countries 
WHERE locations > 0
ORDER BY locations DESC 
LIMIT 10;

-- 3. PAÍSES POR NÚMERO DE PARÁMETROS MONITOREADOS
-- Análisis de qué países monitorean más tipos de contaminantes
SELECT 
    code,
    name,
    jsonb_array_length(parameters) as num_parametros,
    parameters,
    locations,
    sources
FROM openaq_countries 
WHERE jsonb_array_length(parameters) > 0
ORDER BY num_parametros DESC, locations DESC
LIMIT 15;

-- 4. PAÍSES CON DATOS MÁS RECIENTES
-- Identificar países con monitoreo activo
SELECT 
    code,
    name,
    datetimeLast as ultimo_dato,
    locations,
    sources,
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - datetimeLast)) as dias_desde_ultimo_dato
FROM openaq_countries 
WHERE datetimeLast IS NOT NULL
ORDER BY datetimeLast DESC 
LIMIT 10;

-- 5. DISTRIBUCIÓN REGIONAL ESTIMADA
-- Estimación de distribución por regiones basada en zonas horarias
SELECT 
    CASE 
        WHEN datetimeFirst::text LIKE '%T01:00:00%' OR datetimeFirst::text LIKE '%T02:00:00%' THEN 'Europa/África'
        WHEN datetimeFirst::text LIKE '%T05:00:00%' OR datetimeFirst::text LIKE '%T06:00:00%' THEN 'Asia'
        WHEN datetimeFirst::text LIKE '%T08:00:00%' OR datetimeFirst::text LIKE '%T09:00:00%' THEN 'Oceanía'
        ELSE 'América/Otros'
    END as region_estimada,
    COUNT(*) as num_paises,
    SUM(locations) as total_estaciones,
    SUM(sources) as total_fuentes
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL
GROUP BY region_estimada
ORDER BY num_paises DESC;

-- 6. ANÁLISIS DE PARÁMETROS MONITOREADOS
-- Extraer y contar todos los tipos de contaminantes monitoreados
SELECT 
    param_element.value as parametro,
    COUNT(*) as paises_que_monitorean,
    SUM(locations) as total_estaciones,
    STRING_AGG(code, ', ' ORDER BY code) as codigos_paises
FROM openaq_countries,
     jsonb_array_elements_text(parameters) as param_element
WHERE jsonb_array_length(parameters) > 0
GROUP BY param_element.value
ORDER BY paises_que_monitorean DESC;

-- 7. PAÍSES SIN DATOS RECIENTES
-- Identificar países que podrían tener estaciones inactivas
SELECT 
    code,
    name,
    locations,
    datetimeLast,
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - datetimeLast)) as dias_inactivo
FROM openaq_countries 
WHERE datetimeLast IS NOT NULL 
    AND datetimeLast < CURRENT_TIMESTAMP - INTERVAL '30 days'
    AND locations > 0
ORDER BY dias_inactivo DESC;

-- 8. RESUMEN DE ACTIVIDAD POR AÑO
-- Análisis temporal de cuándo empezaron los países a reportar
SELECT 
    EXTRACT(YEAR FROM datetimeFirst) as año_inicio,
    COUNT(*) as paises_iniciaron,
    STRING_AGG(code, ', ' ORDER BY code) as codigos
FROM openaq_countries 
WHERE datetimeFirst IS NOT NULL
GROUP BY EXTRACT(YEAR FROM datetimeFirst)
ORDER BY año_inicio DESC;

-- 9. PAÍSES CON MAYOR DENSIDAD DE ESTACIONES
-- Relación estaciones/fuentes para identificar eficiencia
SELECT 
    code,
    name,
    locations,
    sources,
    CASE 
        WHEN sources > 0 THEN ROUND((locations::decimal / sources), 2)
        ELSE 0 
    END as estaciones_por_fuente,
    jsonb_array_length(parameters) as parametros
FROM openaq_countries 
WHERE locations > 0 AND sources > 0
ORDER BY estaciones_por_fuente DESC
LIMIT 15;

-- 10. BÚSQUEDA POR REGIÓN ESPECÍFICA
-- Ejemplo: Buscar países de América Latina (códigos comunes)
SELECT 
    code,
    name,
    locations,
    sources,
    parameters,
    datetimeFirst,
    datetimeLast
FROM openaq_countries 
WHERE code IN ('AR', 'BR', 'CL', 'CO', 'MX', 'PE', 'UY', 'EC', 'BO', 'PY', 'VE')
ORDER BY locations DESC;

-- 11. ANÁLISIS DETALLADO DE METADATA DE EXTRACCIÓN
-- Información sobre cuándo fueron extraídos los datos
SELECT 
    DATE(extracted_at) as fecha_extraccion,
    COUNT(*) as registros_extraidos,
    COUNT(DISTINCT code) as paises_unicos,
    MIN(extracted_at) as primera_extraccion_dia,
    MAX(extracted_at) as ultima_extraccion_dia
FROM openaq_countries 
GROUP BY DATE(extracted_at)
ORDER BY fecha_extraccion DESC;

-- ================================================================
-- CONSULTAS ADICIONALES PARA ANÁLISIS ESPECÍFICOS
-- ================================================================

-- 12. PAÍSES CON PARÁMETROS ESPECÍFICOS
-- Buscar países que monitorean PM2.5 y PM10
SELECT 
    code,
    name,
    locations,
    parameters
FROM openaq_countries 
WHERE parameters::text LIKE '%pm25%' 
   OR parameters::text LIKE '%pm10%'
ORDER BY locations DESC;

-- 13. VERIFICACIÓN DE INTEGRIDAD DE DATOS
-- Detectar posibles inconsistencias
SELECT 
    'Códigos duplicados' as tipo_problema,
    COUNT(*) as cantidad
FROM (
    SELECT code, COUNT(*) 
    FROM openaq_countries 
    WHERE code IS NOT NULL 
    GROUP BY code 
    HAVING COUNT(*) > 1
) duplicados
UNION ALL
SELECT 
    'IDs duplicados' as tipo_problema,
    COUNT(*) as cantidad
FROM (
    SELECT country_id, COUNT(*) 
    FROM openaq_countries 
    WHERE country_id IS NOT NULL 
    GROUP BY country_id 
    HAVING COUNT(*) > 1
) duplicados_id
UNION ALL
SELECT 
    'Registros sin código' as tipo_problema,
    COUNT(*) as cantidad
FROM openaq_countries 
WHERE code IS NULL
UNION ALL
SELECT 
    'Registros sin nombre' as tipo_problema,
    COUNT(*) as cantidad
FROM openaq_countries 
WHERE name IS NULL;

-- ================================================================
-- NOTAS DE USO:
-- - Estas consultas están optimizadas para PostgreSQL
-- - Usan funciones JSONB para análisis de parámetros
-- - Incluyen análisis temporal con EXTRACT e INTERVAL
-- - Todas las consultas respetan los índices creados en el DAG
-- ================================================================

-- ================================================================
-- CONSULTAS PARA MEDICIONES DE SENSORES - SANTIAGO DE CHILE
-- Análisis específico de mediciones de calidad del aire
-- ================================================================

-- 1. PROMEDIO DIARIO DE PM2.5 PARA LOS ÚLTIMOS 7 DÍAS
-- Calcula el promedio diario de PM2.5 para cada día disponible
SELECT 
    DATE(period_from_utc) as fecha,
    ROUND(AVG(value)::numeric, 2) as promedio_pm25_ug_m3,
    COUNT(*) as numero_mediciones,
    ROUND(MIN(value)::numeric, 2) as valor_minimo,
    ROUND(MAX(value)::numeric, 2) as valor_maximo,
    ROUND(STDDEV(value)::numeric, 2) as desviacion_estandar
FROM openaq_measurements 
WHERE parameter_name = 'pm25'
    AND period_from_utc >= CURRENT_DATE - INTERVAL '7 days'
    AND value IS NOT NULL
GROUP BY DATE(period_from_utc)
ORDER BY fecha DESC;

-- 2. DÍAS DONDE SE SUPERÓ EL VALOR DE 25 µg/m³ DE PM2.5 (SEGÚN LA OMS)
-- Identifica días con promedio diario superior al límite recomendado por la OMS
WITH promedios_diarios AS (
    SELECT 
        DATE(period_from_utc) as fecha,
        ROUND(AVG(value)::numeric, 2) as promedio_pm25,
        COUNT(*) as mediciones,
        ROUND(MIN(value)::numeric, 2) as min_valor,
        ROUND(MAX(value)::numeric, 2) as max_valor
    FROM openaq_measurements 
    WHERE parameter_name = 'pm25'
        AND value IS NOT NULL
    GROUP BY DATE(period_from_utc)
)
SELECT 
    fecha,
    promedio_pm25 as promedio_pm25_ug_m3,
    (promedio_pm25 - 25) as exceso_sobre_limite_oms,
    ROUND(((promedio_pm25 - 25) / 25 * 100)::numeric, 1) as porcentaje_exceso,
    mediciones,
    min_valor,
    max_valor,
    CASE 
        WHEN promedio_pm25 > 75 THEN 'Muy Alto (>75)'
        WHEN promedio_pm25 > 50 THEN 'Alto (50-75)'
        WHEN promedio_pm25 > 25 THEN 'Moderado (25-50)'
        ELSE 'Aceptable (≤25)'
    END as categoria_calidad
FROM promedios_diarios
WHERE promedio_pm25 > 25
ORDER BY promedio_pm25 DESC;

-- 3. ESTACIÓN CON MAYOR PROMEDIO DE NO2 DURANTE LOS ÚLTIMOS 3 DÍAS
-- Identifica la estación (sensor) con mayor concentración promedio de NO2
WITH no2_ultimos_3_dias AS (
    SELECT 
        sensor_id,
        location_id,
        location_name,
        parameter_name,
        parameter_units,
        COUNT(*) as total_mediciones,
        ROUND(AVG(value)::numeric, 2) as promedio_no2,
        ROUND(MIN(value)::numeric, 2) as valor_minimo,
        ROUND(MAX(value)::numeric, 2) as valor_maximo,
        ROUND(STDDEV(value)::numeric, 2) as desviacion_estandar,
        MIN(period_from_utc) as primera_medicion,
        MAX(period_from_utc) as ultima_medicion
    FROM openaq_measurements 
    WHERE parameter_name = 'no2'
        AND period_from_utc >= CURRENT_DATE - INTERVAL '3 days'
        AND value IS NOT NULL
    GROUP BY sensor_id, location_id, location_name, parameter_name, parameter_units
)
SELECT 
    sensor_id,
    location_name as estacion,
    promedio_no2 as promedio_no2_ug_m3,
    parameter_units as unidades,
    total_mediciones,
    valor_minimo,
    valor_maximo,
    desviacion_estandar,
    primera_medicion,
    ultima_medicion,
    EXTRACT(DAY FROM (ultima_medicion - primera_medicion)) as dias_monitoreados,
    CASE 
        WHEN promedio_no2 > 200 THEN 'Muy Alto (>200)'
        WHEN promedio_no2 > 100 THEN 'Alto (100-200)'
        WHEN promedio_no2 > 40 THEN 'Moderado (40-100)'
        ELSE 'Aceptable (≤40)'
    END as categoria_calidad_oms
FROM no2_ultimos_3_dias
ORDER BY promedio_no2 DESC
LIMIT 1;

-- ================================================================
-- CONSULTAS ADICIONALES DE ANÁLISIS DE CALIDAD DEL AIRE
-- ================================================================

-- 4. RESUMEN SEMANAL DE TODOS LOS PARÁMETROS DE CALIDAD DEL AIRE
-- Vista general de la semana para todos los contaminantes monitoreados
SELECT 
    parameter_name as contaminante,
    parameter_units as unidades,
    COUNT(*) as total_mediciones,
    ROUND(AVG(value)::numeric, 2) as promedio_semanal,
    ROUND(MIN(value)::numeric, 2) as valor_minimo,
    ROUND(MAX(value)::numeric, 2) as valor_maximo,
    ROUND(STDDEV(value)::numeric, 2) as desviacion_estandar,
    COUNT(DISTINCT sensor_id) as sensores_activos,
    MIN(period_from_utc) as periodo_inicio,
    MAX(period_from_utc) as periodo_fin
FROM openaq_measurements 
WHERE period_from_utc >= CURRENT_DATE - INTERVAL '7 days'
    AND value IS NOT NULL
GROUP BY parameter_name, parameter_units
ORDER BY 
    CASE parameter_name 
        WHEN 'pm25' THEN 1
        WHEN 'pm10' THEN 2
        WHEN 'no2' THEN 3
        WHEN 'co' THEN 4
        WHEN 'o3' THEN 5
        WHEN 'so2' THEN 6
        ELSE 7
    END;

-- 5. ANÁLISIS HORARIO DE CONTAMINACIÓN - PATRÓN DIARIO
-- Identifica las horas del día con mayor contaminación promedio
SELECT 
    EXTRACT(HOUR FROM period_from_utc) as hora_utc,
    parameter_name as contaminante,
    COUNT(*) as mediciones,
    ROUND(AVG(value)::numeric, 2) as promedio_horario,
    ROUND(MIN(value)::numeric, 2) as minimo,
    ROUND(MAX(value)::numeric, 2) as maximo
FROM openaq_measurements 
WHERE value IS NOT NULL
    AND parameter_name IN ('pm25', 'pm10', 'no2', 'co', 'o3')
GROUP BY EXTRACT(HOUR FROM period_from_utc), parameter_name
ORDER BY parameter_name, hora_utc;

-- 6. CALIDAD DEL AIRE SEGÚN ESTÁNDARES INTERNACIONALES
-- Evaluación según límites de OMS y EPA
WITH evaluacion_calidad AS (
    SELECT 
        DATE(period_from_utc) as fecha,
        parameter_name,
        ROUND(AVG(value)::numeric, 2) as promedio_diario,
        COUNT(*) as mediciones
    FROM openaq_measurements 
    WHERE value IS NOT NULL
        AND period_from_utc >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(period_from_utc), parameter_name
)
SELECT 
    fecha,
    parameter_name as contaminante,
    promedio_diario,
    mediciones,
    CASE parameter_name
        WHEN 'pm25' THEN 
            CASE 
                WHEN promedio_diario <= 15 THEN 'Buena (OMS: ≤15)'
                WHEN promedio_diario <= 25 THEN 'Moderada (OMS: 15-25)'
                WHEN promedio_diario <= 50 THEN 'Dañina para grupos sensibles (25-50)'
                WHEN promedio_diario <= 75 THEN 'Dañina para la salud (50-75)'
                ELSE 'Muy dañina (>75)'
            END
        WHEN 'pm10' THEN 
            CASE 
                WHEN promedio_diario <= 45 THEN 'Buena (OMS: ≤45)'
                WHEN promedio_diario <= 75 THEN 'Moderada (OMS: 45-75)'
                WHEN promedio_diario <= 150 THEN 'Dañina para grupos sensibles (75-150)'
                ELSE 'Dañina para la salud (>150)'
            END
        WHEN 'no2' THEN 
            CASE 
                WHEN promedio_diario <= 25 THEN 'Buena (OMS: ≤25)'
                WHEN promedio_diario <= 50 THEN 'Moderada (25-50)'
                WHEN promedio_diario <= 100 THEN 'Dañina para grupos sensibles (50-100)'
                ELSE 'Dañina para la salud (>100)'
            END
        WHEN 'o3' THEN 
            CASE 
                WHEN promedio_diario <= 60 THEN 'Buena (OMS: ≤60)'
                WHEN promedio_diario <= 120 THEN 'Moderada (60-120)'
                WHEN promedio_diario <= 180 THEN 'Dañina para grupos sensibles (120-180)'
                ELSE 'Dañina para la salud (>180)'
            END
        ELSE 'No evaluado'
    END as evaluacion_oms
FROM evaluacion_calidad
ORDER BY fecha DESC, parameter_name;

-- ================================================================
-- CONSULTAS DE MONITOREO Y ALERTAS
-- ================================================================

-- 7. DETECCIÓN DE PICOS DE CONTAMINACIÓN
-- Identifica mediciones que exceden significativamente el promedio
WITH estadisticas AS (
    SELECT 
        parameter_name,
        AVG(value) as promedio_general,
        STDDEV(value) as desviacion_general
    FROM openaq_measurements 
    WHERE value IS NOT NULL
    GROUP BY parameter_name
)
SELECT 
    m.sensor_id,
    m.parameter_name as contaminante,
    m.period_from_utc as fecha_hora,
    ROUND(m.value::numeric, 2) as valor_medido,
    ROUND(e.promedio_general::numeric, 2) as promedio_historico,
    ROUND((m.value - e.promedio_general)::numeric, 2) as diferencia,
    ROUND(((m.value - e.promedio_general) / e.desviacion_general)::numeric, 2) as desviaciones_estandar
FROM openaq_measurements m
JOIN estadisticas e ON m.parameter_name = e.parameter_name
WHERE m.value > (e.promedio_general + 2 * e.desviacion_general)
    AND m.period_from_utc >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY desviaciones_estandar DESC
LIMIT 20;

-- 8. DISPONIBILIDAD DE DATOS POR SENSOR
-- Evalúa la completitud de datos de cada sensor
SELECT 
    sensor_id,
    parameter_name as contaminante,
    location_name as estacion,
    COUNT(*) as mediciones_disponibles,
    MIN(period_from_utc) as primera_medicion,
    MAX(period_from_utc) as ultima_medicion,
    EXTRACT(EPOCH FROM (MAX(period_from_utc) - MIN(period_from_utc))) / 3600 as horas_monitoreadas,
    ROUND(COUNT(*) / (EXTRACT(EPOCH FROM (MAX(period_from_utc) - MIN(period_from_utc))) / 3600)::numeric, 2) as mediciones_por_hora,
    CASE 
        WHEN COUNT(*) / (EXTRACT(EPOCH FROM (MAX(period_from_utc) - MIN(period_from_utc))) / 3600) >= 0.8 THEN 'Excelente (≥80%)'
        WHEN COUNT(*) / (EXTRACT(EPOCH FROM (MAX(period_from_utc) - MIN(period_from_utc))) / 3600) >= 0.6 THEN 'Buena (60-80%)'
        WHEN COUNT(*) / (EXTRACT(EPOCH FROM (MAX(period_from_utc) - MIN(period_from_utc))) / 3600) >= 0.4 THEN 'Regular (40-60%)'
        ELSE 'Deficiente (<40%)'
    END as calidad_datos
FROM openaq_measurements 
WHERE value IS NOT NULL
GROUP BY sensor_id, parameter_name, location_name
ORDER BY mediciones_por_hora DESC;

-- ================================================================
-- NOTAS PARA LAS CONSULTAS DE MEDICIONES:
-- - Todas las fechas están en UTC según los datos de la API
-- - Los límites de la OMS son para promedios de 24 horas
-- - Las consultas están optimizadas para el período de datos disponible
-- - Se incluyen categorizaciones según estándares internacionales
-- ================================================================