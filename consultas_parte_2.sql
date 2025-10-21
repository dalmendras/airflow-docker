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
    AND period_from_utc >= CURRENT_DATE - INTERVAL '3 days'
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
        om.sensor_id,
        om.location_id,
        ol."name" as location_name,
        om.parameter_name,
        om.parameter_units,
        COUNT(*) as total_mediciones,
        ROUND(AVG(om.value)::numeric, 2) as promedio_no2,
        ROUND(MIN(om.value)::numeric, 2) as valor_minimo,
        ROUND(MAX(om.value)::numeric, 2) as valor_maximo,
        ROUND(STDDEV(om.value)::numeric, 2) as desviacion_estandar,
        MIN(om.period_from_utc) as primera_medicion,
        MAX(om.period_from_utc) as ultima_medicion
    FROM openaq_measurements om
    INNER JOIN openaq_locations ol ON ol.location_id = om.location_id 
    WHERE om.parameter_name = 'no2'
        AND om.period_from_utc >= CURRENT_DATE - INTERVAL '3 days'
        AND om.value IS NOT NULL
    GROUP BY om.sensor_id, om.location_id, ol."name", om.parameter_name, om.parameter_units
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
LIMIT 10;