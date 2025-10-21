-- CONSULTAS SQL PARA ESTACIONES DE CHILE - TABLA STATION
-- Base de datos: airflow_countries_stations.db
-- Tablas: openaq_countries (140 países) + station (611 registros de Chile)

-- ==========================================
-- 1. VISTA GENERAL DE ESTACIONES
-- ==========================================

-- Contar total de registros de estaciones
SELECT COUNT(*) as total_station_records FROM station;

-- Contar estaciones únicas
SELECT COUNT(DISTINCT id) as unique_stations FROM station;

-- Ver estructura de la tabla station
.schema station

-- ==========================================
-- 2. EXPLORACIÓN DE ESTACIONES DE CHILE
-- ==========================================

-- Todas las estaciones de Chile con sus sensores
SELECT id, name, locality, sensor_name, provider_name 
FROM station 
ORDER BY name, sensor_name;

-- Estaciones únicas (sin duplicar por sensor)
SELECT DISTINCT id, name, locality, provider_name, latitude, longitude
FROM station 
ORDER BY name;

-- Estadísticas por proveedor
SELECT provider_name, 
       COUNT(*) as total_records,
       COUNT(DISTINCT id) as unique_stations
FROM station 
GROUP BY provider_name
ORDER BY total_records DESC;

-- ==========================================
-- 3. ANÁLISIS DE SENSORES
-- ==========================================

-- Tipos de sensores disponibles
SELECT sensor_name, COUNT(*) as count
FROM station 
WHERE sensor_name IS NOT NULL
GROUP BY sensor_name 
ORDER BY count DESC;

-- Estaciones con más sensores
SELECT id, name, locality, COUNT(*) as sensor_count
FROM station 
GROUP BY id, name, locality
ORDER BY sensor_count DESC
LIMIT 15;

-- Buscar estaciones por tipo de sensor específico
SELECT DISTINCT name, locality, provider_name
FROM station 
WHERE sensor_name LIKE '%pm25%'
ORDER BY name;

-- ==========================================
-- 4. ANÁLISIS GEOGRÁFICO
-- ==========================================

-- Estaciones por localidad/ciudad
SELECT locality, COUNT(DISTINCT id) as stations
FROM station 
WHERE locality IS NOT NULL
GROUP BY locality 
ORDER BY stations DESC;

-- Estaciones en Santiago
SELECT name, sensor_name, latitude, longitude
FROM station 
WHERE locality LIKE '%Santiago%'
ORDER BY name, sensor_name;

-- Rango de coordenadas (cobertura geográfica)
SELECT 
    MIN(latitude) as min_lat,
    MAX(latitude) as max_lat,
    MIN(longitude) as min_lng,
    MAX(longitude) as max_lng,
    COUNT(DISTINCT id) as stations_with_coords
FROM station 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- ==========================================
-- 5. RELACIÓN ENTRE PAÍSES Y ESTACIONES
-- ==========================================

-- Verificar relación con tabla de países
SELECT c.name as country_name, c.code,
       COUNT(s.id) as station_records,
       COUNT(DISTINCT s.id) as unique_stations
FROM openaq_countries c
LEFT JOIN station s ON c.country_id = s.country_id
GROUP BY c.name, c.code
HAVING station_records > 0
ORDER BY station_records DESC;

-- Detalle de estaciones chilenas con información del país
SELECT c.name as country, c.code, 
       s.name as station_name, s.locality, s.sensor_name
FROM openaq_countries c
JOIN station s ON c.country_id = s.country_id
WHERE c.code = 'CL'
ORDER BY s.name, s.sensor_name
LIMIT 20;

-- ==========================================
-- 6. VALIDACIÓN DE LLAVE COMPUESTA
-- ==========================================

-- Verificar unicidad de llave compuesta (id, sensor_id)
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT (id || '-' || sensor_id)) as unique_combinations
FROM station;

-- Buscar posibles duplicados (si los hubiera)
SELECT id, sensor_id, COUNT(*) as duplicates
FROM station 
GROUP BY id, sensor_id 
HAVING COUNT(*) > 1;

-- Estaciones con sensores duplicados (mismo tipo)
SELECT id, name, sensor_name, COUNT(*) as sensor_count
FROM station 
GROUP BY id, name, sensor_name
HAVING COUNT(*) > 1;

-- ==========================================
-- 7. BÚSQUEDAS ESPECÍFICAS POR CIUDAD
-- ==========================================

-- Estaciones en ciudades principales
SELECT locality, name, sensor_name, provider_name
FROM station 
WHERE locality IN ('Santiago', 'Valparaíso', 'Concepción', 'La Serena', 'Antofagasta')
ORDER BY locality, name;

-- Estaciones en la Región Metropolitana
SELECT name, locality, sensor_name, latitude, longitude
FROM station 
WHERE locality LIKE '%Santiago%' 
   OR locality LIKE '%Las Condes%'
   OR locality LIKE '%Providencia%'
   OR locality LIKE '%Maipú%'
ORDER BY locality, name;

-- ==========================================
-- 8. ANÁLISIS DE CALIDAD DEL AIRE
-- ==========================================

-- Estaciones que monitorean PM2.5 y PM10 (partículas)
SELECT DISTINCT name, locality, provider_name
FROM station 
WHERE sensor_name IN ('pm25 µg/m³', 'pm10 µg/m³')
ORDER BY locality, name;

-- Estaciones con monitoreo completo (múltiples contaminantes)
SELECT id, name, locality, 
       GROUP_CONCAT(DISTINCT sensor_name) as all_sensors,
       COUNT(DISTINCT sensor_name) as sensor_types
FROM station 
GROUP BY id, name, locality
HAVING sensor_types >= 3
ORDER BY sensor_types DESC, name;

-- Estaciones que monitorean gases (NO2, SO2, O3)
SELECT DISTINCT name, locality, sensor_name
FROM station 
WHERE sensor_name IN ('no2 µg/m³', 'so2 µg/m³', 'o3 µg/m³')
ORDER BY locality, name, sensor_name;

-- ==========================================
-- 9. REPORTES ESTADÍSTICOS
-- ==========================================

-- Resumen general
SELECT 
    'Total registros' as metric,
    COUNT(*) as value
FROM station
UNION ALL
SELECT 
    'Estaciones únicas' as metric,
    COUNT(DISTINCT id) as value
FROM station
UNION ALL
SELECT 
    'Tipos de sensores' as metric,
    COUNT(DISTINCT sensor_name) as value
FROM station
UNION ALL
SELECT 
    'Localidades' as metric,
    COUNT(DISTINCT locality) as value
FROM station WHERE locality IS NOT NULL
UNION ALL
SELECT 
    'Proveedores' as metric,
    COUNT(DISTINCT provider_name) as value
FROM station WHERE provider_name IS NOT NULL;

-- Distribución de sensores por tipo
SELECT 
    CASE 
        WHEN sensor_name LIKE '%pm%' THEN 'Partículas (PM)'
        WHEN sensor_name LIKE '%no%' OR sensor_name LIKE '%so%' OR sensor_name LIKE '%o3%' THEN 'Gases'
        WHEN sensor_name LIKE '%temp%' OR sensor_name LIKE '%hum%' THEN 'Meteorológicos'
        ELSE 'Otros'
    END as categoria_sensor,
    COUNT(*) as cantidad
FROM station 
WHERE sensor_name IS NOT NULL
GROUP BY categoria_sensor
ORDER BY cantidad DESC;

-- ==========================================
-- 10. CONSULTAS PARA DBEAVER
-- ==========================================

-- Top 10 estaciones con más sensores
SELECT name as "Estación", 
       locality as "Localidad",
       provider_name as "Proveedor",
       COUNT(*) as "Cantidad Sensores"
FROM station 
GROUP BY name, locality, provider_name
ORDER BY "Cantidad Sensores" DESC
LIMIT 10;

-- Sensores por ciudad
SELECT locality as "Ciudad", 
       COUNT(DISTINCT id) as "Estaciones",
       COUNT(*) as "Total Sensores"
FROM station 
WHERE locality IS NOT NULL
GROUP BY locality 
ORDER BY "Total Sensores" DESC;

-- ==========================================
-- NOTAS IMPORTANTES
-- ==========================================

/*
ESTRUCTURA DE DATOS:
- openaq_countries: 140 países con información general
- station: 611 registros (183 estaciones × ~3.3 sensores promedio)
- Llave compuesta: (id, sensor_id) para evitar duplicados
- Cada registro = una combinación estación-sensor única

PROVEEDORES EN CHILE:
- Chile - SINCA: 564 registros (174 estaciones) - Red oficial
- AirGradient: 47 registros (9 estaciones) - Sensores ciudadanos

TIPOS DE SENSORES PRINCIPALES:
- pm25 µg/m³: Material particulado fino (137 registros)
- pm10 µg/m³: Material particulado grueso (124 registros)  
- so2 µg/m³: Dióxido de azufre (109 registros)
- no2 µg/m³: Dióxido de nitrógeno (70 registros)
- o3 µg/m³: Ozono (70 registros)

COBERTURA GEOGRÁFICA:
- Concentración en Santiago y regiones metropolitanas
- Estaciones desde Arica hasta Punta Arenas
- Coordenadas disponibles para geolocalización
*/