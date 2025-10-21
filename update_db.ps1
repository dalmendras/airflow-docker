# Script PowerShell para actualizar la base de datos SQLite local
# Ejecuta este script cada vez que quieras ver los datos más recientes

Write-Host "Copiando base de datos SQLite actualizada desde el contenedor..." -ForegroundColor Green
docker cp airflow-docker-airflow-webserver-1:/opt/airflow/airflow.db ./airflow.db

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Base de datos actualizada exitosamente en: $PWD\airflow.db" -ForegroundColor Green
    Write-Host "💡 Puedes refrescar DBeaver (F5) para ver los datos más recientes" -ForegroundColor Yellow
} else {
    Write-Host "❌ Error al copiar la base de datos" -ForegroundColor Red
}

# Mostrar información del archivo
if (Test-Path "./airflow.db") {
    $fileInfo = Get-Item "./airflow.db"
    Write-Host "📁 Tamaño del archivo: $([math]::Round($fileInfo.Length / 1KB, 2)) KB" -ForegroundColor Cyan
    Write-Host "🕒 Última modificación: $($fileInfo.LastWriteTime)" -ForegroundColor Cyan
}