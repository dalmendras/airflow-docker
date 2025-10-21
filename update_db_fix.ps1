# Script para actualizar la base de datos local desde el contenedor Docker
Write-Host "Actualizando base de datos local..." -ForegroundColor Green

# Verificar si el archivo existe
if (Test-Path "./airflow.db") {
    $fileInfo = Get-Item "./airflow.db"
    Write-Host "Ultima modificacion: $($fileInfo.LastWriteTime)" -ForegroundColor Yellow
}

try {
    # Copiar base de datos desde el contenedor
    docker cp airflow-docker-airflow-webserver-1:/opt/airflow/airflow.db ./airflow_new.db
    
    # Verificar que se copio correctamente
    if (Test-Path "./airflow_new.db") {
        # Reemplazar el archivo anterior
        if (Test-Path "./airflow.db") {
            Remove-Item "./airflow.db" -Force
        }
        Rename-Item "./airflow_new.db" "./airflow.db"
        
        $newFileInfo = Get-Item "./airflow.db"
        Write-Host "Base de datos actualizada exitosamente!" -ForegroundColor Green
        Write-Host "Nueva modificacion: $($newFileInfo.LastWriteTime)" -ForegroundColor Green
        Write-Host "Tamano: $([math]::round($newFileInfo.Length/1KB, 2)) KB" -ForegroundColor Green
    } else {
        Write-Host "Error: No se pudo copiar la base de datos" -ForegroundColor Red
    }
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "Proceso completado. Puedes refrescar DBeaver ahora." -ForegroundColor Cyan