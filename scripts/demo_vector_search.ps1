<#
.SYNOPSIS
  Prepara o limpia el endpoint de Vector Search para una demo (RAG y agente).

.DESCRIPTION
  El endpoint de Vector Search se cobra por hora mientras exista, aunque no se use, así que se
  crea solo para la demo y se borra al terminar (D-22).

    preparar  despliega el bundle en dev, recrea endpoint e índice con el job andina_rag (~15 min)
              y, con -ConAgente, vuelve a correr los escenarios del agente.
    limpiar   borra el índice y el endpoint, con reintentos, y confirma que no quede ninguno.
    estado    muestra los endpoints de Vector Search que existen ahora.

.EXAMPLE
  .\scripts\demo_vector_search.ps1 preparar -ConAgente
  .\scripts\demo_vector_search.ps1 limpiar
#>
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("preparar", "limpiar", "estado")]
    [string]$Accion,
    [string]$Target = "dev",
    [switch]$ConAgente
)

$ErrorActionPreference = "Stop"
$Catalog = "andina_$Target"
$Endpoint = "vs-andina-$Target"
$Index = "$Catalog.genai.doc_chunks_index"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Get-Endpoints {
    # La red del candidato a veces corta la conexión: se reintenta antes de dar por fallida la consulta.
    for ($i = 1; $i -le 5; $i++) {
        $json = databricks vector-search-endpoints list-endpoints -o json 2>$null
        if ($LASTEXITCODE -eq 0) { return @($json | ConvertFrom-Json) }
        Start-Sleep -Seconds 20
    }
    throw "No se pudo consultar Databricks después de 5 intentos"
}

switch ($Accion) {
    "preparar" {
        databricks bundle deploy -t $Target
        Write-Host "Recreando endpoint e índice (unos 15 minutos)..."
        databricks bundle run andina_rag -t $Target
        if ($ConAgente) { databricks bundle run andina_agent -t $Target }
        Get-Endpoints | Where-Object { $_.name -eq $Endpoint } | Select-Object name, @{n = "estado"; e = { $_.endpoint_status.state } }
        Write-Host "Listo. Al terminar la demo: .\scripts\demo_vector_search.ps1 limpiar" -ForegroundColor Yellow
    }
    "limpiar" {
        for ($i = 1; $i -le 10; $i++) {
            databricks vector-search-indexes delete-index $Index 2>$null | Out-Null
            databricks vector-search-endpoints delete-endpoint $Endpoint 2>$null | Out-Null
            try {
                if (-not (Get-Endpoints | Where-Object { $_.name -eq $Endpoint })) {
                    Write-Host "Endpoint $Endpoint borrado: no queda costo por hora." -ForegroundColor Green
                    return
                }
            } catch { }
            Start-Sleep -Seconds 30
        }
        throw "El endpoint $Endpoint sigue existiendo: revisar en Compute > Vector Search"
    }
    "estado" {
        $eps = Get-Endpoints
        if ($eps.Count -eq 0) { Write-Host "No hay endpoints de Vector Search (sin costo por hora)." }
        else { $eps | Select-Object name, @{n = "estado"; e = { $_.endpoint_status.state } } }
    }
}
