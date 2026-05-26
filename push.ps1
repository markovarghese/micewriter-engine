<#
.SYNOPSIS
  Build and push the micewriter-engine Docker image to the local k3s registry.
.EXAMPLE
  .\push.ps1
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$registry = "k8s-node-1.local:5000"
$image    = "micewriter-engine"
$tag      = "latest"
$fullTag  = "${registry}/${image}:${tag}"

docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Docker is not running."; exit 1 }

Write-Host "Building $image..."
docker build -t $fullTag .

Write-Host "Pushing $fullTag..."
docker push $fullTag

Write-Host "Done. Image available at $fullTag"
