# scripts/setup.ps1
# Check if Ollama is installed
if (-not (Get-Command ollama -ErrorAction SilentlyContinue)) {
    Write-Host "Install Ollama from https://ollama.com/download"
    exit 1
}
ollama pull mistral
ollama pull nomic-embed-text