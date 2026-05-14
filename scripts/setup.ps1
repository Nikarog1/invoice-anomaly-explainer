# scripts/setup.ps1
# Check if Ollama is installed
if (-not (Get-Command ollama -ErrorAction SilentlyContinue)) {
    Write-Host "Install Ollama from https://ollama.com/download"
    exit 1
}
ollama pull llama3.1:8b
ollama pull nomic-embed-text