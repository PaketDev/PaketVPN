param(
  [string]$Message = "",
  [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Message)) {
  $Message = "chore: deploy $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
}

git rev-parse --is-inside-work-tree *> $null
if ($LASTEXITCODE -ne 0) {
  throw "Current directory is not a git repository."
}

git remote get-url origin *> $null
if ($LASTEXITCODE -ne 0) {
  throw "Git remote 'origin' is not configured."
}

git add -A
git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
  Write-Host "No changes to commit."
  exit 0
}

git commit -m "$Message"
git push origin "$Branch"

