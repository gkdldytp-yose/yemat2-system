param(
    [string]$Path = ".\logs\access.log",
    [int]$Tail = 80,
    [switch]$NoWait
)

function Write-Part {
    param(
        [string]$Text,
        [ConsoleColor]$Color = [ConsoleColor]::Gray,
        [switch]$NoNewline
    )
    if ($NoNewline) {
        Write-Host $Text -ForegroundColor $Color -NoNewline
    } else {
        Write-Host $Text -ForegroundColor $Color
    }
}

function Get-StatusColor {
    param([string]$Status)
    $code = 0
    [void][int]::TryParse($Status, [ref]$code)
    if ($code -ge 500) { return [ConsoleColor]::Red }
    if ($code -ge 400) { return [ConsoleColor]::Yellow }
    if ($code -ge 300) { return [ConsoleColor]::Cyan }
    if ($code -ge 200) { return [ConsoleColor]::Green }
    return [ConsoleColor]::DarkGray
}

function Get-MethodColor {
    param([string]$Method)
    switch ($Method.ToUpperInvariant()) {
        "GET" { return [ConsoleColor]::Blue }
        "POST" { return [ConsoleColor]::Cyan }
        "PUT" { return [ConsoleColor]::DarkYellow }
        "PATCH" { return [ConsoleColor]::DarkYellow }
        "DELETE" { return [ConsoleColor]::Red }
        default { return [ConsoleColor]::Gray }
    }
}

function Show-AccessLine {
    param([string]$Line)

    $pattern = '^(?<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[ACCESS\] (?<ip>.*?) \| (?<user>.*?) \| (?<workplace>.*?) \| (?<method>.*?) \| (?<status>.*?) \| (?<endpoint>.*?) \| (?<path>.*?) \| (?<elapsed>.*?) \| (?<referer>.*)$'
    $m = [regex]::Match($Line, $pattern)
    if (-not $m.Success) {
        Write-Part $Line DarkGray
        return
    }

    $time = $m.Groups["time"].Value
    $ip = $m.Groups["ip"].Value.Trim()
    $user = $m.Groups["user"].Value.Trim()
    $workplace = $m.Groups["workplace"].Value.Trim()
    $method = $m.Groups["method"].Value.Trim().ToUpperInvariant()
    $status = $m.Groups["status"].Value.Trim()
    $endpoint = $m.Groups["endpoint"].Value.Trim()
    $pathValue = $m.Groups["path"].Value.Trim()
    $elapsed = $m.Groups["elapsed"].Value.Trim()
    $referer = $m.Groups["referer"].Value.Trim()

    $statusColor = Get-StatusColor $status
    $methodColor = Get-MethodColor $method

    Write-Part $time DarkGray -NoNewline
    Write-Part "  " DarkGray -NoNewline
    Write-Part ($method.PadRight(6)) $methodColor -NoNewline
    Write-Part " " DarkGray -NoNewline
    Write-Part ($status.PadRight(4)) $statusColor -NoNewline
    Write-Part " " DarkGray -NoNewline
    Write-Part ($elapsed.PadLeft(6)) DarkGray -NoNewline
    Write-Part "  " DarkGray -NoNewline
    Write-Part $pathValue White -NoNewline
    Write-Part "  ->  " DarkGray -NoNewline
    Write-Part $endpoint Cyan -NoNewline
    Write-Part "  |  " DarkGray -NoNewline
    Write-Part "user=" DarkGray -NoNewline
    Write-Part $user Green -NoNewline
    Write-Part "  site=" DarkGray -NoNewline
    Write-Part $workplace Blue -NoNewline
    Write-Part "  ip=" DarkGray -NoNewline
    Write-Part $ip DarkGray

    if ($referer -and $referer -ne "-") {
        Write-Part (" " * 34) DarkGray -NoNewline
        Write-Part "ref: " DarkGray -NoNewline
        Write-Part $referer DarkGray
    }
}

if (-not (Test-Path -LiteralPath $Path)) {
    Write-Part "Log file not found: $Path" Red
    exit 1
}

$host.UI.RawUI.WindowTitle = "Yemat2 Access Log Viewer"
Clear-Host
Write-Part "Yemat2 Access Log Viewer" Cyan
Write-Part "GET/POST and status codes are colorized. 2xx=green, 4xx=yellow, 5xx=red." DarkGray
Write-Part "Source: $Path" DarkGray
Write-Part ("-" * 110) DarkGray

$wait = -not $NoWait
Get-Content -LiteralPath $Path -Tail $Tail -Wait:$wait | ForEach-Object {
    Show-AccessLine $_
}
