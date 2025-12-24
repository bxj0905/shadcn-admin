@echo off
setlocal ENABLEDELAYEDEXPANSION

:: === 配置部分 ===
set VAULT_ADDR=http://127.0.0.1:8200
set INIT_FILE=init.txt
set UNSEAL_THRESHOLD=2

:: === 检查 Vault 是否初始化 ===
echo 检查 Vault 是否已初始化...
vault status > nul 2>&1
if ERRORLEVEL 1 (
    echo ❌ 无法连接到 Vault，请检查 Vault 是否已启动。
    pause
    exit /b 1
)

vault status | findstr /C:"Initialized" | findstr "true" > nul
if %ERRORLEVEL%==0 (
    echo ✅ Vault 已初始化。
) else (
    echo ⚠️  Vault 未初始化，开始执行初始化...

    vault operator init -key-shares=3 -key-threshold=%UNSEAL_THRESHOLD% > %INIT_FILE%
    if %ERRORLEVEL%==0 (
        echo ✅ 初始化成功，结果已保存到 %INIT_FILE%
    ) else (
        echo ❌ 初始化失败。
        pause
        exit /b 1
    )
)

:: === 执行解封 ===
echo 🛡 解封 Vault...
for /f "tokens=4" %%A in ('findstr "Unseal Key" %INIT_FILE%') do (
    if !COUNT! LSS %UNSEAL_THRESHOLD% (
        set /a COUNT+=1
        vault operator unseal %%A > nul
    )
)

:: === 登录 Vault ===
for /f "tokens=4" %%B in ('findstr "Initial Root Token" %INIT_FILE%') do (
    set ROOT_TOKEN=%%B
)

echo 🔐 使用 Root Token 登录 Vault...
vault login !ROOT_TOKEN! > nul
if ERRORLEVEL 1 (
    echo ❌ 登录失败，请检查 Root Token。
) else (
    echo ✅ 登录成功，Vault 准备就绪。
)

pause
