@echo off
REM ==========================================================
REM HOTTRACK SAAS - DEPLOY COM DOCKER (Windows)
REM ==========================================================

setlocal enabledelayedexpansion

echo 🐳 Iniciando deploy do HotTrack com Docker...

REM Verificar se Docker está instalado
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker não encontrado. Instale o Docker Desktop primeiro.
    pause
    exit /b 1
)

echo [SUCCESS] Docker detectado

REM Verificar se estamos no diretório correto
if not exist "Dockerfile" (
    echo [ERROR] Execute este script na raiz do projeto HotTrack
    pause
    exit /b 1
)

echo [INFO] Construindo imagens Docker...

REM Build das imagens
echo [INFO] Construindo backend...
docker build -t hottrack-backend .

if %errorlevel% neq 0 (
    echo [ERROR] Falha ao construir backend
    pause
    exit /b 1
)

echo [INFO] Construindo frontend...
docker build -t hottrack-frontend ./frontend

if %errorlevel% neq 0 (
    echo [ERROR] Falha ao construir frontend
    pause
    exit /b 1
)

echo [INFO] Construindo admin frontend...
docker build -t hottrack-admin ./admin-frontend

if %errorlevel% neq 0 (
    echo [ERROR] Falha ao construir admin frontend
    pause
    exit /b 1
)

echo [SUCCESS] Imagens Docker construídas com sucesso!

REM Verificar se foi solicitado teste
if "%1"=="--test" (
    echo [INFO] Iniciando teste local...
    
    REM Parar containers existentes
    docker-compose down 2>nul
    
    REM Iniciar com docker-compose
    docker-compose up --build -d
    
    if %errorlevel% neq 0 (
        echo [ERROR] Falha ao iniciar containers
        pause
        exit /b 1
    )
    
    echo [SUCCESS] Teste local iniciado!
    echo [INFO] Acesse:
    echo   - Backend: http://localhost:3000
    echo   - Frontend: http://localhost:3001
    echo   - Admin: http://localhost:3002
    echo.
    echo [INFO] Para parar: docker-compose down
)

echo [SUCCESS] ✅ Projeto pronto para deploy no Render!
echo.
echo [INFO] Próximos passos:
echo 1. Faça push do código para o GitHub:
echo    git add .
echo    git commit -m "Deploy Docker para Render"
echo    git push origin main
echo.
echo 2. No Render Dashboard:
echo    - Clique em 'New +'
echo    - Selecione 'Blueprint'
echo    - Conecte seu repositório
echo    - O Render detectará automaticamente os Dockerfiles
echo.
echo [WARNING] ⚠️  Configure as variáveis de ambiente no Render!
echo [WARNING]    Consulte o arquivo DEPLOY_RENDER.md
echo.
echo [INFO] 📚 Documentação: DEPLOY_RENDER.md
echo.
echo [SUCCESS] 🎉 Deploy Docker preparado!

pause
