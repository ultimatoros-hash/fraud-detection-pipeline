@echo off
setlocal enabledelayedexpansion

echo.
echo ══════════════════════════════════════════════════════════════
echo           FRAUD DETECTION PIPELINE - DEMO LAUNCHER
echo                    Lambda Architecture
echo ══════════════════════════════════════════════════════════════
echo.

REM Step 1: Check Docker
echo [1] Checking Docker services...
docker ps --filter "name=fraud-kafka" --format "{{.Names}}" | findstr /C:"fraud-kafka" >nul
if %errorlevel%==0 (echo     √ fraud-kafka running) else (echo     X fraud-kafka NOT running & goto :docker_error)

docker ps --filter "name=fraud-mongodb" --format "{{.Names}}" | findstr /C:"fraud-mongodb" >nul
if %errorlevel%==0 (echo     √ fraud-mongodb running) else (echo     X fraud-mongodb NOT running & goto :docker_error)

docker ps --filter "name=fraud-zookeeper" --format "{{.Names}}" | findstr /C:"fraud-zookeeper" >nul
if %errorlevel%==0 (echo     √ fraud-zookeeper running) else (echo     X fraud-zookeeper NOT running & goto :docker_error)

REM Step 2: Clear MongoDB
echo.
echo [2] Clearing MongoDB for fresh demo...
docker exec fraud-mongodb mongosh fraud_detection --quiet --eval "db.realtime_views.drop(); db.fraud_alerts.drop(); db.batch_views.drop(); print('Collections cleared')"
echo     √ MongoDB cleared

echo.
echo ══════════════════════════════════════════════════════════════
echo   READY TO LAUNCH!
echo ══════════════════════════════════════════════════════════════
echo.
echo Open 3 separate terminals and run:
echo.
echo   Terminal 1 - Producer:
echo     python src/producer/transaction_producer.py
echo.
echo   Terminal 2 - Speed Layer:
echo     python src/speed/simple_processor.py
echo.
echo   Terminal 3 - Dashboard:
echo     python src/dashboard/pro_dashboard.py
echo.
echo Then open: http://localhost:8050
echo.
echo Other URLs:
echo   • Kafka UI:      http://localhost:8082
echo   • Mongo Express: http://localhost:8081
echo   • Grafana:       http://localhost:3000
echo.
echo ══════════════════════════════════════════════════════════════
goto :end

:docker_error
echo.
echo ERROR: Docker containers not running!
echo Run: docker-compose up -d
goto :end

:end
pause
