#!/bin/sh
set -e

echo "running migrations..."
/app/migrate -path /app/db/migrations -database "$DATABASE_URL" up

echo "starting app..."
exec /app/workerpool
