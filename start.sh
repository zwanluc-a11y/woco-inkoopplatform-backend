#!/bin/bash
echo "Starting WoCo Inkoopplatform Backend..."

# Skip alembic - tables are created via SQLAlchemy create_all in main.py lifespan
# alembic upgrade head

# Start the application
echo "Starting uvicorn server on port ${PORT:-8000}..."
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port ${PORT:-8000} \
    --workers ${WORKERS:-1} \
    --log-level ${LOG_LEVEL:-info}
