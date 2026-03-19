import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import JSONResponse

from app.config import settings as app_settings
from app.database import Base, SessionLocal, engine
from app.services.seed_service import seed_inkoop_categories, seed_user_organizations, seed_platform_eigenaar

# Import all models so Base.metadata knows about every table
import app.models  # noqa: F401

from app.api import auth, organizations, categories, suppliers, imports, spend, categorization, risk, contracts, calendar, export, dashboard, settings, invitations, members, team, supplier_master

logger = logging.getLogger(__name__)

# Rate limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["120/minute"])


async def seed_initial_data() -> None:
    db = SessionLocal()
    try:
        seed_inkoop_categories(db)
        seed_user_organizations(db)
        seed_platform_eigenaar(db)
    finally:
        db.close()


def _sqlite_add_missing_columns() -> None:
    """Add columns to existing SQLite tables (create_all won't do this)."""
    from sqlalchemy import text
    conn = engine.connect()
    columns_to_add = [
        ("import_sessions", "progress_current", "INTEGER DEFAULT 0"),
        ("import_sessions", "progress_total", "INTEGER DEFAULT 0"),
    ]
    for table, col, col_type in columns_to_add:
        try:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"))
            logger.info("SQLite: added column %s.%s", table, col)
        except Exception:
            pass
    conn.commit()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Creating database tables (create_all) ...")
    Base.metadata.create_all(bind=engine)
    if app_settings.DATABASE_URL.startswith("sqlite"):
        _sqlite_add_missing_columns()
    logger.info("Running seed_initial_data ...")
    await seed_initial_data()
    yield


app = FastAPI(
    title="WoCo Inkoopplatform API",
    lifespan=lifespan,
)

# Rate limiting
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Te veel verzoeken. Probeer het later opnieuw."},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Interne serverfout: {type(exc).__name__}"},
    )


# CORS
_cors_origins = app_settings.CORS_ORIGINS
if _cors_origins == "*":
    _allow_origins = ["*"]
    _allow_credentials = False
else:
    _allow_origins = [o.strip() for o in _cors_origins.split(",")]
    _allow_credentials = True

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Security headers
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = (
        "camera=(), microphone=(), geolocation=(), payment=()"
    )
    return response


# Routers
app.include_router(auth.router, prefix="/api")
app.include_router(organizations.router, prefix="/api")
app.include_router(categories.router, prefix="/api")
app.include_router(suppliers.router, prefix="/api")
app.include_router(imports.router, prefix="/api")
app.include_router(spend.router, prefix="/api")
app.include_router(categorization.router, prefix="/api")
app.include_router(risk.router, prefix="/api")
app.include_router(contracts.router, prefix="/api")
app.include_router(calendar.router, prefix="/api")
app.include_router(export.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(settings.router, prefix="/api")
app.include_router(invitations.router, prefix="/api")
app.include_router(members.router, prefix="/api")
app.include_router(team.router, prefix="/api")
app.include_router(supplier_master.router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "WoCo Inkoopplatform API is running"}
