import logging

from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.responses import JSONResponse

from app.config import settings as app_settings
from app.database import Base, SessionLocal, engine
from app.services.seed_service import seed_inkoop_categories, seed_user_organizations, seed_platform_eigenaar, seed_woningcorporaties, seed_leveranciers

# Import all models so Base.metadata knows about every table
import app.models  # noqa: F401

from app.api import auth, organizations, categories, suppliers, imports, spend, categorization, risk, contracts, calendar, export, dashboard, settings, invitations, members, team, supplier_master, corporaties, referentie
from app.api.deps import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

# Rate limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["120/minute"])


async def seed_initial_data() -> None:
    db = SessionLocal()
    try:
        for seed_fn in [seed_woningcorporaties, seed_inkoop_categories, seed_leveranciers, seed_user_organizations, seed_platform_eigenaar]:
            try:
                seed_fn(db)
            except Exception as e:
                import traceback
                logger.error("Seed function %s failed: %s\n%s", seed_fn.__name__, e, traceback.format_exc())
                db.rollback()
    finally:
        db.close()


def _add_missing_columns() -> None:
    """Add columns to existing tables (create_all won't do this for existing tables)."""
    from sqlalchemy import text, inspect as sa_inspect
    inspector = sa_inspect(engine)
    columns_to_add = [
        ("import_sessions", "progress_current", "INTEGER DEFAULT 0"),
        ("import_sessions", "progress_total", "INTEGER DEFAULT 0"),
        ("inkoop_categories", "classificatie", "VARCHAR(50)"),
        ("organizations", "corporatie_l_nummer", "VARCHAR(20)"),
        ("organizations", "aantal_vhe", "INTEGER"),
        ("contracts", "contract_vorm", "VARCHAR(100)"),
    ]
    conn = engine.connect()
    for table, col, col_type in columns_to_add:
        try:
            existing = {c["name"] for c in inspector.get_columns(table)}
            if col not in existing:
                logger.info("Adding missing column %s.%s (%s)...", table, col, col_type)
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"))
                conn.commit()
                logger.info("Successfully added column %s.%s", table, col)
            else:
                logger.debug("Column %s.%s already exists", table, col)
        except Exception as e:
            logger.error("Failed to add column %s.%s: %s", table, col, e)
            conn.rollback()

    # Double-check: try adding aantal_vhe via IF NOT EXISTS (PostgreSQL 9.6+)
    try:
        conn.execute(text(
            "ALTER TABLE organizations ADD COLUMN IF NOT EXISTS aantal_vhe INTEGER"
        ))
        conn.commit()
        logger.info("Ensured organizations.aantal_vhe column exists (IF NOT EXISTS)")
    except Exception as e:
        logger.error("IF NOT EXISTS fallback failed: %s", e)
        conn.rollback()

    # Make category_id nullable in supplier_master_categories (was NOT NULL)
    try:
        conn.execute(text("ALTER TABLE supplier_master_categories ALTER COLUMN category_id DROP NOT NULL"))
        conn.commit()
        logger.info("Made supplier_master_categories.category_id nullable")
    except Exception:
        conn.rollback()

    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Creating database tables (create_all) ...")
    Base.metadata.create_all(bind=engine)
    _add_missing_columns()
    logger.info("Running seed_initial_data ...")
    await seed_initial_data()
    yield


app = FastAPI(
    title="WoCo Inkoopplatform API",
    lifespan=lifespan,
    redirect_slashes=False,
)


# Strip trailing slashes from incoming requests so both /team and /team/ work.
# This avoids FastAPI's built-in redirect_slashes which generates http:// URLs
# behind Railway's HTTPS proxy.
@app.middleware("http")
async def strip_trailing_slash(request: Request, call_next):
    path = request.scope.get("path", "")
    if path != "/" and path.endswith("/"):
        request.scope["path"] = path.rstrip("/")
    return await call_next(request)

# Rate limiting
app.state.limiter = limiter


def _cors_headers(request: Request) -> dict[str, str]:
    """Build CORS headers to include on error responses.

    When an exception is raised, CORSMiddleware may not always
    attach the Access-Control-Allow-* headers.  Including them
    manually ensures browsers can still read the error body
    instead of showing an opaque "Failed to fetch".
    """
    origin = request.headers.get("origin", "")
    if origin and origin in _allow_origins:
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
        }
    return {}


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Te veel verzoeken. Probeer het later opnieuw."},
        headers=_cors_headers(request),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Er is een interne serverfout opgetreden. Probeer het later opnieuw."},
        headers=_cors_headers(request),
    )


# CORS - always include Vercel frontend
_cors_origins = app_settings.CORS_ORIGINS
_allow_origins = [o.strip() for o in _cors_origins.split(",") if o.strip()]
# Always include the Vercel frontend
if "https://woco-inkoopplatform.vercel.app" not in _allow_origins:
    _allow_origins.append("https://woco-inkoopplatform.vercel.app")
if "http://localhost:3000" not in _allow_origins:
    _allow_origins.append("http://localhost:3000")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
)


# Fix HTTPS redirects behind reverse proxy (Railway)
# FastAPI's trailing-slash redirects generate http:// URLs because the
# app doesn't know it's behind an HTTPS proxy.  This middleware rewrites
# Location headers to https:// when X-Forwarded-Proto indicates HTTPS.
@app.middleware("http")
async def fix_https_redirects(request: Request, call_next):
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        logger.exception("Middleware caught unhandled error on %s %s", request.method, request.url.path)
        response = JSONResponse(
            status_code=500,
            content={"detail": "Er is een interne serverfout opgetreden."},
        )
        # Ensure CORS headers so the browser can read the error
        origin = request.headers.get("origin", "")
        if origin and origin in _allow_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
        return response
    if (
        response.status_code in (301, 302, 307, 308)
        and "location" in response.headers
        and request.headers.get("x-forwarded-proto") == "https"
    ):
        loc = response.headers["location"]
        if loc.startswith("http://"):
            response.headers["location"] = "https://" + loc[7:]
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = (
        "camera=(), microphone=(), geolocation=(), payment=()"
    )
    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains; preload"
    )
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; frame-ancestors 'none'"
    )
    # Remove headers that leak technology info
    for h in ("server", "x-powered-by", "X-Powered-By"):
        if h in response.headers:
            del response.headers[h]
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
app.include_router(corporaties.router, prefix="/api")
app.include_router(referentie.router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "WoCo Inkoopplatform API is running"}


@app.get("/debug/seed-status")
def seed_status(
    current_user: User = Depends(get_current_user),
):
    """Seed status (platform eigenaar only)."""
    if current_user.platform_role != "eigenaar":
        raise HTTPException(status_code=403, detail="Alleen platform eigenaar")
    from sqlalchemy import text
    db = SessionLocal()
    try:
        counts = {}
        for table in ["woningcorporaties", "inkoop_categories", "supplier_master_categories", "users", "organizations"]:
            try:
                row = db.execute(text(f"SELECT count(*) FROM {table}")).scalar()
                counts[table] = row
            except Exception as e:
                counts[table] = "error"
        return counts
    finally:
        db.close()


@app.post("/debug/reseed")
def reseed(
    current_user: User = Depends(get_current_user),
):
    """Reseed data (platform eigenaar only)."""
    if current_user.platform_role != "eigenaar":
        raise HTTPException(status_code=403, detail="Alleen platform eigenaar")
    db = SessionLocal()
    results = {}
    try:
        for seed_fn in [seed_woningcorporaties, seed_inkoop_categories, seed_leveranciers]:
            try:
                seed_fn(db)
                results[seed_fn.__name__] = "ok"
            except Exception as e:
                results[seed_fn.__name__] = f"ERROR: {type(e).__name__}"
                db.rollback()
    finally:
        db.close()
    return results
