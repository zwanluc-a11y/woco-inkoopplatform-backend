from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./data/woco_inkoopplatform.db"
    ANTHROPIC_API_KEY: str = ""
    CORS_ORIGINS: str = "http://localhost:3000"
    FRONTEND_URL: str = "http://localhost:3000"

    # Clerk authentication
    CLERK_JWKS_URL: str = ""
    CLERK_ISSUER: str = ""
    CLERK_SECRET_KEY: str = ""

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
