from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BROKER: str = "localhost:29092"
    KAFKA_GROUP_ID: str = "fastapi-ws-group"
    
    TOPICS: list[str] = [
        "cdc.public.emprestimos_solicitados_agregado_diario",
        "cdc.public.emprestimos_aprovados_agregado_diario",
    ]

    class Config:
        env_file = ".env"

settings = Settings()