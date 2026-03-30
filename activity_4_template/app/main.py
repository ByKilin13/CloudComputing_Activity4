from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.authentication.api.router import router as authentication_router
from app.files.api.router import router as files_router
from app.config import DATABASE_URL, models

app = FastAPI()

@app.get("/healthcheck")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}

app.include_router(authentication_router)
app.include_router(files_router, prefix="/files")

register_tortoise(
    app,
    db_url=DATABASE_URL,
    modules={"models": models},
    generate_schemas=False,
    add_exception_handlers=True,
)
