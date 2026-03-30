from app.config import DATABASE_URL, models

TORTOISE_ORM = {
    "connections": {"default": DATABASE_URL},
    "apps": {
        "models": {
            "models": models,
            "default_connection": "default",
        },
    },
}