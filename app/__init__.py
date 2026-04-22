from fastapi import FastAPI

from .api import router
from .lifespan import lifespan

app = FastAPI(title="bookee-backend", lifespan=lifespan)
app.include_router(router)
