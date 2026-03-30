from fastapi import APIRouter, Body, HTTPException, Header
from pydantic import BaseModel
from hashlib import sha256
import uuid
import os
import redis

router = APIRouter()

users = {}

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
TOKEN_TTL = int(os.getenv("TOKEN_TTL", "3600"))


class User(BaseModel):
    username: str
    password: bytes
    mail: str
    age_of_birth: int


class RegisterInput(BaseModel):
    username: str
    password: str
    mail: str
    age_of_birth: int


class RegisterOutput(BaseModel):
    username: str
    mail: str
    age_of_birth: int


@router.post("/register")
async def register_post(input: RegisterInput = Body()) -> dict[str, RegisterOutput]:
    if input.username in users:
        raise HTTPException(status_code=409, detail="This username is already taken")
    hash_password = input.username + input.password
    hashed_password = sha256(hash_password.encode()).digest()
    new_user = User(
        username=input.username,
        password=hashed_password,
        mail=input.mail,
        age_of_birth=input.age_of_birth,
    )
    users[input.username] = new_user
    output = RegisterOutput(
        username=input.username,
        mail=input.mail,
        age_of_birth=input.age_of_birth,
    )
    return {"new_user": output}


class LoginInput(BaseModel):
    username: str
    password: str


@router.post("/login")
async def healthcheck(input: LoginInput = Body()) -> dict[str, str]:
    if input.username not in users:
        raise HTTPException(status_code=404, detail="User not found")
    hashed_stored_password = users[input.username].password
    hash_password = input.username + input.password
    hashed_input_password = sha256(hash_password.encode()).digest()
    if hashed_stored_password == hashed_input_password:
        random_id = str(uuid.uuid4())
        r.set(random_id, input.username, ex=TOKEN_TTL)
        return {"auth": random_id}
    else:
        raise HTTPException(status_code=403, detail="Password is not correct")


class IntrospectOutput(BaseModel):
    username: str
    mail: str
    age_of_birth: int


@router.get("/introspect")
async def introspect_get(auth: str = Header()) -> IntrospectOutput:
    current_username = r.get(auth)
    if not current_username:
        raise HTTPException(status_code=403, detail="Forbidden")
    user = users[current_username]
    return IntrospectOutput(
        username=user.username,
        mail=user.mail,
        age_of_birth=user.age_of_birth,
    )


@router.delete("/logout")
async def healthcheck(auth: str = Header()) -> dict[str, str]:
    if not r.get(auth):
        raise HTTPException(status_code=403, detail="Forbidden")
    r.delete(auth)
    return {"status": "ok"}
