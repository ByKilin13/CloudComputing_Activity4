from typing import Optional
import httpx
import uuid
from pypdf import PdfMerger
from io import BytesIO

from fastapi import APIRouter, Header, HTTPException, Body, File, UploadFile
from pydantic import BaseModel

from app.files.persistence.s3_repo import s3_upload_bytes, s3_download_bytes, s3_delete

router = APIRouter()


class User(BaseModel):
    username: str
    address: Optional[str] = None


class FileBusinesObject(BaseModel):
    id: int
    user: User
    title: str
    author: str
    path: Optional[str] = None


id_counter = 0
files_database = {}


async def introspect(token: str) -> User:
    url = "http://localhost/introspect"
    headers = {"accept": "application/json", "auth": token}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return User(**response.json())


def check_file_ownership(id: int, user: User):
    if id not in files_database:
        raise HTTPException(status_code=404, detail="File not found")
    file = files_database[id]
    if user.username != file.user.username:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return file


class PostFilesMerge(BaseModel):
    file_id_1: int
    file_id_2: int


@router.post("/merge")
async def post_files_merge(token: str = Header(alias="auth"), input: PostFilesMerge = Body()) -> dict[str, int]:
    user = await introspect(token=token)
    f1 = check_file_ownership(input.file_id_1, user)
    f2 = check_file_ownership(input.file_id_2, user)
    if not f1.path or not f2.path:
        raise HTTPException(status_code=400, detail="One file has no content")
    pdf1 = BytesIO(s3_download_bytes(f1.path))
    pdf2 = BytesIO(s3_download_bytes(f2.path))
    merger = PdfMerger()
    pdf1.seek(0)
    merger.append(pdf1)
    pdf2.seek(0)
    merger.append(pdf2)
    out = BytesIO()
    merger.write(out)
    merger.close()
    out.seek(0)
    global id_counter
    current_id = id_counter
    id_counter += 1
    new_file = FileBusinesObject(id=current_id, user=user, title="merged.pdf", author=user.username, path=None)
    files_database[current_id] = new_file
    s3_key = f"files/{uuid.uuid4()}.pdf"
    s3_upload_bytes(s3_key, out.read(), content_type="application/pdf")
    new_file.path = s3_key
    return {"id": current_id}


@router.get("")
async def get_files(token: str = Header(alias="auth")) -> dict[str, list[dict]]:
    user = await introspect(token=token)
    items = []
    for f in files_database.values():
        if f.user.username == user.username:
            items.append({"id": f.id, "title": f.title, "author": f.author, "has_content": f.path is not None})
    return {"files": items}


class FilesPostInput(BaseModel):
    author: str
    title: str


@router.post("")
async def post_files(token: str = Header(alias="auth"), input: FilesPostInput = Body()) -> int:
    user = await introspect(token=token)
    global id_counter
    current_id = id_counter
    id_counter += 1
    file = FileBusinesObject(id=current_id, user=user, title=input.title, author=input.author, path=None)
    files_database[current_id] = file
    return current_id


@router.get("/{id}")
async def get_files_id(id: int, token: str = Header(alias="auth")) -> FileBusinesObject:
    user = await introspect(token=token)
    _ = check_file_ownership(id, user)
    return files_database[id]


@router.post("/{id}")
async def post_files_id(id: int, token: str = Header(alias="auth"), file_content: UploadFile = File()) -> dict[str, str]:
    user = await introspect(token=token)
    f = check_file_ownership(id, user)
    content = await file_content.read()
    ct = file_content.content_type or "application/pdf"
    s3_key = f"files/{uuid.uuid4()}.pdf"
    s3_upload_bytes(s3_key, content, content_type=ct)
    f.path = s3_key
    return {"status": "ok"}


@router.delete("/{id}")
async def delete_files_id(id: int, token: str = Header(alias="auth")) -> dict[str, str]:
    user = await introspect(token=token)
    f = check_file_ownership(id, user)
    if f.path:
        s3_delete(f.path)
    del files_database[id]
    return {"status": "deleted"}