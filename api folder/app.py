from fastapi import FastAPI, Body
from pydantic import BaseModel
from multi_sheet_loader import load_specific_file_to_sql

app = FastAPI()

class IngestionRequest(BaseModel):
    filename: str
    prefix: str = "CustomTable"

@app.post("/ingest-file")
def ingest_file(req: IngestionRequest):
    status = load_specific_file_to_sql(req.filename, req.prefix)
    return {"status": status}
