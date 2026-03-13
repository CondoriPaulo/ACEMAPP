import json
import logging

import azure.functions as func
import pandas as pd

from cleaners.acemapp_cleaner import clean_acemapp

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def _extract_bytes(req: func.HttpRequest) -> bytes:
    """
    Extract raw file bytes from the request.

    Logic App sends the file two ways:
      - Raw body (preferred): Content-Type: application/octet-stream
      - Multipart: Content-Type: multipart/form-data, field name 'file'
    """
    ct = req.headers.get("Content-Type", "")
    if "multipart/form-data" in ct:
        f = req.files.get("file")
        if not f:
            raise ValueError("multipart/form-data request is missing the 'file' field")
        return f.read()
    body = req.get_body()
    if not body:
        raise ValueError("Request body is empty")
    return body


@app.route(methods=["POST"])
def ingest_acemapp(req: func.HttpRequest) -> func.HttpResponse:
    """
    Receive an ACEMAPP Excel file, clean it, and return JSON for Logic App → SQL.

    Returns:
        200  {"table": "fact_rotation", "rows": [...]}
        400  validation error message
        500  internal server error
    """
    try:
        file_bytes = _extract_bytes(req)
        df = clean_acemapp(file_bytes)
        rows = df.where(pd.notnull(df), None).to_dict(orient="records")
        payload = json.dumps({"table": "fact_rotation", "rows": rows}, default=str)
        logging.info("ingest_acemapp: returning %d rows", len(rows))
        return func.HttpResponse(payload, status_code=200, mimetype="application/json")
    except ValueError as exc:
        logging.warning("ingest_acemapp validation error: %s", exc)
        return func.HttpResponse(str(exc), status_code=400)
    except Exception:
        logging.exception("Unexpected error in ingest_acemapp")
        return func.HttpResponse("Internal server error", status_code=500)
