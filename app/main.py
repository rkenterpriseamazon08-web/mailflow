from __future__ import annotations

import csv
import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import mailflow  # noqa: E402


CLIENT_TYPES = ["storage", "office", "cafe", "house", "public toilet", "security cabin"]
TEMPLATE_FILES = {
    "storage": "storage",
    "office": "office",
    "cafe": "cafe",
    "house": "house",
    "public toilet": "public_toilet",
    "security cabin": "security_cabin",
}

app = FastAPI(title="Mailflow Dashboard")
app.mount("/static", StaticFiles(directory=ROOT / "web"), name="static")

jobs: dict[str, dict[str, Any]] = {}


def load_config() -> dict[str, Any]:
    with (ROOT / "campaigns" / "campaign_config.yml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def normalize_row(row: dict[str, Any]) -> dict[str, str]:
    normalized = {
        str(key).strip().lower().replace(" ", "_").replace("-", "_"): str(value or "").strip()
        for key, value in row.items()
    }
    normalized.setdefault("custom_note", normalized.get("custome_note", normalized.get("customer_note", normalized.get("note", normalized.get("notes", normalized.get("remarks", ""))))))
    normalized.setdefault("service", normalized.get("services", normalized.get("requirement", normalized.get("requirements", normalized.get("project", normalized.get("project_type", ""))))))
    normalized.setdefault("email", normalized.get("email_address", normalized.get("email_id", normalized.get("mail", normalized.get("mail_id", normalized.get("recipient_email", ""))))))
    normalized.setdefault("client_type", normalized.get("clienttype", normalized.get("client", normalized.get("type", normalized.get("category", "")))))
    normalized.setdefault("company", normalized.get("company_name", normalized.get("business", normalized.get("business_name", ""))))
    normalized.setdefault("name", normalized.get("client_name", normalized.get("customer_name", normalized.get("full_name", ""))))
    return normalized


def template_path_for_client_type(client_type: str) -> Path:
    normalized = mailflow.normalize_client_type(client_type)
    template_name = TEMPLATE_FILES.get(normalized)
    if not template_name:
        raise HTTPException(status_code=400, detail="Unsupported client type")
    return ROOT / "templates" / f"{template_name}.html"


def split_template(raw: str) -> tuple[str, str, str]:
    if raw.startswith("---"):
        _, front_matter, body = raw.split("---", 2)
        meta = yaml.safe_load(front_matter) or {}
        subject = str(meta.get("subject", "")).strip()
        client_type = str(meta.get("clientType", meta.get("client_type", ""))).strip()
        return subject, client_type, body.strip()
    return "", "", raw.strip()


def read_template(client_type: str) -> dict[str, str]:
    path = template_path_for_client_type(client_type)
    if not path.exists():
        return {"clientType": client_type, "subject": "", "body": ""}
    subject, tagged_type, body = split_template(path.read_text(encoding="utf-8"))
    return {"clientType": tagged_type or client_type, "subject": subject, "body": body}


def write_template(client_type: str, subject: str, body: str) -> dict[str, str]:
    normalized = mailflow.normalize_client_type(client_type)
    path = template_path_for_client_type(normalized)
    safe_subject = subject.strip()
    safe_body = body.strip()
    if not safe_subject:
        raise HTTPException(status_code=400, detail="Subject is required")
    if not safe_body:
        raise HTTPException(status_code=400, detail="Template body is required")
    path.write_text(
        f"---\nsubject: {json.dumps(safe_subject)}\nclientType: {json.dumps(normalized)}\n---\n{safe_body}\n",
        encoding="utf-8",
    )
    return read_template(normalized)


def parse_csv_text(text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(text.splitlines())
    return [normalize_row(row) for row in reader]


def dataframe_rows_to_dicts(df) -> list[dict[str, str]]:
    df = mailflow.apply_column_aliases(mailflow.normalize_columns(df))
    return [normalize_row(row) for row in df.to_dict(orient="records")]


def job_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "state": job["state"],
        "total": job["total"],
        "sent": job["sent"],
        "failed": job["failed"],
        "skipped": job["skipped"],
        "dryRun": job["dryRun"],
        "rows": job["rows"],
        "startedAt": job["startedAt"],
        "finishedAt": job["finishedAt"],
    }


def run_send_job(job_id: str, rows: list[dict[str, str]], dry_run: bool) -> None:
    config = load_config()
    delay_seconds = int(config.get("sending", {}).get("delay_seconds", 5))
    job = jobs[job_id]
    job["state"] = "running"

    for index, row in enumerate(rows):
        normalized = normalize_row(row)
        job_row = job["rows"][index]
        job_row["status"] = "sending"
        job_row["error"] = ""

        try:
            if mailflow.truthy(normalized.get("do_not_email", "")):
                job_row["status"] = "skipped"
                job_row["error"] = "do_not_email is enabled"
                job["skipped"] += 1
                continue

            email = normalized.get("email", "")
            if not mailflow.validate_email(email):
                job_row["status"] = "skipped"
                job_row["error"] = "Invalid email"
                job["skipped"] += 1
                continue

            client_type = mailflow.normalize_client_type(normalized.get("client_type", ""))
            template_name = mailflow.matching_template_for_client_type(client_type)
            if not template_name:
                job_row["status"] = "skipped"
                job_row["error"] = f"No template for client type: {client_type or 'blank'}"
                job["skipped"] += 1
                continue

            context = dict(normalized)
            context["from_name"] = config.get("sending", {}).get("from_name", "Mailflow")
            subject, html = mailflow.render_template(template_name, context)
            rendered = mailflow.RenderedEmail(
                recipient=email,
                subject=subject,
                html=html,
                template_name=template_name,
                row_number=index,
                context=context,
            )
            result = mailflow.send_email(rendered, config, dry_run)
            job_row["template"] = template_name
            job_row["subject"] = subject
            job_row["status"] = "previewed" if result == "dry_run" else "sent"
            job["sent"] += 1 if result != "dry_run" else 0

            if not dry_run and delay_seconds > 0:
                time.sleep(delay_seconds)
        except Exception as exc:
            job_row["status"] = "failed"
            job_row["error"] = str(exc)
            job["failed"] += 1

    job["state"] = "finished"
    job["finishedAt"] = datetime.now(timezone.utc).isoformat()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(ROOT / "web" / "index.html")


@app.get("/api/client-types")
def get_client_types() -> dict[str, list[str]]:
    return {"clientTypes": CLIENT_TYPES}


@app.get("/api/templates")
def get_templates() -> dict[str, list[dict[str, str]]]:
    return {"templates": [read_template(client_type) for client_type in CLIENT_TYPES]}


@app.put("/api/templates/{client_type}")
async def save_template(client_type: str, request: Request) -> dict[str, str]:
    payload = await request.json()
    return write_template(client_type, str(payload.get("subject", "")), str(payload.get("body", "")))


@app.post("/api/import-url")
async def import_url(request: Request) -> dict[str, list[dict[str, str]]]:
    payload = await request.json()
    url = str(payload.get("url", "")).strip()
    if not url:
        raise HTTPException(status_code=400, detail="Google Sheet CSV URL is required")
    try:
        df = mailflow.read_csv_url(url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read CSV URL: {exc}") from exc
    return {"rows": dataframe_rows_to_dicts(df)}


@app.post("/api/send")
async def send_bulk(request: Request) -> dict[str, Any]:
    payload = await request.json()
    rows = [normalize_row(row) for row in payload.get("rows", [])]
    if not rows:
        raise HTTPException(status_code=400, detail="No rows to send")

    dry_run = bool(payload.get("dryRun", False))
    job_id = uuid.uuid4().hex
    jobs[job_id] = {
        "id": job_id,
        "state": "queued",
        "total": len(rows),
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "dryRun": dry_run,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "finishedAt": "",
        "rows": [
            {
                "name": row.get("name", ""),
                "email": row.get("email", ""),
                "client_type": row.get("client_type", ""),
                "template": "",
                "subject": "",
                "status": "queued",
                "error": "",
            }
            for row in rows
        ],
    }

    thread = threading.Thread(target=run_send_job, args=(job_id, rows, dry_run), daemon=True)
    thread.start()
    return job_snapshot(jobs[job_id])


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job_snapshot(job)
