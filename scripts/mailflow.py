from __future__ import annotations

import argparse
import csv
import os
import re
import smtplib
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pandas as pd
import yaml
from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[1]
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass
class RenderedEmail:
    recipient: str
    subject: str
    html: str
    template_name: str
    row_number: int
    context: dict[str, Any]


def load_config(path: str) -> dict[str, Any]:
    config_path = resolve_path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return ROOT / candidate


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(column).strip().lower().replace(" ", "_").replace("-", "_")
        for column in df.columns
    ]
    return df.fillna("")


def read_recipients(config: dict[str, Any]) -> pd.DataFrame:
    source = config.get("data_source", {})
    source_type = source.get("type", "local_csv")

    if source_type == "local_csv":
        df = pd.read_csv(resolve_path(source["path"]))
    elif source_type == "local_excel":
        df = pd.read_excel(resolve_path(source["path"]))
    elif source_type == "google_csv":
        url_env = source.get("url_env", "GOOGLE_SHEET_CSV_URL")
        sheet_url = os.environ.get(url_env)
        if not sheet_url:
            raise RuntimeError(f"Missing environment variable: {url_env}")
        with urlopen(sheet_url, timeout=30) as response:
            df = pd.read_csv(response)
    else:
        raise RuntimeError(f"Unsupported data source type: {source_type}")

    return normalize_columns(df)


def parse_template(template_name: str) -> tuple[str, str]:
    template_path = resolve_path(f"templates/{template_name}.html")
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    raw = template_path.read_text(encoding="utf-8")
    if raw.startswith("---"):
        _, front_matter, body = raw.split("---", 2)
        meta = yaml.safe_load(front_matter) or {}
        subject = str(meta.get("subject", "")).strip()
    else:
        subject = ""
        body = raw

    if not subject:
        raise RuntimeError(f"Template {template_name} is missing a subject")
    return subject, body.strip()


def render_template(template_name: str, context: dict[str, Any]) -> tuple[str, str]:
    subject_template, body_template = parse_template(template_name)
    env = Environment(autoescape=True, undefined=StrictUndefined)
    subject = env.from_string(subject_template).render(**context)
    html = env.from_string(body_template).render(**context)
    return subject, html


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "skip"}


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def parse_date(value: Any) -> date | None:
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def validate_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email.strip()))


def choose_campaign_rows(df: pd.DataFrame, config: dict[str, Any]) -> list[tuple[int, pd.Series]]:
    cols = config.get("columns", {})
    email_col = cols.get("email", "email")
    status_col = cols.get("status", "status")
    skip_col = cols.get("do_not_email", "do_not_email")

    rows: list[tuple[int, pd.Series]] = []
    for idx, row in df.iterrows():
        email = str(row.get(email_col, "")).strip()
        status = str(row.get(status_col, "")).strip().lower()
        if truthy(row.get(skip_col, "")):
            continue
        if not validate_email(email):
            continue
        if status in {"", "pending", "queued"}:
            rows.append((idx, row))
    return rows


def choose_followup_rows(df: pd.DataFrame, config: dict[str, Any]) -> list[tuple[int, pd.Series]]:
    cols = config.get("columns", {})
    email_col = cols.get("email", "email")
    skip_col = cols.get("do_not_email", "do_not_email")
    next_col = cols.get("next_followup_at", "next_followup_at")
    stage_col = cols.get("followup_stage", "followup_stage")
    max_stage = as_int(config.get("followups", {}).get("max_stage", 1), 1)
    today = date.today()

    rows: list[tuple[int, pd.Series]] = []
    for idx, row in df.iterrows():
        email = str(row.get(email_col, "")).strip()
        if truthy(row.get(skip_col, "")) or not validate_email(email):
            continue
        due_date = parse_date(row.get(next_col, ""))
        stage = as_int(row.get(stage_col, 0), 0)
        if due_date and due_date <= today and stage < max_stage:
            rows.append((idx, row))
    return rows


def row_context(row: pd.Series, config: dict[str, Any]) -> dict[str, Any]:
    sending = config.get("sending", {})
    context = {key: str(value) for key, value in row.to_dict().items()}
    context.setdefault("from_name", sending.get("from_name", "Mailflow"))
    context["from_name"] = sending.get("from_name", context["from_name"])
    return context


def build_emails(
    rows: list[tuple[int, pd.Series]],
    config: dict[str, Any],
    mode: str,
    template_override: str | None,
) -> list[RenderedEmail]:
    cols = config.get("columns", {})
    email_col = cols.get("email", "email")
    template_col = cols.get("template", "template")
    default_template = config.get("campaign", {}).get("default_template", "cold_lead")
    followup_templates = config.get("followups", {}).get("templates", {})
    stage_col = cols.get("followup_stage", "followup_stage")

    emails: list[RenderedEmail] = []
    for row_number, row in rows:
        if mode == "followups":
            next_stage = as_int(row.get(stage_col, 0), 0) + 1
            template_name = followup_templates.get(str(next_stage), f"follow_up_{next_stage}")
        else:
            template_name = (
                template_override
                or str(row.get(template_col, "")).strip()
                or default_template
            )

        context = row_context(row, config)
        subject, html = render_template(template_name, context)
        emails.append(
            RenderedEmail(
                recipient=str(row.get(email_col, "")).strip(),
                subject=subject,
                html=html,
                template_name=template_name,
                row_number=int(row_number),
                context=context,
            )
        )
    return emails


def smtp_settings() -> dict[str, Any]:
    required = ["SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "FROM_EMAIL"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing SMTP environment variables: " + ", ".join(missing))
    return {
        "host": os.environ["SMTP_HOST"],
        "port": int(os.environ.get("SMTP_PORT", "587")),
        "user": os.environ["SMTP_USER"],
        "password": os.environ["SMTP_PASSWORD"],
        "from_email": os.environ["FROM_EMAIL"],
    }


def send_email(email: RenderedEmail, config: dict[str, Any], dry_run: bool) -> str:
    sending = config.get("sending", {})
    test_recipient_env = sending.get("dry_run_recipient_env", "TEST_RECIPIENT_EMAIL")
    recipient = os.environ.get(test_recipient_env, email.recipient) if dry_run else email.recipient

    if dry_run:
        print(f"[DRY RUN] Would send to {email.recipient}; test target is {recipient}")
        print(f"[DRY RUN] Subject: {email.subject}")
        return "dry_run"

    settings = smtp_settings()
    from_name = sending.get("from_name", "Mailflow")
    reply_to = sending.get("reply_to", "")

    message = EmailMessage()
    message["Subject"] = email.subject
    message["From"] = formataddr((from_name, settings["from_email"]))
    message["To"] = recipient
    if reply_to:
        message["Reply-To"] = reply_to
    message.set_content("This email contains HTML content. Please view it in an HTML mail client.")
    message.add_alternative(email.html, subtype="html")

    with smtplib.SMTP(settings["host"], settings["port"], timeout=30) as smtp:
        smtp.starttls()
        smtp.login(settings["user"], settings["password"])
        smtp.send_message(message)

    return "sent"


def write_log(records: list[dict[str, Any]]) -> Path:
    logs_dir = resolve_path("logs")
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    path = logs_dir / f"mailflow-{timestamp}.csv"
    fieldnames = [
        "timestamp_utc",
        "mode",
        "row_number",
        "recipient",
        "template",
        "subject",
        "status",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    return path


def update_local_status(
    df: pd.DataFrame,
    source_path: str,
    sent_rows: list[int],
    config: dict[str, Any],
    mode: str,
) -> None:
    source = config.get("data_source", {})
    if source.get("type") not in {"local_csv", "local_excel"}:
        return

    cols = config.get("columns", {})
    status_col = cols.get("status", "status")
    next_col = cols.get("next_followup_at", "next_followup_at")
    stage_col = cols.get("followup_stage", "followup_stage")
    followups = config.get("followups", {})

    for row_idx in sent_rows:
        if mode == "followups":
            next_stage = as_int(df.at[row_idx, stage_col], 0) + 1
            df.at[row_idx, stage_col] = next_stage
            df.at[row_idx, status_col] = f"followup_{next_stage}_sent"
            df.at[row_idx, next_col] = ""
        else:
            df.at[row_idx, status_col] = "sent"
            if followups.get("enabled", False):
                days = as_int(followups.get("days_after_initial", {}).get("1", 3), 3)
                df.at[row_idx, next_col] = (date.today() + timedelta(days=days)).isoformat()
                if stage_col in df.columns:
                    df.at[row_idx, stage_col] = as_int(df.at[row_idx, stage_col], 0)

    output_path = resolve_path(source_path)
    if output_path.suffix.lower() == ".csv":
        df.to_csv(output_path, index=False)
    else:
        df.to_excel(output_path, index=False)


def run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    df = read_recipients(config)
    mode = args.mode or config.get("campaign", {}).get("mode", "campaign")

    if mode == "validate":
        print(f"Loaded {len(df)} recipient rows")
        for template in sorted(path.stem for path in resolve_path("templates").glob("*.html")):
            parse_template(template)
            print(f"Template OK: {template}")
        return 0

    rows = choose_followup_rows(df, config) if mode == "followups" else choose_campaign_rows(df, config)
    max_emails = args.max_emails or int(config.get("sending", {}).get("max_emails_per_run", 25))
    rows = rows[:max_emails]
    emails = build_emails(rows, config, mode, args.template)

    if not emails:
        print("No eligible emails found.")
        return 0

    records: list[dict[str, Any]] = []
    sent_rows: list[int] = []
    failures = 0
    stop_after_failures = int(config.get("sending", {}).get("stop_after_failures", 5))
    delay_seconds = int(config.get("sending", {}).get("delay_seconds", 5))

    for email in emails:
        record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "row_number": email.row_number + 2,
            "recipient": email.recipient,
            "template": email.template_name,
            "subject": email.subject,
            "status": "",
            "error": "",
        }
        try:
            record["status"] = send_email(email, config, args.dry_run)
            if record["status"] == "sent":
                sent_rows.append(email.row_number)
        except Exception as exc:
            failures += 1
            record["status"] = "failed"
            record["error"] = str(exc)
            print(f"Failed to send to {email.recipient}: {exc}", file=sys.stderr)
            if failures >= stop_after_failures:
                records.append(record)
                print("Stopping after repeated failures.", file=sys.stderr)
                break
        records.append(record)
        if not args.dry_run and delay_seconds > 0:
            time.sleep(delay_seconds)

    log_path = write_log(records)
    print(f"Wrote log: {log_path}")

    if args.update_source and sent_rows and not args.dry_run:
        source_path = config.get("data_source", {}).get("path")
        if source_path:
            update_local_status(df, source_path, sent_rows, config, mode)
            print("Updated local recipient source status.")

    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Mailflow GitHub Actions email runner")
    parser.add_argument("--config", default="campaigns/campaign_config.yml")
    parser.add_argument("--mode", choices=["campaign", "followups", "validate"], default=None)
    parser.add_argument("--template", default=None, help="Override template for campaign sends")
    parser.add_argument("--max-emails", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-source", action="store_true")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

