from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
from io import StringIO
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
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

import pandas as pd
import yaml
from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
GOOGLE_SHEET_RE = re.compile(r"https://docs\.google\.com/spreadsheets/d/([^/]+)")
SUPPORTED_CLIENT_TYPES = {
    "storage",
    "office",
    "cafe",
    "house",
    "public toilet",
    "security cabin",
}
EMAIL_ALIASES = {"email", "email_address", "email_id", "mail", "mail_id", "recipient_email"}
CLIENT_TYPE_ALIASES = {"client_type", "clienttype", "client", "type", "category"}
NAME_ALIASES = {"name", "client_name", "customer_name", "full_name"}
COMPANY_ALIASES = {"company", "company_name", "business", "business_name"}
CUSTOM_NOTE_ALIASES = {"custom_note", "custome_note", "customer_note", "note", "notes", "remarks"}
SERVICE_ALIASES = {"service", "services", "requirement", "requirements", "project", "project_type"}


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


def coalesce_alias_columns(df: pd.DataFrame, aliases: set[str], target: str) -> pd.DataFrame:
    existing = [column for column in df.columns if column in aliases]
    if target in df.columns or not existing:
        return df
    df[target] = df[existing[0]]
    return df


def apply_column_aliases(df: pd.DataFrame) -> pd.DataFrame:
    df = coalesce_alias_columns(df, EMAIL_ALIASES, "email")
    df = coalesce_alias_columns(df, CLIENT_TYPE_ALIASES, "client_type")
    df = coalesce_alias_columns(df, NAME_ALIASES, "name")
    df = coalesce_alias_columns(df, COMPANY_ALIASES, "company")
    df = coalesce_alias_columns(df, CUSTOM_NOTE_ALIASES, "custom_note")
    df = coalesce_alias_columns(df, SERVICE_ALIASES, "service")
    return df


def google_sheet_csv_url(url: str) -> str:
    match = GOOGLE_SHEET_RE.search(url)
    if not match:
        return url

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    fragment_query = parse_qs(parsed.fragment)
    gid = query.get("gid", fragment_query.get("gid", ["0"]))[0]
    sheet_id = match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?{urlencode({'format': 'csv', 'gid': gid})}"


def read_csv_url(url: str) -> pd.DataFrame:
    csv_url = google_sheet_csv_url(url.strip())
    with urlopen(csv_url, timeout=30) as response:
        content_type = response.headers.get("content-type", "")
        raw = response.read()

    text = raw.decode("utf-8-sig", errors="replace")
    if "text/html" in content_type.lower() or text.lstrip().lower().startswith("<!doctype html") or text.lstrip().lower().startswith("<html"):
        raise RuntimeError(
            "Google Sheet URL returned HTML instead of CSV. Open the sheet sharing settings and set access to "
            "'Anyone with the link can view', or use File -> Share -> Publish to web -> CSV."
        )

    return pd.read_csv(StringIO(text), sep=None, engine="python")


def read_recipients(config: dict[str, Any]) -> pd.DataFrame:
    source = config.get("data_source", {})
    source_type = os.environ.get("DATA_SOURCE_TYPE", source.get("type", "local_csv")).strip()
    source_path = os.environ.get("DATA_SOURCE_PATH", source.get("path", "")).strip()

    if source_type == "local_csv":
        if not source_path:
            raise RuntimeError("DATA_SOURCE_PATH or data_source.path is required for local_csv")
        df = pd.read_csv(resolve_path(source_path), sep=None, engine="python")
    elif source_type == "local_excel":
        if not source_path:
            raise RuntimeError("DATA_SOURCE_PATH or data_source.path is required for local_excel")
        df = pd.read_excel(resolve_path(source_path))
    elif source_type == "google_csv":
        url_env = source.get("url_env", "GOOGLE_SHEET_CSV_URL")
        sheet_url = os.environ.get(url_env)
        if not sheet_url:
            fallback_path = source.get("fallback_path")
            if fallback_path:
                print(f"{url_env} is not set. Using fallback source: {fallback_path}")
                df = pd.read_csv(resolve_path(fallback_path), sep=None, engine="python")
                return apply_column_aliases(normalize_columns(df))
            raise RuntimeError(f"Missing environment variable: {url_env}")
        df = read_csv_url(sheet_url)
    else:
        raise RuntimeError(f"Unsupported data source type: {source_type}")

    return apply_column_aliases(normalize_columns(df))


def normalize_client_type(value: Any) -> str:
    normalized = str(value).strip().lower().replace("_", " ").replace("-", " ")
    if normalized == "security":
        return "security cabin"
    if normalized == "toilet":
        return "public toilet"
    return normalized


def parse_template(template_name: str) -> tuple[str, str, dict[str, Any]]:
    template_path = resolve_path(f"templates/{template_name}.html")
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    raw = template_path.read_text(encoding="utf-8")
    if raw.startswith("---"):
        _, front_matter, body = raw.split("---", 2)
        meta = yaml.safe_load(front_matter) or {}
        subject = str(meta.get("subject", "")).strip()
    else:
        meta = {}
        subject = ""
        body = raw

    if not subject:
        raise RuntimeError(f"Template {template_name} is missing a subject")
    return subject, body.strip(), meta


def render_template(template_name: str, context: dict[str, Any]) -> tuple[str, str]:
    subject_template, body_template, _ = parse_template(template_name)
    env = Environment(autoescape=True)
    subject = env.from_string(subject_template).render(**context)
    html = env.from_string(body_template).render(**context)
    return subject, html


def template_client_type(template_name: str) -> str:
    _, _, meta = parse_template(template_name)
    return normalize_client_type(meta.get("clientType", meta.get("client_type", "")))


def safe_template_client_type(template_name: str) -> str:
    try:
        return template_client_type(template_name)
    except FileNotFoundError:
        return ""


def all_template_names() -> list[str]:
    return sorted(path.stem for path in resolve_path("templates").glob("*.html"))


def matching_template_for_client_type(client_type: str) -> str:
    normalized = normalize_client_type(client_type)
    if normalized not in SUPPORTED_CLIENT_TYPES:
        return ""
    for template_name in all_template_names():
        if template_client_type(template_name) == normalized:
            return template_name
    return ""


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


def redact_email(email: str) -> str:
    email = str(email).strip()
    if "@" not in email:
        return email[:3] + "***" if email else ""
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


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


def explain_campaign_rows(df: pd.DataFrame, config: dict[str, Any]) -> dict[str, int]:
    cols = config.get("columns", {})
    email_col = cols.get("email", "email")
    status_col = cols.get("status", "status")
    skip_col = cols.get("do_not_email", "do_not_email")
    stats = {
        "total": len(df),
        "eligible": 0,
        "do_not_email": 0,
        "invalid_email": 0,
        "not_pending": 0,
    }

    for _, row in df.iterrows():
        email = str(row.get(email_col, "")).strip()
        status = str(row.get(status_col, "")).strip().lower()
        if truthy(row.get(skip_col, "")):
            stats["do_not_email"] += 1
            continue
        if not validate_email(email):
            stats["invalid_email"] += 1
            continue
        if status in {"", "pending", "queued"}:
            stats["eligible"] += 1
        else:
            stats["not_pending"] += 1
    return stats


def write_source_debug(df: pd.DataFrame, config: dict[str, Any], stats: dict[str, int] | None = None) -> Path:
    logs_dir = resolve_path("logs")
    logs_dir.mkdir(exist_ok=True)
    path = logs_dir / "recipient-source-debug.txt"
    cols = config.get("columns", {})
    email_col = cols.get("email", "email")
    client_type_col = cols.get("client_type", "client_type")
    status_col = cols.get("status", "status")
    skip_col = cols.get("do_not_email", "do_not_email")

    lines = [
        "Recipient source debug",
        f"columns={list(df.columns)}",
        f"row_count={len(df)}",
    ]
    if stats:
        lines.append(f"stats={stats}")
    lines.append("")
    lines.append("First rows:")
    for index, row in df.head(10).iterrows():
        email = str(row.get(email_col, "")).strip()
        lines.append(
            f"row={index + 2} email={redact_email(email)} valid_email={validate_email(email)} "
            f"client_type={row.get(client_type_col, '')} status={row.get(status_col, '')} "
            f"do_not_email={row.get(skip_col, '')}"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


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
    client_type_col = cols.get("client_type", "client_type")
    default_template = config.get("campaign", {}).get("default_template", "cold_lead")
    followup_templates = config.get("followups", {}).get("templates", {})
    stage_col = cols.get("followup_stage", "followup_stage")

    emails: list[RenderedEmail] = []
    for row_number, row in rows:
        client_type = normalize_client_type(row.get(client_type_col, ""))
        if mode == "followups":
            next_stage = as_int(row.get(stage_col, 0), 0) + 1
            template_name = followup_templates.get(str(next_stage), f"follow_up_{next_stage}")
        else:
            template_name = (
                template_override
                or str(row.get(template_col, "")).strip()
                or default_template
            )

        if safe_template_client_type(template_name) != client_type:
            template_name = matching_template_for_client_type(client_type)
        if not template_name:
            print(
                f"No matching template for client type '{client_type}' on row {int(row_number) + 2}. Skipping."
            )
            continue

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
        "security": os.environ.get("SMTP_SECURITY", "auto").strip().lower(),
    }


@contextmanager
def smtp_connection(settings: dict[str, Any]):
    host = settings["host"]
    port = settings["port"]
    security = settings["security"]

    if security == "auto":
        security = "ssl" if port == 465 else "starttls"

    try:
        if security == "ssl":
            smtp = smtplib.SMTP_SSL(host, port, timeout=30)
        else:
            smtp = smtplib.SMTP(host, port, timeout=30)
            if security == "starttls":
                smtp.starttls()
        smtp.login(settings["user"], settings["password"])
        yield smtp
    except smtplib.SMTPServerDisconnected as exc:
        raise RuntimeError(
            "SMTP server closed the connection. Check SMTP_HOST, SMTP_PORT, and SMTP_SECURITY. "
            "For Zoho, use smtp.zoho.com or smtp.zoho.in with port 587 and SMTP_SECURITY=starttls, "
            "or port 465 and SMTP_SECURITY=ssl."
        ) from exc
    finally:
        try:
            smtp.quit()
        except Exception:
            pass


def verify_smtp_login() -> None:
    settings = smtp_settings()
    with smtp_connection(settings):
        pass


def send_smtp_test_email() -> None:
    settings = smtp_settings()
    recipient = os.environ.get("TEST_RECIPIENT_EMAIL", settings["user"])
    message = EmailMessage()
    message["Subject"] = "Mailflow SMTP test"
    message["From"] = formataddr(("Mailflow", settings["from_email"]))
    message["To"] = recipient
    message.set_content(
        "This is a Mailflow SMTP test email. If you received this, Zoho SMTP sending is working."
    )

    with smtp_connection(settings) as smtp:
        smtp.send_message(message)


def send_email(email: RenderedEmail, config: dict[str, Any], dry_run: bool) -> str:
    sending = config.get("sending", {})

    if dry_run:
        print(f"[DRY RUN] Would send to {email.recipient}")
        print(f"[DRY RUN] Subject: {email.subject}")
        return "dry_run"

    recipient = email.recipient
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

    with smtp_connection(settings) as smtp:
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
    mode = args.mode or config.get("campaign", {}).get("mode", "campaign")

    if mode == "smtp-test":
        send_smtp_test_email()
        print("SMTP test email sent.")
        return 0

    df = read_recipients(config)
    print(f"Loaded source with {len(df)} row(s) and columns: {list(df.columns)}")

    if mode == "validate":
        print(f"Loaded {len(df)} recipient rows")
        for template in all_template_names():
            _, _, meta = parse_template(template)
            client_type = normalize_client_type(meta.get("clientType", meta.get("client_type", "")))
            if client_type and client_type not in SUPPORTED_CLIENT_TYPES:
                raise RuntimeError(f"Template {template} has unsupported clientType: {client_type}")
            if not client_type:
                print(f"Template warning: {template} has no clientType")
            print(f"Template OK: {template}")
        return 0

    rows = choose_followup_rows(df, config) if mode == "followups" else choose_campaign_rows(df, config)
    if mode == "campaign":
        stats = explain_campaign_rows(df, config)
        debug_path = write_source_debug(df, config, stats)
        print(
            "Campaign row summary: "
            f"total={stats['total']}, eligible={stats['eligible']}, "
            f"do_not_email={stats['do_not_email']}, invalid_email={stats['invalid_email']}, "
            f"not_pending={stats['not_pending']}"
        )
        print(f"Wrote source debug: {debug_path}")
        if "email" not in df.columns:
            print(
                "No usable email column found. Add a column named email, email_address, email_id, mail, mail_id, or recipient_email.",
                file=sys.stderr,
            )
            return 2
    max_emails = args.max_emails or int(config.get("sending", {}).get("max_emails_per_run", 25))
    rows = rows[:max_emails]
    emails = build_emails(rows, config, mode, args.template)
    print(f"Prepared {len(emails)} email(s). dry_run={args.dry_run}")

    if not emails:
        print("No eligible emails found or no matching templates were available.", file=sys.stderr)
        return 2

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
    print(
        f"Send summary: sent={sum(1 for record in records if record['status'] == 'sent')}, "
        f"dry_run={sum(1 for record in records if record['status'] == 'dry_run')}, "
        f"failed={sum(1 for record in records if record['status'] == 'failed')}"
    )

    if args.update_source and sent_rows and not args.dry_run:
        source_path = config.get("data_source", {}).get("path")
        if source_path:
            update_local_status(df, source_path, sent_rows, config, mode)
            print("Updated local recipient source status.")

    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Mailflow GitHub Actions email runner")
    parser.add_argument("--config", default="campaigns/campaign_config.yml")
    parser.add_argument("--mode", choices=["campaign", "followups", "validate", "smtp-test"], default=None)
    parser.add_argument("--template", default=None, help="Override template for campaign sends")
    parser.add_argument("--max-emails", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-source", action="store_true")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
