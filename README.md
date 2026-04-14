# MailFlow GitHub Edition

A simplified GitHub-only version of your bulk email flow app.

## What changed

The original project used:
- a permanent Express backend
- SQLite
- local server hosting

That does **not** fit your requirement of hosting everything on GitHub.

So this version is redesigned to use:
- **GitHub Pages** for the web UI
- **GitHub Actions** for secure background jobs
- **Google Sheets as the only data store**
- **Zoho SMTP through GitHub Secrets**

## Important limitation

A static GitHub Pages website **cannot safely send SMTP emails or use Google service account secrets directly in the browser**.

So this repo uses the secure GitHub-native pattern:
- UI is hosted on GitHub Pages
- email/import/follow-up jobs run through GitHub Actions
- all master tracking stays in Google Sheets

This keeps everything on GitHub and avoids external hosting.

## Repository structure

- `web/` → static UI for GitHub Pages
- `scripts/` → Node scripts used by GitHub Actions
- `.github/workflows/deploy.yml` → deploy Pages
- `.github/workflows/mailflow-runner.yml` → run import/send/follow-up jobs
- `templates/` → JSON email templates
- `imports/` → upload your CSV/XLSX files here before running import workflow

## Required GitHub Secrets

Go to **Repo → Settings → Secrets and variables → Actions** and add:

- `GOOGLE_CREDENTIALS_JSON`
- `MASTER_SHEET_ID`
- `MASTER_SHEET_TAB`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_SECURE`
- `SMTP_USER`
- `SMTP_PASS`
- `SMTP_FROM_EMAIL`
- `SMTP_FROM_NAME`

## How to use

### 1. Initialize master sheet
Run workflow **MailFlow Runner** with:
- task = `init-sheet`

### 2. Import contacts from file
Commit your file into `imports/` and run workflow with:
- task = `import-file`
- import_file = `imports/your-file.csv`
- campaign_name = `Campaign A`

### 3. Send initial emails
Create a template JSON in `templates/`, then run:
- task = `send-initial`
- campaign_name = `Campaign A`
- template_file = `templates/initial.json`
- retry_failed = `false`

### 4. Send follow-up emails
Run:
- task = `send-followup`
- campaign_name = `Campaign A`
- template_file = `templates/followup1.json`
- followup_stage = `1`
- retry_failed = `false`

## Master Sheet columns

This repo uses your exact Google Sheet structure, including 10 follow-up stages.

## Template format

Example `templates/initial.json`:

```json
{
  "name": "Initial Outreach",
  "subject": "Hi {{name}} | Quick note from {{company}}",
  "body": "Hi {{name}},\n\nI wanted to reach out regarding {{company}}.\n{{customLine}}\n\nBest regards,\nKapil"
}
```

## Deploying GitHub Pages

Push this repo to GitHub and enable Pages source as **GitHub Actions**.
The `deploy.yml` workflow will publish the `web/` folder automatically.
