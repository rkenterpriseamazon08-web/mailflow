const { google } = require('googleapis');
const { MASTER_HEADERS } = require('./constants');

function getEnv(name, fallback = '') {
  return process.env[name] || fallback;
}

function getSheetsClient() {
  const raw = getEnv('GOOGLE_CREDENTIALS_JSON');
  if (!raw) throw new Error('Missing GOOGLE_CREDENTIALS_JSON');
  const credentials = JSON.parse(raw);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

function getSheetInfo() {
  const spreadsheetId = getEnv('MASTER_SHEET_ID');
  const tabName = getEnv('MASTER_SHEET_TAB', 'MasterData');
  if (!spreadsheetId) throw new Error('Missing MASTER_SHEET_ID');
  return { spreadsheetId, tabName };
}

async function ensureTabExists() {
  const sheets = getSheetsClient();
  const { spreadsheetId, tabName } = getSheetInfo();
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const exists = (meta.data.sheets || []).some(s => s.properties && s.properties.title === tabName);
  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{ addSheet: { properties: { title: tabName } } }]
      }
    });
  }
}

async function initHeaderRow() {
  await ensureTabExists();
  const sheets = getSheetsClient();
  const { spreadsheetId, tabName } = getSheetInfo();
  const current = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${tabName}!1:1`
  });
  const row = current.data.values && current.data.values[0] ? current.data.values[0] : [];
  if (row.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${tabName}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: [MASTER_HEADERS] }
    });
    return { created: true };
  }
  return { created: false, existingHeaders: row };
}

async function getAllRows() {
  await ensureTabExists();
  const sheets = getSheetsClient();
  const { spreadsheetId, tabName } = getSheetInfo();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${tabName}!A:ZZ`
  });
  const values = res.data.values || [];
  if (values.length === 0) return { headers: MASTER_HEADERS, rows: [] };
  const headers = values[0];
  const rows = values.slice(1).map((arr, idx) => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = arr[i] || ''; });
    obj.__rowIndex = idx + 2;
    return obj;
  });
  return { headers, rows };
}

async function appendRows(objects) {
  if (!objects.length) return;
  const sheets = getSheetsClient();
  const { spreadsheetId, tabName } = getSheetInfo();
  const values = objects.map(obj => MASTER_HEADERS.map(h => obj[h] ?? ''));
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${tabName}!A:ZZ`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });
}

async function updateRow(rowIndex, object) {
  const sheets = getSheetsClient();
  const { spreadsheetId, tabName } = getSheetInfo();
  const values = [MASTER_HEADERS.map(h => object[h] ?? '')];
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${tabName}!A${rowIndex}:ZZ${rowIndex}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values }
  });
}

module.exports = { getSheetInfo, initHeaderRow, getAllRows, appendRows, updateRow };
