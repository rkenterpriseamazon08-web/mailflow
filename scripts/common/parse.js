const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const { FIELD_ALIASES } = require('./constants');
const validator = require('validator');

function normalizeKey(value) {
  return String(value || '').trim().toLowerCase().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ');
}

function detectField(headers, fieldName) {
  const aliases = FIELD_ALIASES[fieldName] || [fieldName];
  const normalized = headers.map(h => ({ raw: h, key: normalizeKey(h) }));
  for (const alias of aliases) {
    const wanted = normalizeKey(alias);
    const found = normalized.find(h => h.key === wanted);
    if (found) return found.raw;
  }
  return null;
}

function cleanMultiline(text) {
  return String(text || '')
    .replace(/\r/g, '')
    .split('\n')
    .filter((line, idx, arr) => !(line.trim() === '' && arr[idx - 1]?.trim() === ''))
    .join('\n')
    .trim();
}

function parseSpreadsheet(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' });
  if (!rows.length) return { total: 0, valid: [], invalid: [] };

  const headers = Object.keys(rows[0]);
  const mapping = {
    name: detectField(headers, 'name'),
    email_id: detectField(headers, 'email_id'),
    company_name: detectField(headers, 'company_name'),
    custom_line: detectField(headers, 'custom_line'),
    business_segment: detectField(headers, 'business_segment'),
    status: detectField(headers, 'status')
  };

  const valid = [];
  const invalid = [];

  rows.forEach((row, index) => {
    const email = String(row[mapping.email_id] || '').trim();
    if (!email || !validator.isEmail(email)) {
      invalid.push({ row: index + 2, reason: 'Invalid or missing email', rowData: row });
      return;
    }

    valid.push({
      name: String(row[mapping.name] || '').trim(),
      email_id: email,
      company_name: String(row[mapping.company_name] || '').trim(),
      custom_line: cleanMultiline(row[mapping.custom_line] || ''),
      business_segment: String(row[mapping.business_segment] || '').trim(),
      status: String(row[mapping.status] || '').trim() || 'Pending'
    });
  });

  return { total: rows.length, valid, invalid, mapping };
}

module.exports = { parseSpreadsheet, cleanMultiline };
