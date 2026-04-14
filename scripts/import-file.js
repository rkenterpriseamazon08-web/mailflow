const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { parseSpreadsheet } = require('./common/parse');
const { getAllRows, appendRows, initHeaderRow } = require('./common/google');
const { MASTER_HEADERS } = require('./common/constants');

const importFile = process.env.IMPORT_FILE;
const campaignName = process.env.CAMPAIGN_NAME;

if (!importFile) throw new Error('IMPORT_FILE is required');
if (!campaignName) throw new Error('CAMPAIGN_NAME is required');

(async () => {
  await initHeaderRow();
  const parsed = parseSpreadsheet(importFile);
  const { rows: existingRows } = await getAllRows();

  const emailStatusMap = new Map();
  existingRows.forEach(r => {
    const key = String(r.email_id || '').trim().toLowerCase();
    if (!key) return;
    if (!emailStatusMap.has(key)) emailStatusMap.set(key, []);
    emailStatusMap.get(key).push(r);
  });

  const now = new Date().toISOString();
  const sourceFileName = path.basename(importFile);
  const toAppend = [];
  let skipped = 0;
  let added = 0;

  parsed.valid.forEach(contact => {
    const existing = emailStatusMap.get(contact.email_id.toLowerCase()) || [];
    const alreadySentForCampaign = existing.some(r => (r.campaign_name || '') === campaignName && (r.status || '') === 'Sent');
    const exactExistingForCampaign = existing.some(r => (r.campaign_name || '') === campaignName);

    if (alreadySentForCampaign || exactExistingForCampaign) {
      skipped += 1;
      return;
    }

    const row = Object.fromEntries(MASTER_HEADERS.map(h => [h, '']));
    row.record_id = uuidv4();
    row.name = contact.name;
    row.email_id = contact.email_id;
    row.company_name = contact.company_name;
    row.custom_line = contact.custom_line;
    row.business_segment = contact.business_segment;
    row.status = contact.status || 'Pending';
    row.campaign_name = campaignName;
    row.template_name = '';
    row.sent_at = '';
    row.error_message = '';
    row.source_file_name = sourceFileName;
    row.uploaded_at = now;
    row.updated_at = now;
    toAppend.push(row);
    added += 1;
  });

  if (toAppend.length) await appendRows(toAppend);

  console.log(JSON.stringify({
    file: sourceFileName,
    campaignName,
    total: parsed.total,
    valid: parsed.valid.length,
    invalid: parsed.invalid.length,
    added,
    skipped,
    invalidRows: parsed.invalid.slice(0, 20)
  }, null, 2));
})();
