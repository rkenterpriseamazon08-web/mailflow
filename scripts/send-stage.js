const { getAllRows, updateRow, initHeaderRow } = require('./common/google');
const { loadTemplate, renderTemplate } = require('./common/template');
const { sendMail } = require('./common/smtp');

const mode = process.argv[2];
const campaignName = process.env.CAMPAIGN_NAME;
const templateFile = process.env.TEMPLATE_FILE;
const followupStage = Number(process.env.FOLLOWUP_STAGE || '1');
const retryFailed = String(process.env.RETRY_FAILED || 'false') === 'true';
const sendDelayMs = Number(process.env.SEND_DELAY_MS || '1500');

if (!campaignName) throw new Error('CAMPAIGN_NAME is required');
if (!templateFile) throw new Error('TEMPLATE_FILE is required');
if (!['initial', 'followup'].includes(mode)) throw new Error('Mode must be initial or followup');
if (mode === 'followup' && (followupStage < 1 || followupStage > 10)) throw new Error('FOLLOWUP_STAGE must be 1-10');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function eligibleInitial(row) {
  if ((row.campaign_name || '') !== campaignName) return false;
  const status = row.status || 'Pending';
  if (status === 'Sent') return false;
  if (status === 'Failed' && retryFailed) return true;
  return status === 'Pending' || status === '';
}

function eligibleFollowup(row) {
  if ((row.campaign_name || '') !== campaignName) return false;
  if ((row.status || '') !== 'Sent') return false;
  const stageStatus = row[`follow_up_${followupStage}_status`] || '';
  if (stageStatus === 'Sent') return false;
  if (stageStatus === 'Failed' && !retryFailed) return false;
  if (followupStage === 1) return true;
  return (row[`follow_up_${followupStage - 1}_status`] || '') === 'Sent';
}

(async () => {
  await initHeaderRow();
  const template = loadTemplate(templateFile);
  const { rows } = await getAllRows();
  const eligible = rows.filter(mode === 'initial' ? eligibleInitial : eligibleFollowup);

  console.log(`Found ${eligible.length} eligible contacts for ${mode} in campaign ${campaignName}`);

  let sent = 0;
  let failed = 0;
  let skipped = 0;

  for (const row of eligible) {
    const now = new Date().toISOString();
    try {
      const rendered = renderTemplate(template, row);
      if (!row.email_id) {
        skipped += 1;
        continue;
      }

      await sendMail({ to: row.email_id, subject: rendered.subject, body: rendered.body });

      if (mode === 'initial') {
        row.status = 'Sent';
        row.template_name = rendered.template_name;
        row.sent_at = now;
        row.error_message = '';
      } else {
        row[`follow_up_${followupStage}_status`] = 'Sent';
        row[`follow_up_${followupStage}_sent_at`] = now;
        row[`follow_up_${followupStage}_template_name`] = rendered.template_name;
        row[`follow_up_${followupStage}_error_message`] = '';
      }
      row.updated_at = now;
      await updateRow(row.__rowIndex, row);
      sent += 1;
      console.log(`Sent: ${row.email_id}`);
    } catch (err) {
      const nowFailed = new Date().toISOString();
      if (mode === 'initial') {
        row.status = 'Failed';
        row.error_message = err.message;
        row.template_name = template.name || 'Unnamed Template';
      } else {
        row[`follow_up_${followupStage}_status`] = 'Failed';
        row[`follow_up_${followupStage}_error_message`] = err.message;
        row[`follow_up_${followupStage}_template_name`] = template.name || 'Unnamed Template';
      }
      row.updated_at = nowFailed;
      await updateRow(row.__rowIndex, row);
      failed += 1;
      console.log(`Failed: ${row.email_id} -> ${err.message}`);
    }
    await sleep(sendDelayMs);
  }

  console.log(JSON.stringify({ mode, campaignName, followupStage: mode === 'followup' ? followupStage : null, sent, failed, skipped, eligible: eligible.length }, null, 2));
})();
