const fs = require('fs');
const { cleanMultiline } = require('./parse');

function loadTemplate(templateFile) {
  if (!templateFile) throw new Error('template_file is required');
  return JSON.parse(fs.readFileSync(templateFile, 'utf8'));
}

function renderText(template, contact) {
  const rendered = String(template || '')
    .replace(/{{\s*name\s*}}/gi, contact.name || '')
    .replace(/{{\s*email\s*}}/gi, contact.email_id || '')
    .replace(/{{\s*company\s*}}/gi, contact.company_name || '')
    .replace(/{{\s*customLine\s*}}/gi, contact.custom_line || '');

  return cleanMultiline(rendered)
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function renderTemplate(templateObj, contact) {
  return {
    template_name: templateObj.name || 'Unnamed Template',
    subject: renderText(templateObj.subject || '', contact),
    body: renderText(templateObj.body || '', contact)
  };
}

module.exports = { loadTemplate, renderTemplate };
