const clientTypes = ["storage", "office", "cafe", "house", "public toilet", "security cabin"];

const defaultTemplates = [
  {
    clientType: "storage",
    subject: "Storage requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>I noticed that {{ company }} may need support with {{ service }}.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>Would you be open to a quick conversation this week?</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
  {
    clientType: "office",
    subject: "Office requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>I wanted to check whether {{ company }} needs help with {{ service }}.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>If useful, I can share a simple next step.</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
  {
    clientType: "cafe",
    subject: "Cafe requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>Thanks for showing interest in {{ service }}. I wanted to share a quick note for {{ company }}.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>If it helps, I can send a short proposal or discuss the next step.</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
  {
    clientType: "house",
    subject: "House requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>I hope things are going well at {{ company }}.</p>\n\n<p>I wanted to check if you need any help with {{ service }} or related support.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
  {
    clientType: "public toilet",
    subject: "Public toilet requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>I wanted to check whether {{ company }} needs support with {{ service }}.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
  {
    clientType: "security cabin",
    subject: "Security cabin requirement for {{ company }}",
    body: "<p>Hi {{ name }},</p>\n\n<p>I wanted to check whether {{ company }} needs support with {{ service }}.</p>\n\n<p>{{ custom_note }}</p>\n\n<p>Regards,<br>{{ from_name }}</p>",
  },
];

const state = {
  rows: [],
  templates: loadTemplates(),
};

const els = {
  sheetUrl: document.querySelector("#sheetUrl"),
  loadSheet: document.querySelector("#loadSheet"),
  csvFile: document.querySelector("#csvFile"),
  importMessage: document.querySelector("#importMessage"),
  tablePanel: document.querySelector("#tablePanel"),
  rowCount: document.querySelector("#rowCount"),
  recipientsBody: document.querySelector("#recipientsBody"),
  sendEmails: document.querySelector("#sendEmails"),
  templatesButton: document.querySelector("#templatesButton"),
  templatesDialog: document.querySelector("#templatesDialog"),
  templateType: document.querySelector("#templateType"),
  templateSubject: document.querySelector("#templateSubject"),
  templateBody: document.querySelector("#templateBody"),
  saveTemplate: document.querySelector("#saveTemplate"),
  templateMessage: document.querySelector("#templateMessage"),
};

function loadTemplates() {
  const saved = localStorage.getItem("mailflowTemplates");
  if (!saved) return defaultTemplates;
  try {
    return JSON.parse(saved);
  } catch {
    return defaultTemplates;
  }
}

function saveTemplates() {
  localStorage.setItem("mailflowTemplates", JSON.stringify(state.templates));
}

function normalizeHeader(value) {
  return String(value || "").trim().toLowerCase().replaceAll(" ", "_").replaceAll("-", "_");
}

function normalizeClientType(value) {
  const normalized = String(value || "").trim().toLowerCase().replaceAll("_", " ").replaceAll("-", " ");
  if (normalized === "security") return "security cabin";
  if (normalized === "toilet") return "public toilet";
  return normalized;
}

function parseCsv(text) {
  const rows = [];
  let current = "";
  let row = [];
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      index += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === "," && !inQuotes) {
      row.push(current);
      current = "";
    } else if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(current);
      if (row.some((cell) => cell.trim() !== "")) rows.push(row);
      row = [];
      current = "";
    } else {
      current += char;
    }
  }

  row.push(current);
  if (row.some((cell) => cell.trim() !== "")) rows.push(row);

  const headers = rows.shift().map(normalizeHeader);
  return rows.map((cells) => {
    const item = {};
    headers.forEach((header, index) => {
      item[header] = cells[index] || "";
    });
    item.status = item.status || "ready";
    item.error = "";
    item.template = templateForType(item.client_type);
    return item;
  });
}

function templateForType(clientType) {
  const normalized = normalizeClientType(clientType);
  const found = state.templates.find((template) => template.clientType === normalized);
  return found ? found.clientType : "";
}

function renderTemplate(text, row) {
  return String(text || "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, key) => {
    if (key === "from_name") return "Mailflow";
    return row[key] || "";
  });
}

function renderRows() {
  els.rowCount.textContent = `${state.rows.length} rows loaded`;
  els.recipientsBody.innerHTML = state.rows
    .map((row) => {
      const status = row.status || "ready";
      return `
        <tr>
          <td>${escapeHtml(row.name || "")}</td>
          <td>${escapeHtml(row.email || "")}</td>
          <td>${escapeHtml(row.client_type || "")}</td>
          <td>${escapeHtml(row.company || "")}</td>
          <td>${escapeHtml(row.template || templateForType(row.client_type) || "")}</td>
          <td class="status-${escapeHtml(status)}">${escapeHtml(status)}</td>
          <td>${escapeHtml(row.error || "")}</td>
        </tr>
      `;
    })
    .join("");
  els.tablePanel.classList.remove("hidden");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fillTemplateOptions() {
  els.templateType.innerHTML = clientTypes
    .map((type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`)
    .join("");
  fillTemplateForm();
}

function fillTemplateForm() {
  const type = els.templateType.value;
  const template = state.templates.find((item) => item.clientType === type) || {
    subject: "",
    body: "",
  };
  els.templateSubject.value = template.subject;
  els.templateBody.value = template.body;
  els.templateMessage.textContent = "";
}

async function importSheetUrl() {
  const url = els.sheetUrl.value.trim();
  if (!url) {
    els.importMessage.textContent = "Paste a Google Sheet CSV link first.";
    return;
  }
  els.importMessage.textContent = "Loading sheet...";
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error("Could not read the CSV link.");
    const text = await response.text();
    state.rows = parseCsv(text);
    els.importMessage.textContent = "Sheet imported.";
    renderRows();
  } catch (error) {
    els.importMessage.textContent = `${error.message} If the link is blocked, download the sheet as CSV and upload it here.`;
  }
}

function importCsvFile() {
  const file = els.csvFile.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    state.rows = parseCsv(String(reader.result || ""));
    els.importMessage.textContent = "CSV imported.";
    renderRows();
  };
  reader.onerror = () => {
    els.importMessage.textContent = "Could not read CSV file.";
  };
  reader.readAsText(file);
}

function saveTemplate() {
  const clientType = els.templateType.value;
  const subject = els.templateSubject.value.trim();
  const body = els.templateBody.value.trim();
  if (!subject || !body) {
    els.templateMessage.textContent = "Subject and body are required.";
    return;
  }
  state.templates = state.templates.map((template) =>
    template.clientType === clientType ? { clientType, subject, body } : template
  );
  saveTemplates();
  state.rows = state.rows.map((row) => ({ ...row, template: templateForType(row.client_type) }));
  renderRows();
  els.templateMessage.textContent = "Template saved in this browser.";
}

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

async function sendBulkEmails() {
  if (!state.rows.length) {
    els.importMessage.textContent = "Import recipients before sending.";
    return;
  }

  els.sendEmails.disabled = true;
  els.sendEmails.textContent = "Previewing...";

  for (let index = 0; index < state.rows.length; index += 1) {
    const row = state.rows[index];
    row.status = "previewing";
    row.error = "";
    renderRows();
    await wait(350);

    if (String(row.do_not_email || "").trim().toLowerCase() === "yes") {
      row.status = "skipped";
      row.error = "do_not_email is yes";
      renderRows();
      continue;
    }

    const template = state.templates.find((item) => item.clientType === normalizeClientType(row.client_type));
    if (!template) {
      row.status = "skipped";
      row.error = "No matching template";
      renderRows();
      continue;
    }

    row.template = template.clientType;
    row.subject = renderTemplate(template.subject, row);
    row.status = "previewed";
    renderRows();
  }

  els.sendEmails.disabled = false;
  els.sendEmails.textContent = "Preview Bulk Emails";
}

els.loadSheet.addEventListener("click", importSheetUrl);
els.csvFile.addEventListener("change", importCsvFile);
els.templatesButton.addEventListener("click", () => els.templatesDialog.showModal());
els.templateType.addEventListener("change", fillTemplateForm);
els.saveTemplate.addEventListener("click", saveTemplate);
els.sendEmails.addEventListener("click", sendBulkEmails);

fillTemplateOptions();
