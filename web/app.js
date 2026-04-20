const state = {
  rows: [],
  templates: [],
  clientTypes: [],
  jobId: "",
  pollTimer: null,
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
  dryRun: document.querySelector("#dryRun"),
  templatesButton: document.querySelector("#templatesButton"),
  templatesDialog: document.querySelector("#templatesDialog"),
  templateType: document.querySelector("#templateType"),
  templateSubject: document.querySelector("#templateSubject"),
  templateBody: document.querySelector("#templateBody"),
  saveTemplate: document.querySelector("#saveTemplate"),
  templateMessage: document.querySelector("#templateMessage"),
};

function normalizeHeader(value) {
  return String(value || "").trim().toLowerCase().replaceAll(" ", "_").replaceAll("-", "_");
}

function normalizeRow(item) {
  const row = { ...item };
  if (!row.custom_note) row.custom_note = row.custome_note || row.customer_note || row.note || row.notes || row.remarks || "";
  if (!row.service) row.service = row.services || row.requirement || row.requirements || row.project || row.project_type || "";
  if (!row.email) row.email = row.email_address || row.email_id || row.mail || row.mail_id || row.recipient_email || "";
  if (!row.client_type) row.client_type = row.clienttype || row.client || row.type || row.category || "";
  if (!row.company) row.company = row.company_name || row.business || row.business_name || "";
  if (!row.name) row.name = row.client_name || row.customer_name || row.full_name || "";
  return row;
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
  if (!rows.length) return [];

  const headers = rows.shift().map(normalizeHeader);
  return rows.map((cells) => {
    const item = {};
    headers.forEach((header, index) => {
      item[header] = cells[index] || "";
    });
    return normalizeRow(item);
  });
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail || "Request failed");
  return data;
}

function templateForType(clientType) {
  const normalized = normalizeClientType(clientType);
  const found = state.templates.find((template) => template.clientType === normalized);
  return found ? found.clientType : "";
}

function renderRows(jobRows = null) {
  const rows = jobRows || state.rows.map((row) => ({
    ...row,
    status: row.status || "ready",
    template: templateForType(row.client_type),
    error: "",
  }));

  els.rowCount.textContent = `${rows.length} rows loaded`;
  els.recipientsBody.innerHTML = rows
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

async function loadInitialData() {
  const [clientTypeData, templateData] = await Promise.all([
    api("/api/client-types"),
    api("/api/templates"),
  ]);
  state.clientTypes = clientTypeData.clientTypes;
  state.templates = templateData.templates;
  els.templateType.innerHTML = state.clientTypes
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
    const data = await api("/api/import-url", {
      method: "POST",
      body: JSON.stringify({ url }),
    });
    state.rows = data.rows;
    els.importMessage.textContent = "Sheet imported.";
    renderRows();
  } catch (error) {
    els.importMessage.textContent = error.message;
  }
}

function importCsvFile() {
  const file = els.csvFile.files[0];
  if (!file) return;
  const extension = file.name.split(".").pop().toLowerCase();

  if (extension === "xlsx" || extension === "xls") {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        if (!window.XLSX) throw new Error("Excel parser did not load. Please refresh and try again.");
        const workbook = XLSX.read(reader.result, { type: "array" });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const csv = XLSX.utils.sheet_to_csv(sheet);
        state.rows = parseCsv(csv);
        els.importMessage.textContent = "Excel file imported.";
        renderRows();
      } catch (error) {
        els.importMessage.textContent = error.message;
      }
    };
    reader.onerror = () => {
      els.importMessage.textContent = "Could not read Excel file.";
    };
    reader.readAsArrayBuffer(file);
    return;
  }

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

async function saveTemplate() {
  const clientType = els.templateType.value;
  els.templateMessage.textContent = "Saving...";
  try {
    const saved = await api(`/api/templates/${encodeURIComponent(clientType)}`, {
      method: "PUT",
      body: JSON.stringify({
        subject: els.templateSubject.value,
        body: els.templateBody.value,
      }),
    });
    state.templates = state.templates.map((template) =>
      template.clientType === saved.clientType ? saved : template
    );
    els.templateMessage.textContent = "Template saved.";
    if (state.rows.length) renderRows();
  } catch (error) {
    els.templateMessage.textContent = error.message;
  }
}

async function sendBulkEmails() {
  if (!state.rows.length) {
    els.importMessage.textContent = "Import recipients before sending.";
    return;
  }
  els.sendEmails.disabled = true;
  els.sendEmails.textContent = "Sending...";
  try {
    const job = await api("/api/send", {
      method: "POST",
      body: JSON.stringify({ rows: state.rows, dryRun: els.dryRun.checked }),
    });
    state.jobId = job.id;
    renderRows(job.rows);
    pollJob();
  } catch (error) {
    els.importMessage.textContent = error.message;
    els.sendEmails.disabled = false;
    els.sendEmails.textContent = "Send Bulk Emails";
  }
}

async function pollJob() {
  if (!state.jobId) return;
  const job = await api(`/api/jobs/${state.jobId}`);
  renderRows(job.rows);
  if (job.state === "finished") {
    els.sendEmails.disabled = false;
    els.sendEmails.textContent = "Send Bulk Emails";
    return;
  }
  state.pollTimer = window.setTimeout(pollJob, 1000);
}

els.loadSheet.addEventListener("click", importSheetUrl);
els.csvFile.addEventListener("change", importCsvFile);
els.templatesButton.addEventListener("click", () => els.templatesDialog.showModal());
els.templateType.addEventListener("change", fillTemplateForm);
els.saveTemplate.addEventListener("click", saveTemplate);
els.sendEmails.addEventListener("click", sendBulkEmails);

loadInitialData().catch((error) => {
  els.importMessage.textContent = error.message;
});
