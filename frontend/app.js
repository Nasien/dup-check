const API_BASE_URL = (window.APP_CONFIG?.API_BASE_URL || '').replace(/\/$/, '');

const state = {
  uploads: [],
  currentResults: [],
  currentUploadId: null,
};

const el = {
  apiBadge: document.getElementById('apiBadge'),
  statsGrid: document.getElementById('statsGrid'),
  uploadForm: document.getElementById('uploadForm'),
  fileInput: document.getElementById('fileInput'),
  uploadMessage: document.getElementById('uploadMessage'),
  recentUploadsBody: document.getElementById('recentUploadsBody'),
  historyBody: document.getElementById('historyBody'),
  historySearch: document.getElementById('historySearch'),
  resultSearch: document.getElementById('resultSearch'),
  duplicatesOnly: document.getElementById('duplicatesOnly'),
  matchTypeFilter: document.getElementById('matchTypeFilter'),
  resultsTitle: document.getElementById('resultsTitle'),
  resultSummaryCards: document.getElementById('resultSummaryCards'),
  matchSummary: document.getElementById('matchSummary'),
  resultsBody: document.getElementById('resultsBody'),
  views: {
    dashboard: document.getElementById('dashboardView'),
    history: document.getElementById('historyView'),
    results: document.getElementById('resultsView'),
  }
};

function setMessage(text, type = '') {
  el.uploadMessage.textContent = text;
  el.uploadMessage.className = `form-message${type ? ` ${type}` : ''}`;
}

function activateView(view) {
  Object.entries(el.views).forEach(([key, node]) => {
    node.classList.toggle('hidden', key !== view);
  });
  document.querySelectorAll('.nav-link').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.view === view);
  });
}

function formatDate(value) {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? value : dt.toLocaleString();
}

function statusBadge(matchType) {
  if (matchType === 'cross_program_exact') return `<span class="result-badge badge-warning">Cross-program exact</span>`;
  if (matchType === 'exact_duplicate') return `<span class="result-badge badge-danger">Exact duplicate</span>`;
  if (matchType === 'possible_duplicate') return `<span class="result-badge badge-info">Possible duplicate</span>`;
  return `<span class="result-badge badge-clean">Clean</span>`;
}

async function api(path, options = {}) {
  const res = await fetch(`${API_BASE_URL}${path}`, options);
  if (!res.ok) {
    let detail = 'Request failed.';
    try {
      const data = await res.json();
      detail = data.detail || detail;
    } catch (_) {}
    throw new Error(detail);
  }
  return res.json();
}

async function loadStats() {
  const stats = await api('/api/stats');
  el.statsGrid.innerHTML = `
    <div class="stat-card"><span>Total uploads</span><strong>${stats.total_uploads}</strong></div>
    <div class="stat-card"><span>Total grantees</span><strong>${stats.total_grantees}</strong></div>
    <div class="stat-card tone-danger"><span>Flagged duplicates</span><strong>${stats.flagged_duplicates}</strong></div>
    <div class="stat-card tone-warning"><span>Cross-program exact</span><strong>${stats.cross_program_exact}</strong></div>
  `;
}

function renderUploadTables(items) {
  const recent = items.slice(0, 5);
  el.recentUploadsBody.innerHTML = recent.length ? recent.map((item) => `
    <tr>
      <td>${item.filename}</td>
      <td>${formatDate(item.uploaded_at)}</td>
      <td><button class="action-link" data-open="${item.id}">View results</button></td>
    </tr>
  `).join('') : '<tr><td colspan="3" class="empty-state">No uploads yet.</td></tr>';

  const filtered = items.filter((item) => item.filename.toLowerCase().includes(el.historySearch.value.trim().toLowerCase()));
  el.historyBody.innerHTML = filtered.length ? filtered.map((item, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${item.filename}</td>
      <td>${formatDate(item.uploaded_at)}</td>
      <td>${item.total_count}</td>
      <td>${item.duplicate_count}</td>
      <td>
        <div class="action-group">
          <button class="action-link" data-open="${item.id}">View</button>
          <button class="danger-link" data-delete="${item.id}">Delete</button>
        </div>
      </td>
    </tr>
  `).join('') : '<tr><td colspan="6" class="empty-state">No upload history available.</td></tr>';
}

async function loadUploads() {
  state.uploads = await api('/api/uploads');
  renderUploadTables(state.uploads);
}

function renderResultsRows() {
  const needle = el.resultSearch.value.trim().toLowerCase();
  const duplicatesOnly = el.duplicatesOnly.checked;
  const matchType = el.matchTypeFilter.value;

  const rows = state.currentResults.filter((row) => {
    const rowText = [row.full_name, row.hei, row.academic_year, row.semester, row.scholarship, row.duplicate_with_name, row.duplicate_with_hei, row.duplicate_with_scholarship].join(' ').toLowerCase();
    const duplicateOk = duplicatesOnly ? row.duplicate === 'YES' : true;
    const matchTypeOk = matchType === 'all' ? true : (row.match_type || 'clean') === matchType;
    return duplicateOk && matchTypeOk && rowText.includes(needle);
  });

  el.resultsBody.innerHTML = rows.length ? rows.map((row, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${row.full_name}</td>
      <td>${row.hei || ''}</td>
      <td>${row.academic_year}</td>
      <td>${row.semester}</td>
      <td>${row.scholarship}</td>
      <td>${statusBadge(row.match_type)}</td>
      <td>${row.duplicate_with_name || ''}</td>
      <td>${row.duplicate_with_hei || ''}</td>
      <td>${row.duplicate_with_scholarship || ''}</td>
      <td>${row.match_score || ''}</td>
    </tr>
  `).join('') : '<tr><td colspan="11" class="empty-state">No matching rows found.</td></tr>';
}

async function openResults(uploadId) {
  const data = await api(`/api/uploads/${uploadId}`);
  state.currentUploadId = uploadId;
  state.currentResults = data.rows;
  el.resultsTitle.textContent = data.upload.filename;
  const summary = {
    clean: data.summary.clean || 0,
    cross_program_exact: data.summary.cross_program_exact || 0,
    exact_duplicate: data.summary.exact_duplicate || 0,
    possible_duplicate: data.summary.possible_duplicate || 0,
  };
  el.resultSummaryCards.innerHTML = `
    <div class="stat-card"><span>Total rows</span><strong>${data.upload.total_count}</strong></div>
    <div class="stat-card tone-danger"><span>Flagged duplicates</span><strong>${data.upload.duplicate_count}</strong></div>
    <div class="stat-card tone-warning"><span>Cross-program exact</span><strong>${data.upload.cross_program_exact}</strong></div>
    <div class="stat-card tone-info"><span>Possible duplicates</span><strong>${data.upload.possible_duplicates}</strong></div>
  `;
  el.matchSummary.innerHTML = `
    <span class="pill">Clean: ${summary.clean}</span>
    <span class="pill warning">Cross-program exact: ${summary.cross_program_exact}</span>
    <span class="pill danger">Exact duplicate: ${summary.exact_duplicate}</span>
    <span class="pill info">Possible duplicate: ${summary.possible_duplicate}</span>
  `;
  renderResultsRows();
  activateView('results');
}

async function deleteUpload(uploadId) {
  const confirmed = window.confirm('Delete this upload and its results?');
  if (!confirmed) return;
  await api(`/api/uploads/${uploadId}`, { method: 'DELETE' });
  if (state.currentUploadId === uploadId) {
    state.currentUploadId = null;
    state.currentResults = [];
  }
  await Promise.all([loadStats(), loadUploads()]);
  activateView('history');
}

async function submitUpload(event) {
  event.preventDefault();
  const file = el.fileInput.files[0];
  if (!file) {
    setMessage('Please choose an Excel file first.', 'error');
    return;
  }
  setMessage('Uploading and checking duplicates...');
  const formData = new FormData();
  formData.append('file', file);

  try {
    const data = await api('/api/uploads', { method: 'POST', body: formData });
    setMessage('Upload completed.', 'success');
    el.uploadForm.reset();
    await Promise.all([loadStats(), loadUploads()]);
    await openResults(data.upload_id);
  } catch (error) {
    setMessage(error.message, 'error');
  }
}

function attachEvents() {
  document.querySelectorAll('.nav-link').forEach((btn) => {
    btn.addEventListener('click', () => activateView(btn.dataset.view));
  });

  el.uploadForm.addEventListener('submit', submitUpload);
  el.historySearch.addEventListener('input', () => renderUploadTables(state.uploads));
  el.resultSearch.addEventListener('input', renderResultsRows);
  el.duplicatesOnly.addEventListener('change', renderResultsRows);
  el.matchTypeFilter.addEventListener('change', renderResultsRows);

  document.body.addEventListener('click', (event) => {
    const openBtn = event.target.closest('[data-open]');
    if (openBtn) {
      openResults(openBtn.dataset.open).catch((error) => alert(error.message));
      return;
    }
    const deleteBtn = event.target.closest('[data-delete]');
    if (deleteBtn) {
      deleteUpload(deleteBtn.dataset.delete).catch((error) => alert(error.message));
    }
  });
}

async function init() {
  try {
    await api('/api/healthz');
    el.apiBadge.textContent = 'API connected';
  } catch (_) {
    el.apiBadge.textContent = 'API unreachable';
    el.apiBadge.classList.add('danger');
  }

  attachEvents();
  await Promise.all([loadStats(), loadUploads()]);
  activateView('dashboard');
}

init().catch((error) => {
  console.error(error);
  setMessage(error.message, 'error');
});
