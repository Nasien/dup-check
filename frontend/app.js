const API_BASE_URL = (window.APP_CONFIG?.API_BASE_URL || '').replace(/\/$/, '');
const TOKEN_KEY = 'dgc_admin_token';
const USER_KEY = 'dgc_admin_user';
const THEME_KEY = 'dgc_theme';

const state = {
  uploads: [],
  currentResults: [],
  currentUpload: null,
  currentUploadId: null,
  options: { hei: [], scholarship: [], academic_year: [], semester: [], batch: [] },
  token: localStorage.getItem(TOKEN_KEY) || '',
  user: JSON.parse(localStorage.getItem(USER_KEY) || 'null'),
};

const el = {
  appShell: document.getElementById('appShell'),
  apiBadge: document.getElementById('apiBadge'),
  statsGrid: document.getElementById('statsGrid'),
  uploadForm: document.getElementById('uploadForm'),
  heiSelect: document.getElementById('heiSelect'),
  scholarshipSelect: document.getElementById('scholarshipSelect'),
  academicYearSelect: document.getElementById('academicYearSelect'),
  semesterSelect: document.getElementById('semesterSelect'),
  batchSelect: document.getElementById('batchSelect'),
  fileInput: document.getElementById('fileInput'),
  uploadMessage: document.getElementById('uploadMessage'),
  recentUploadsBody: document.getElementById('recentUploadsBody'),
  historyBody: document.getElementById('historyBody'),
  historySearch: document.getElementById('historySearch'),
  resultSearch: document.getElementById('resultSearch'),
  duplicatesOnly: document.getElementById('duplicatesOnly'),
  matchTypeFilter: document.getElementById('matchTypeFilter'),
  resultsTitle: document.getElementById('resultsTitle'),
  resultsMeta: document.getElementById('resultsMeta'),
  resultSummaryCards: document.getElementById('resultSummaryCards'),
  matchSummary: document.getElementById('matchSummary'),
  resultsBody: document.getElementById('resultsBody'),
  userPill: document.getElementById('userPill'),
  adminGrid: document.getElementById('adminGrid'),
  loginModal: document.getElementById('loginModal'),
  loginToggle: document.getElementById('loginToggle'),
  loginForm: document.getElementById('loginForm'),
  loginMessage: document.getElementById('loginMessage'),
  sidebarToggle: document.getElementById('sidebarToggle'),
  themeToggle: document.getElementById('themeToggle'),
  matchModal: document.getElementById('matchModal'),
  matchModalBody: document.getElementById('matchModalBody'),
  views: {
    dashboard: document.getElementById('dashboardView'),
    history: document.getElementById('historyView'),
    results: document.getElementById('resultsView'),
    admin: document.getElementById('adminView'),
  },
};

function authHeaders(extra = {}) {
  return state.token ? { ...extra, Authorization: `Bearer ${state.token}` } : extra;
}

function setMessage(text, type = '') {
  el.uploadMessage.textContent = text;
  el.uploadMessage.className = `form-message${type ? ` ${type}` : ''}`;
}

function setLoginMessage(text, type = '') {
  el.loginMessage.textContent = text;
  el.loginMessage.className = `form-message${type ? ` ${type}` : ''}`;
}

function formatDate(value) {
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? value : dt.toLocaleString();
}

function badgeClass(matchType) {
  if (matchType === 'exact_duplicate') return 'badge-danger';
  if (matchType === 'possible_duplicate') return 'badge-info';
  return 'badge-clean';
}

function statusBadge(matchType) {
  if (matchType === 'exact_duplicate') return '<span class="result-badge badge-danger">Exact duplicate</span>';
  if (matchType === 'possible_duplicate') return '<span class="result-badge badge-info">Possible duplicate</span>';
  return '<span class="result-badge badge-clean">Clean</span>';
}

function uploadStatusBadge(status) {
  const cls = status === 'No duplicates found' ? 'badge-clean' : status === 'With duplicates' ? 'badge-danger' : 'badge-info';
  return `<span class="result-badge ${cls}">${status}</span>`;
}

async function api(path, options = {}) {
  const config = { ...options, headers: authHeaders(options.headers || {}) };
  const res = await fetch(`${API_BASE_URL}${path}`, config);
  if (!res.ok) {
    let detail = 'Request failed.';
    try {
      const data = await res.json();
      detail = data.detail || detail;
    } catch (_) {}
    throw new Error(detail);
  }
  if (res.status === 204) return null;
  return res.json();
}

function activateView(view) {
  Object.entries(el.views).forEach(([key, node]) => {
    node.classList.toggle('hidden', key !== view);
  });
  document.querySelectorAll('.nav-link').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.view === view);
  });
}

function openModal(node) {
  node.classList.remove('hidden');
}

function closeModal(node) {
  node.classList.add('hidden');
}

function populateSelect(selectEl, items, placeholder) {
  selectEl.innerHTML = `<option value="">${placeholder}</option>` + items.map((item) => `<option value="${item.name}">${item.name}</option>`).join('');
}

function renderOptions() {
  populateSelect(el.heiSelect, state.options.hei, 'Select HEI');
  populateSelect(el.scholarshipSelect, state.options.scholarship, 'Select Scholarship');
  populateSelect(el.academicYearSelect, state.options.academic_year, 'Select Academic Year');
  populateSelect(el.semesterSelect, state.options.semester, 'Select Semester');
  populateSelect(el.batchSelect, state.options.batch, 'Select Batch');
}

async function loadOptions() {
  state.options = await api('/api/options');
  renderOptions();
}

async function loadStats() {
  const stats = await api('/api/stats');
  el.statsGrid.innerHTML = `
    <div class="stat-card"><span>Total uploads</span><strong>${stats.total_uploads}</strong></div>
    <div class="stat-card"><span>Total grantees</span><strong>${stats.total_grantees}</strong></div>
    <div class="stat-card tone-danger"><span>Flagged duplicates</span><strong>${stats.flagged_duplicates}</strong></div>
    <div class="stat-card tone-info"><span>Possible duplicates</span><strong>${stats.possible_duplicates}</strong></div>
  `;
}

function renderUploadTables(items) {
  const recent = items.slice(0, 5);
  el.recentUploadsBody.innerHTML = recent.length ? recent.map((item) => `
    <tr>
      <td>${item.filename}</td>
      <td>${uploadStatusBadge(item.status)}</td>
      <td>${formatDate(item.uploaded_at)}</td>
      <td><button class="action-link" data-open="${item.id}">View results</button></td>
    </tr>
  `).join('') : '<tr><td colspan="4" class="empty-state">No uploads yet.</td></tr>';

  const needle = el.historySearch.value.trim().toLowerCase();
  const filtered = items.filter((item) => `${item.filename} ${item.status}`.toLowerCase().includes(needle));
  el.historyBody.innerHTML = filtered.length ? filtered.map((item, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${item.filename}</td>
      <td>${uploadStatusBadge(item.status)}</td>
      <td>${formatDate(item.uploaded_at)}</td>
      <td>${item.total_count}</td>
      <td>${item.duplicate_count}</td>
      <td>
        <div class="action-group">
          <button class="action-link" data-open="${item.id}">View</button>
          ${state.user?.role === 'admin' ? `<button class="danger-link" data-delete="${item.id}">Delete</button>` : ''}
        </div>
      </td>
    </tr>
  `).join('') : '<tr><td colspan="7" class="empty-state">No upload history available.</td></tr>';
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
    const rowText = [row.full_name, row.hei, row.scholarship, row.batch, row.duplicate_with_name, row.duplicate_with_batch].join(' ').toLowerCase();
    const duplicateOk = duplicatesOnly ? row.duplicate === 'YES' : true;
    const rowType = row.match_type || 'clean';
    const matchTypeOk = matchType === 'all' ? true : rowType === matchType;
    return duplicateOk && matchTypeOk && rowText.includes(needle);
  });

  el.resultsBody.innerHTML = rows.length ? rows.map((row, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${row.full_name}</td>
      <td>${row.hei}</td>
      <td>${row.scholarship}</td>
      <td>${row.batch}</td>
      <td>${statusBadge(row.match_type)}</td>
      <td>${row.duplicate_with_name || ''}</td>
      <td>${row.duplicate_with_batch || ''}</td>
      <td>${row.match_score || ''}</td>
      <td>${row.duplicate === 'YES' ? `<button class="action-link" data-match="${encodeURIComponent(JSON.stringify(row))}">View match</button>` : ''}</td>
    </tr>
  `).join('') : '<tr><td colspan="10" class="empty-state">No matching rows found.</td></tr>';
}

function openMatchModal(row) {
  el.matchModalBody.innerHTML = `
    <div class="compare-card">
      <h4>Uploaded Record</h4>
      <dl>
        <dt>Full name</dt><dd>${row.full_name}</dd>
        <dt>HEI</dt><dd>${row.hei}</dd>
        <dt>Scholarship</dt><dd>${row.scholarship}</dd>
        <dt>Academic Year</dt><dd>${row.academic_year}</dd>
        <dt>Semester</dt><dd>${row.semester}</dd>
        <dt>Batch</dt><dd>${row.batch}</dd>
      </dl>
    </div>
    <div class="compare-card ${badgeClass(row.match_type)}-card">
      <h4>Matched Record</h4>
      <dl>
        <dt>Full name</dt><dd>${row.duplicate_with_name || ''}</dd>
        <dt>HEI</dt><dd>${row.duplicate_with_hei || ''}</dd>
        <dt>Scholarship</dt><dd>${row.duplicate_with_scholarship || ''}</dd>
        <dt>Batch</dt><dd>${row.duplicate_with_batch || ''}</dd>
        <dt>Match type</dt><dd>${row.match_type || ''}</dd>
        <dt>Score</dt><dd>${row.match_score || ''}</dd>
      </dl>
    </div>
  `;
  openModal(el.matchModal);
}

async function openResults(uploadId) {
  const data = await api(`/api/uploads/${uploadId}`);
  state.currentUploadId = uploadId;
  state.currentUpload = data.upload;
  state.currentResults = data.rows;
  el.resultsTitle.textContent = data.upload.filename;
  el.resultsMeta.textContent = `${data.upload.status} · Uploaded ${formatDate(data.upload.uploaded_at)}`;
  el.resultSummaryCards.innerHTML = `
    <div class="stat-card"><span>Total rows</span><strong>${data.upload.total_count}</strong></div>
    <div class="stat-card tone-danger"><span>Flagged duplicates</span><strong>${data.upload.duplicate_count}</strong></div>
    <div class="stat-card tone-info"><span>Possible duplicates</span><strong>${data.upload.possible_duplicates}</strong></div>
    <div class="stat-card"><span>Batch</span><strong>${data.upload.batch}</strong></div>
  `;
  el.matchSummary.innerHTML = `
    <span class="pill">Clean: ${data.summary.clean || 0}</span>
    <span class="pill danger">Exact duplicate: ${data.summary.exact_duplicate || 0}</span>
    <span class="pill info">Possible duplicate: ${data.summary.possible_duplicate || 0}</span>
  `;
  el.duplicatesOnly.checked = true;
  renderResultsRows();
  activateView('results');
}

async function deleteUpload(uploadId) {
  if (!window.confirm('Delete this upload and its results?')) return;
  await api(`/api/uploads/${uploadId}`, { method: 'DELETE' });
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
  formData.append('hei', el.heiSelect.value);
  formData.append('scholarship', el.scholarshipSelect.value);
  formData.append('academic_year', el.academicYearSelect.value);
  formData.append('semester', el.semesterSelect.value);
  formData.append('batch', el.batchSelect.value);

  try {
    const data = await api('/api/uploads', { method: 'POST', body: formData });
    setMessage(`Upload completed. Status: ${data.status}`, 'success');
    el.uploadForm.reset();
    await Promise.all([loadStats(), loadUploads()]);
    await openResults(data.upload_id);
  } catch (error) {
    setMessage(error.message, 'error');
  }
}

function renderUserState() {
  const isAdmin = state.user?.role === 'admin';
  document.querySelectorAll('.admin-only').forEach((node) => node.classList.toggle('hidden', !isAdmin));
  el.userPill.classList.toggle('hidden', !isAdmin);
  if (isAdmin) {
    el.userPill.textContent = `Admin: ${state.user.username}`;
    el.loginToggle.textContent = 'Sign out';
  } else {
    el.userPill.textContent = '';
    el.loginToggle.textContent = 'Admin Login';
  }
  renderUploadTables(state.uploads);
}

async function loadAdminOptions() {
  if (state.user?.role !== 'admin') return;
  const data = await api('/api/admin/options');
  el.adminGrid.innerHTML = Object.entries(data).map(([category, items]) => `
    <div class="panel admin-card">
      <div class="panel-head compact-head">
        <div>
          <span class="eyebrow">${category.replace('_', ' ')}</span>
          <h3>${category.replace('_', ' ').toUpperCase()}</h3>
        </div>
      </div>
      <form class="inline-form" data-admin-form="${category}">
        <input class="search-input" name="name" placeholder="Add ${category.replace('_', ' ')}" required />
        <button class="btn" type="submit">Add</button>
      </form>
      <div class="option-list">
        ${items.map((item) => `<div class="option-row"><span>${item.name}</span><button class="action-link" data-toggle-option="${category}" data-option-id="${item.id}">${item.is_active ? 'Deactivate' : 'Activate'}</button></div>`).join('') || '<div class="empty-state">No options yet.</div>'}
      </div>
    </div>
  `).join('');
}

async function handleLogin(event) {
  event.preventDefault();
  const formData = new FormData();
  formData.append('username', document.getElementById('loginUsername').value);
  formData.append('password', document.getElementById('loginPassword').value);
  try {
    const res = await fetch(`${API_BASE_URL}/api/auth/login`, { method: 'POST', body: formData });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Login failed.');
    state.token = data.token;
    state.user = data.user;
    localStorage.setItem(TOKEN_KEY, state.token);
    localStorage.setItem(USER_KEY, JSON.stringify(state.user));
    setLoginMessage('Login successful.', 'success');
    renderUserState();
    await loadAdminOptions();
    closeModal(el.loginModal);
  } catch (error) {
    setLoginMessage(error.message, 'error');
  }
}

function signOut() {
  state.token = '';
  state.user = null;
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
  renderUserState();
  activateView('dashboard');
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem(THEME_KEY, theme);
}

function initTheme() {
  applyTheme(localStorage.getItem(THEME_KEY) || 'dark');
}

function attachEvents() {
  document.querySelectorAll('.nav-link').forEach((btn) => {
    btn.addEventListener('click', () => {
      if (btn.dataset.view === 'admin' && state.user?.role !== 'admin') return;
      activateView(btn.dataset.view);
    });
  });

  el.sidebarToggle.addEventListener('click', () => el.appShell.classList.toggle('sidebar-collapsed'));
  el.themeToggle.addEventListener('click', () => {
    const current = document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
    applyTheme(current === 'light' ? 'dark' : 'light');
  });

  el.loginToggle.addEventListener('click', async () => {
    if (state.user?.role === 'admin') {
      signOut();
      return;
    }
    setLoginMessage('');
    openModal(el.loginModal);
  });
  el.loginForm.addEventListener('submit', handleLogin);

  document.querySelectorAll('[data-close-modal]').forEach((btn) => {
    btn.addEventListener('click', () => closeModal(document.getElementById(btn.dataset.closeModal)));
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
      return;
    }
    const matchBtn = event.target.closest('[data-match]');
    if (matchBtn) {
      openMatchModal(JSON.parse(decodeURIComponent(matchBtn.dataset.match))); 
      return;
    }
    const toggleOptionBtn = event.target.closest('[data-toggle-option]');
    if (toggleOptionBtn) {
      api(`/api/admin/options/${toggleOptionBtn.dataset.toggleOption}/${toggleOptionBtn.dataset.optionId}/toggle`, { method: 'POST' })
        .then(async () => { await Promise.all([loadOptions(), loadAdminOptions()]); })
        .catch((error) => alert(error.message));
    }
  });

  document.body.addEventListener('submit', (event) => {
    const form = event.target.closest('[data-admin-form]');
    if (!form) return;
    event.preventDefault();
    const category = form.dataset.adminForm;
    const value = form.querySelector('input[name="name"]').value.trim();
    const body = new FormData();
    body.append('name', value);
    api(`/api/admin/options/${category}`, { method: 'POST', body })
      .then(async () => {
        form.reset();
        await Promise.all([loadOptions(), loadAdminOptions()]);
      })
      .catch((error) => alert(error.message));
  });
}

async function init() {
  initTheme();
  try {
    await api('/api/healthz');
    el.apiBadge.textContent = 'API connected';
  } catch (_) {
    el.apiBadge.textContent = 'API unreachable';
    el.apiBadge.classList.add('danger');
  }
  attachEvents();
  await Promise.all([loadOptions(), loadStats(), loadUploads()]);
  renderUserState();
  if (state.user?.role === 'admin') await loadAdminOptions();
  activateView('dashboard');
}

init().catch((error) => {
  console.error(error);
  setMessage(error.message, 'error');
});
