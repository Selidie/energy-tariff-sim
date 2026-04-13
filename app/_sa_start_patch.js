// ── Import execution ───────────────────────────────────────────────────────

async function saStartImport() {
  if (!_saHasFile) return;

  const rangeStart = document.getElementById('sa-range-start').value.trim();

  document.getElementById('sa-start-btn').disabled = true;
  document.getElementById('sa-clear-btn').style.display = 'none';
  document.getElementById('sa-success-banner').style.display = 'none';
  document.getElementById('sa-error-banner').style.display = 'none';

  // Show progress panel
  document.getElementById('sa-progress-panel').style.display = 'block';
  document.getElementById('sa-counter-num').textContent = '0';
  document.getElementById('sa-progress-label').textContent = 'Importing\u2026';
  document.getElementById('sa-progress-sub').textContent = 'Starting up, please wait';
  document.getElementById('sa-progress-bar').className = '';
  document.getElementById('sa-big-spinner').style.display = '';

  _saStartTime = Date.now();

  // Fire a plain POST to start the import — the server spawns a background
  // thread and returns immediately.  No SSE stream is opened, so there is no
  // persistent HTTP connection adding load during a multi-hour import.
  try {
    const body = rangeStart ? { range_start: rangeStart } : {};
    const r = await fetch('/api/sa-import/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const d = await r.json();
    if (!d.success) {
      saImportFailed(d.error || 'Failed to start import');
      return;
    }
  } catch(e) {
    saImportFailed('Could not reach server: ' + e.message);
    return;
  }

  // Poll /api/sa-import/status every 3 seconds for progress updates
  _saPollTimer = setInterval(saPollStatus, 3000);
  setTimeout(saPollStatus, 500);
}