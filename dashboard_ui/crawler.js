const crawlerState = {
  currentJobId: null,
  pollTimer: null,
};

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function truncate(value, maxLen = 80) {
  const text = String(value || "");
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen - 1) + "...";
}

function stamp(value) {
  return String(value || "-").replace("T", " ").replace("Z", "");
}

function showToast(msg, icon = "info", type = "default") {
  if (typeof window.ksToast === "function") {
    window.ksToast(msg, icon, type);
  }
}

function statusPill(status) {
  const key = String(status || "queued").toLowerCase();
  const map = {
    completed: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300",
    running: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
    queued: "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300",
    failed: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300",
  };
  return `<span class="rounded-full px-2 py-0.5 text-[10px] font-bold ${map[key] || map.queued}">${escapeHtml(key)}</span>`;
}

async function apiRequest(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const raw = await response.text();
  let data;
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = { detail: raw };
  }

  if (!response.ok) {
    throw new Error(data.detail || `${response.status} ${response.statusText}`);
  }
  return data;
}

function buildScrapePayload() {
  return {
    url: el("runUrl").value.trim(),
    depth: Number(el("runDepth").value || 2),
    max_pages: Number(el("runPages").value || 200),
    max_files: Number(el("runFiles").value || 300),
    model: el("runModel").value.trim(),
    selenium_site: el("runSeleniumSite").checked,
    js: el("runJs").checked,
    no_ai: el("runNoAi").checked,
    all_domains: el("runAllDomains").checked,
    api_key: el("runApiKey").value.trim() || null,
  };
}

function setRunButtonLoading(isLoading) {
  const btn = el("runSubmit");
  if (!btn) return;
  btn.disabled = Boolean(isLoading);
  btn.innerHTML = isLoading
    ? '<span class="material-symbols-outlined align-middle text-[16px] animate-spin">refresh</span> Starting...'
    : '<span class="material-symbols-outlined align-middle text-[16px]">play_arrow</span> Start Crawl Job';
}

function updateJobMeta(job) {
  const text =
    `Job ${job.id} · ${job.status} · exit ${job.exit_code ?? "-"} · ` +
    `created ${stamp(job.created_at)} · updated ${stamp(job.updated_at)}`;
  el("jobMeta").textContent = text;
}

function updateJobLog(logs) {
  const target = el("jobLog");
  target.textContent = (logs || []).join("\n") || "No logs yet.";
  target.scrollTop = target.scrollHeight;
}

function stopPolling() {
  if (crawlerState.pollTimer) {
    window.clearInterval(crawlerState.pollTimer);
    crawlerState.pollTimer = null;
  }
}

async function pollJob(jobId) {
  const job = await apiRequest(`/api/jobs/${jobId}`);
  updateJobMeta(job);
  updateJobLog(job.logs || []);

  if (job.status === "completed" || job.status === "failed") {
    stopPolling();
    setRunButtonLoading(false);
    await Promise.all([loadJobs(), loadReports(), loadExtractedSchemes()]);
    showToast(
      job.status === "completed" ? "Crawl job completed" : "Crawl job failed",
      job.status === "completed" ? "check_circle" : "error",
      job.status === "completed" ? "success" : "error",
    );
  }
}

async function startScrapeJob(event) {
  event.preventDefault();
  const payload = buildScrapePayload();
  setRunButtonLoading(true);

  const started = await apiRequest("/api/jobs/scrape", {
    method: "POST",
    body: JSON.stringify(payload),
  });

  crawlerState.currentJobId = started.job_id;
  el("jobMeta").textContent = `Job ${started.job_id} queued.`;
  el("jobLog").textContent = "Waiting for logs...";

  stopPolling();
  crawlerState.pollTimer = window.setInterval(() => {
    pollJob(started.job_id).catch(showError);
  }, 2000);

  await pollJob(started.job_id);
}

async function loadJobs() {
  const data = await apiRequest("/api/jobs?limit=15");
  const rows = data.items || [];
  const body = el("jobRows");

  if (!rows.length) {
    body.innerHTML = '<tr><td class="px-3 py-4 text-gray-500" colspan="4">No crawler jobs yet.</td></tr>';
    el("jobsMeta").textContent = "0 jobs";
    return;
  }

  el("jobsMeta").textContent = `${rows.length} recent jobs`;

  body.innerHTML = rows
    .map((row) => {
      const url = ((row.params || {}).url) || "-";
      return `
        <tr class="border-t border-gray-100 dark:border-gray-800">
          <td class="px-3 py-2 font-semibold">${escapeHtml(row.id || "-")}</td>
          <td class="px-3 py-2">${statusPill(row.status || "queued")}</td>
          <td class="px-3 py-2" title="${escapeHtml(url)}">${escapeHtml(truncate(url, 52))}</td>
          <td class="px-3 py-2 text-gray-500">${escapeHtml(stamp(row.updated_at))}</td>
        </tr>
      `;
    })
    .join("");
}

function renderArtifactLinks(item) {
  const links = [];
  if (item.report_url) {
    links.push(`<a href="${escapeHtml(item.report_url)}" target="_blank" rel="noreferrer" class="text-primary hover:underline">report</a>`);
  }
  if (item.json_url) {
    links.push(`<a href="${escapeHtml(item.json_url)}" target="_blank" rel="noreferrer" class="text-primary hover:underline">json</a>`);
  }
  if (item.csv_url) {
    links.push(`<a href="${escapeHtml(item.csv_url)}" target="_blank" rel="noreferrer" class="text-primary hover:underline">csv</a>`);
  }
  return links.length ? links.join(" · ") : "-";
}

async function loadReports() {
  const data = await apiRequest("/api/reports?limit=20");
  const rows = data.items || [];
  const body = el("reportRows");

  if (!rows.length) {
    body.innerHTML = '<tr><td class="px-3 py-4 text-gray-500" colspan="4">No reports available yet.</td></tr>';
    el("reportsMeta").textContent = "0 reports";
    return;
  }

  el("reportsMeta").textContent = `${rows.length} report entries`;

  body.innerHTML = rows
    .map((row) => {
      const stats = row.stats || {};
      return `
        <tr class="border-t border-gray-100 dark:border-gray-800">
          <td class="px-3 py-2 font-semibold">${escapeHtml(row.task_id || "-")}</td>
          <td class="px-3 py-2">${statusPill(row.status || "queued")}</td>
          <td class="px-3 py-2">${escapeHtml(stats.run_unique_schemes ?? "-")}</td>
          <td class="px-3 py-2">${renderArtifactLinks(row)}</td>
        </tr>
      `;
    })
    .join("");
}

async function loadExtractedSchemes() {
  const data = await apiRequest("/api/master/overview");
  const items = data.recent_updates || [];
  const wrap = el("extractedRows");

  if (!items.length) {
    wrap.innerHTML = '<p class="text-sm text-gray-500">No extracted schemes found yet.</p>';
    el("extractedMeta").textContent = "0 schemes";
    return;
  }

  el("extractedMeta").textContent = `${items.length} recently updated`;
  wrap.innerHTML = items
    .slice(0, 12)
    .map((item) => {
      const score = Number(item.confidence_score || 0);
      const scoreColor = score >= 70 ? "text-emerald-600" : score >= 40 ? "text-amber-600" : "text-red-500";
      return `
        <article class="rounded-xl border border-gray-100 bg-gray-50 p-3 dark:border-gray-800 dark:bg-[#223922]">
          <div class="mb-1 flex items-start justify-between gap-2">
            <p class="text-sm font-semibold leading-snug">${escapeHtml(truncate(item.scheme_name || "Scheme", 64))}</p>
            <span class="text-xs font-black ${scoreColor}">${score}%</span>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-300">${escapeHtml(item.scheme_type || "other")}</p>
          <div class="mt-2 flex items-center justify-between text-[11px] text-gray-500 dark:text-gray-400">
            <span>${escapeHtml(truncate(item.nodal_ministry || "", 30) || "Ministry not listed")}</span>
            <a href="/schemes" class="font-bold text-primary hover:underline">open</a>
          </div>
        </article>
      `;
    })
    .join("");
}

async function loadHealth() {
  try {
    const data = await apiRequest("/api/health");
    if (data.status === "ok") {
      el("apiHealthBadge").className =
        "inline-flex items-center gap-1 rounded-full bg-emerald-100 px-3 py-1 font-semibold text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300";
      el("apiHealthBadge").innerHTML =
        '<span class="material-symbols-outlined text-[14px]">check_circle</span> API Healthy';
    }
  } catch {
    el("apiHealthBadge").className =
      "inline-flex items-center gap-1 rounded-full bg-red-100 px-3 py-1 font-semibold text-red-700 dark:bg-red-900/30 dark:text-red-300";
    el("apiHealthBadge").innerHTML =
      '<span class="material-symbols-outlined text-[14px]">error</span> API Error';
  }
}

function showError(error) {
  const message = (error && error.message) || String(error);
  el("jobMeta").textContent = `Error: ${message}`;
  setRunButtonLoading(false);
  showToast(message, "error", "error");
}

async function bootstrapCrawlerPage() {
  el("runForm").addEventListener("submit", (event) => {
    startScrapeJob(event).catch(showError);
  });

  el("stopPollingBtn").addEventListener("click", () => {
    stopPolling();
    setRunButtonLoading(false);
    el("jobMeta").textContent = "Polling stopped.";
  });

  await Promise.all([loadHealth(), loadJobs(), loadReports(), loadExtractedSchemes()]);
}

bootstrapCrawlerPage().catch(showError);