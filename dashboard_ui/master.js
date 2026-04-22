const masterState = {
  currentOffset: 0,
  pageLimit: 10,
  totalRows: 0,
  loading: false,
  requestSeq: 0,
  charts: {},
  bootstrapped: false,
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

function fmt(value, fallback = "-") {
  const text = String(value || "").trim();
  return text || fallback;
}

function truncate(value, maxLen = 76) {
  const text = String(value || "");
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 1)}...`;
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
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

function destroyChart(canvasId) {
  const existing = masterState.charts[canvasId];
  if (existing) {
    existing.destroy();
    delete masterState.charts[canvasId];
  }
}

function renderChart(canvasId, config) {
  if (typeof window.Chart === "undefined") return;

  const canvas = el(canvasId);
  if (!canvas) return;

  destroyChart(canvasId);
  masterState.charts[canvasId] = new window.Chart(canvas.getContext("2d"), config);
}

function renderMatrixRows(targetId, rows, nameKey) {
  const body = el(targetId);
  if (!body) return;

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="2">No matrix data.</td></tr>`;
    return;
  }

  body.innerHTML = rows
    .map((row) => {
      return `
        <tr>
          <td title="${escapeHtml(row[nameKey] || "Unknown")}">${escapeHtml(truncate(row[nameKey] || "Unknown", 44))}</td>
          <td>${escapeHtml(row.count)}</td>
        </tr>
      `;
    })
    .join("");
}

function renderMasterMinistryChart(rows) {
  const labels = rows.map((row) => String(row.nodal_ministry || "Unknown"));
  const values = rows.map((row) => toNumber(row.count));

  renderChart("masterMinistryChart", {
    type: "bar",
    data: {
      labels: labels.length ? labels : ["no-data"],
      datasets: [
        {
          label: "Schemes",
          data: values.length ? values : [0],
          backgroundColor: "rgba(47, 122, 68, 0.75)",
          borderRadius: 8,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: { beginAtZero: true },
      },
    },
  });
}

function renderMasterTypeChart(rows) {
  const labels = rows.map((row) => String(row.scheme_type || "other"));
  const values = rows.map((row) => toNumber(row.count));

  renderChart("masterTypeChart", {
    type: "bar",
    data: {
      labels: labels.length ? labels : ["no-data"],
      datasets: [
        {
          label: "Schemes",
          data: values.length ? values : [0],
          backgroundColor: "rgba(207, 127, 47, 0.78)",
          borderRadius: 8,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
      },
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

async function refreshMasterMetrics() {
  const data = await apiRequest("/api/master/overview");
  const totals = data.totals || {};
  const topMinistries = (data.top_ministries || []).slice(0, 8);
  const topTypes = (data.top_types || []).slice(0, 8);

  el("masterMetricTotal").textContent = totals.master_schemes ?? "-";
  el("masterMetricCurated").textContent = totals.curated_master ?? "-";
  el("masterMetricSources").textContent = totals.source_links ?? "-";
  el("masterMetricMedia").textContent = totals.media_links ?? "-";
  el("masterMetricVersions").textContent = totals.versions ?? "-";

  renderMatrixRows("masterMinistryRows", topMinistries, "nodal_ministry");
  renderMatrixRows("masterTypeRows", topTypes, "scheme_type");
  renderMasterMinistryChart(topMinistries);
  renderMasterTypeChart(topTypes);
}

function applyScopeDefaults() {
  const scope = el("masterQueryScope").value;
  const minScoreInput = el("masterQueryMinScore");
  const current = Number(minScoreInput.value || "0");

  if (scope === "curated" || scope === "strict") {
    if (current < 45) minScoreInput.value = "45";
    return;
  }

  if ((scope === "trusted" || scope === "balanced" || scope === "all" || scope === "raw") && current === 45) {
    minScoreInput.value = "0";
  }
}

function buildSearchParams() {
  const params = new URLSearchParams();
  const minScoreInput = el("masterQueryMinScore");
  const limitInput = el("masterQueryLimit");

  const minScore = Math.max(0, Math.min(100, Number(minScoreInput.value || "0")));
  const pageLimit = Math.max(1, Math.min(300, Number(limitInput.value || "30")));

  minScoreInput.value = String(minScore);
  limitInput.value = String(pageLimit);

  params.set("query", el("masterQueryText").value.trim());
  params.set("ministry", el("masterQueryMinistry").value.trim());
  params.set("scheme_type", el("masterQueryType").value.trim());
  params.set("mode", el("masterQueryMode").value);
  params.set("scope", el("masterQueryScope").value);
  params.set("min_score", String(minScore));

  masterState.pageLimit = pageLimit;
  params.set("limit", String(pageLimit));
  params.set("offset", String(Math.max(0, masterState.currentOffset)));
  return params;
}

async function runMasterSearch(event) {
  if (event) {
    event.preventDefault();
    masterState.currentOffset = 0;
  }

  const requestId = ++masterState.requestSeq;
  masterState.loading = true;
  renderPager();

  const params = buildSearchParams();
  try {
    const data = await apiRequest(`/api/master/search?${params.toString()}`);
    if (requestId !== masterState.requestSeq) return;

    masterState.totalRows = Number(data.total || 0);
    masterState.currentOffset = Number(data.offset || 0);
    masterState.pageLimit = Number(data.limit || masterState.pageLimit || 10);

    // If filters reduced total rows and offset moved past last page, snap to last valid page.
    if ((data.items || []).length === 0 && masterState.totalRows > 0 && masterState.currentOffset >= masterState.totalRows) {
      masterState.currentOffset = Math.max(0, Math.floor((masterState.totalRows - 1) / masterState.pageLimit) * masterState.pageLimit);
      const retryParams = buildSearchParams();
      const retryData = await apiRequest(`/api/master/search?${retryParams.toString()}`);
      if (requestId !== masterState.requestSeq) return;

      masterState.totalRows = Number(retryData.total || 0);
      masterState.currentOffset = Number(retryData.offset || 0);
      masterState.pageLimit = Number(retryData.limit || masterState.pageLimit || 10);
      renderMasterRows(
        retryData.items || [],
        retryData.total || 0,
        retryData.mode || "smart",
        retryData.tokens || [],
        masterState.currentOffset,
      );
    } else {
      renderMasterRows(data.items || [], data.total || 0, data.mode || "smart", data.tokens || [], masterState.currentOffset);
    }
  } finally {
    if (requestId === masterState.requestSeq) {
      masterState.loading = false;
      renderPager();
    }
  }
}

function renderMasterRows(items, total, mode, tokens, offset) {
  const body = el("masterRows");
  const start = total > 0 ? offset + 1 : 0;
  const end = total > 0 ? offset + items.length : 0;
  const tokenText = tokens.length ? ` | Tokens: ${tokens.slice(0, 12).join(", ")}` : "";

  el("masterResultsMeta").textContent = `Showing ${start}-${end} of ${total} | Mode: ${mode}${tokenText}`;

  if (!items.length) {
    body.innerHTML = `<tr><td colspan="9">No master schemes matched your query.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map((item, idx) => {
      return `
        <tr>
          <td>${offset + idx + 1}</td>
          <td title="${escapeHtml(item.scheme_name)}">${escapeHtml(truncate(item.scheme_name, 54))}</td>
          <td>${escapeHtml(fmt(item.scheme_type))}</td>
          <td title="${escapeHtml(item.nodal_ministry || "")}">${escapeHtml(truncate(item.nodal_ministry || "-", 30))}</td>
          <td>${escapeHtml(item.confidence_score)}</td>
          <td>${escapeHtml(item.source_count)}</td>
          <td>${escapeHtml(item.media_count)}</td>
          <td title="${escapeHtml(item.match_reason || "")}">${escapeHtml(truncate(item.match_reason || "quality-rank", 28))}</td>
          <td>${escapeHtml(fmt(item.updated_at, "-"))}</td>
        </tr>
      `;
    })
    .join("");
}

function renderPager() {
  const limit = Math.max(1, Number(masterState.pageLimit || 30));
  const total = Math.max(0, Number(masterState.totalRows || 0));
  const offset = Math.max(0, Number(masterState.currentOffset || 0));

  const totalPages = Math.max(1, Math.ceil(total / limit));
  const currentPage = Math.min(totalPages, Math.floor(offset / limit) + 1);
  const hasPrev = !masterState.loading && offset > 0;
  const hasNext = !masterState.loading && offset + limit < total;

  el("masterPrevBtn").textContent = `← Previous`;
  el("masterNextBtn").textContent = `Next →`;
  el("masterPageInfo").textContent = `Page ${currentPage} / ${totalPages}`;
  el("masterPrevBtn").disabled = !hasPrev;
  el("masterNextBtn").disabled = !hasNext;
  el("masterPagerHint").textContent = masterState.loading
    ? "Loading page..."
    : total === 0
      ? "No results for current filters."
      : hasNext
        ? `More results available — ${total.toLocaleString()} total`
        : `Showing all ${total.toLocaleString()} results`;

  if (!hasNext) {
    el("masterNextBtn").title = "No next page for current scope and filters.";
  } else {
    el("masterNextBtn").title = "";
  }
}

async function goToPreviousPage() {
  if (masterState.loading) return;
  masterState.currentOffset = Math.max(0, masterState.currentOffset - masterState.pageLimit);
  await runMasterSearch();
}

async function goToNextPage() {
  if (masterState.loading) return;
  const nextOffset = masterState.currentOffset + masterState.pageLimit;
  if (nextOffset >= masterState.totalRows) return;
  masterState.currentOffset = nextOffset;
  await runMasterSearch();
}

function resetSearchForm() {
  el("masterQueryText").value = "";
  el("masterQueryMinistry").value = "";
  el("masterQueryType").value = "";
  el("masterQueryMode").value = "smart";
  el("masterQueryScope").value = "trusted";
  el("masterQueryMinScore").value = "0";
  el("masterQueryLimit").value = "10";
  masterState.currentOffset = 0;
}

function showError(error) {
  const message = (error && error.message) || String(error);
  el("masterResultsMeta").textContent = `Error: ${message}`;
}

async function bootstrapMasterPage() {
  // Guard against double-initialization (called from both master.html and refresh button)
  if (masterState.bootstrapped) {
    await Promise.all([refreshMasterMetrics(), runMasterSearch()]);
    return;
  }
  masterState.bootstrapped = true;

  el("masterQueryForm").addEventListener("submit", (event) => {
    runMasterSearch(event).catch(showError);
  });

  el("masterPrevBtn").addEventListener("click", () => {
    goToPreviousPage().catch(showError);
  });

  el("masterNextBtn").addEventListener("click", () => {
    goToNextPage().catch(showError);
  });

  el("masterClearBtn").addEventListener("click", () => {
    resetSearchForm();
    runMasterSearch().catch(showError);
  });

  el("masterQueryScope").addEventListener("change", () => {
    applyScopeDefaults();
    masterState.currentOffset = 0;
    runMasterSearch().catch(showError);
  });

  el("masterQueryLimit").addEventListener("change", () => {
    masterState.currentOffset = 0;
    runMasterSearch().catch(showError);
  });

  await Promise.all([refreshMasterMetrics(), runMasterSearch()]);
}

// Expose globally so master.html refresh button can re-run data fetch only
window.bootstrapMasterPage = bootstrapMasterPage;
bootstrapMasterPage().catch(showError);
