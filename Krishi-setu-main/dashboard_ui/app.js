const dashboardState = {
  charts: {},
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

function truncate(value, maxLen = 72) {
  const text = String(value || "");
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen - 1) + "...";
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatStamp(value) {
  const text = String(value || "");
  if (!text) return "-";
  return text.replace("T", " ").replace("Z", "");
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
  const existing = dashboardState.charts[canvasId];
  if (existing) {
    existing.destroy();
    delete dashboardState.charts[canvasId];
  }
}

function renderChart(canvasId, config) {
  if (typeof window.Chart === "undefined") return;

  const canvas = el(canvasId);
  if (!canvas) return;

  destroyChart(canvasId);
  dashboardState.charts[canvasId] = new window.Chart(canvas.getContext("2d"), config);
}

async function loadDashboardData() {
  const masterOverviewPromise = apiRequest("/api/master/overview").catch(() => ({
    totals: {},
    top_ministries: [],
    top_types: [],
  }));

  const [overview, runsPayload, masterOverview] = await Promise.all([
    apiRequest("/api/overview"),
    apiRequest("/api/runs?limit=30"),
    masterOverviewPromise,
  ]);

  return {
    overview,
    runs: runsPayload.items || [],
    masterOverview,
  };
}

function renderOverview(overview) {
  const totals = overview.totals || {};

  el("metricSchemes").textContent = totals.schemes ?? "-";
  el("metricCurated").textContent = totals.curated ?? "-";
  el("metricMaster").textContent = totals.master ?? totals.schemes ?? "-";
  el("metricRuns").textContent = totals.runs ?? "-";
  el("metricSources").textContent = totals.sources ?? "-";

  const latest = overview.latest_run;
  if (latest) {
    const stats = latest.stats || {};
    el("latestRunLine").textContent =
      `Latest Run: ${latest.task_id} | ${latest.status} | ` +
      `Unique schemes ${stats.run_unique_schemes ?? "-"} | Rejected noise ${stats.rejected_noisy_candidates ?? "-"}`;
    el("qualityLine").textContent = `Quality breakdown: ${stats.quality_breakdown || "-"}`;
  } else {
    el("latestRunLine").textContent = "Latest Run: -";
    el("qualityLine").textContent = "Quality: -";
  }

  const types = overview.top_types || [];
  el("topTypesChips").innerHTML = types
    .map((item) => `<span class="chip">${escapeHtml(item.scheme_type || "other")}: ${item.count}</span>`)
    .join("");
}

function renderRunsTable(rows) {
  const body = el("runRows");

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="6">No runs found.</td></tr>`;
    return;
  }

  body.innerHTML = rows
    .map((row) => {
      const stats = row.stats || {};
      return `
        <tr>
          <td>${escapeHtml(row.task_id)}</td>
          <td>${escapeHtml(row.status)}</td>
          <td title="${escapeHtml(row.source_url)}">${escapeHtml(truncate(row.source_url, 58))}</td>
          <td>${escapeHtml(stats.run_unique_schemes ?? "-")}</td>
          <td>${escapeHtml(stats.rejected_noisy_candidates ?? "-")}</td>
          <td>${escapeHtml(row.started_at || "-")}</td>
        </tr>
      `;
    })
    .join("");
}

function renderYieldTrendChart(runs) {
  const ordered = [...runs].reverse().slice(-14);
  const labels = ordered.map((row) => String(row.task_id || "-").slice(-6));
  const uniqueSchemes = ordered.map((row) => toNumber((row.stats || {}).run_unique_schemes));
  const noisyRejected = ordered.map((row) => toNumber((row.stats || {}).rejected_noisy_candidates));

  renderChart("yieldTrendChart", {
    type: "line",
    data: {
      labels: labels.length ? labels : ["No Data"],
      datasets: [
        {
          label: "Unique Schemes",
          data: uniqueSchemes.length ? uniqueSchemes : [0],
          borderColor: "#2f7a44",
          backgroundColor: "rgba(47, 122, 68, 0.2)",
          tension: 0.28,
          fill: true,
        },
        {
          label: "Rejected Noise",
          data: noisyRejected.length ? noisyRejected : [0],
          borderColor: "#cf7f2f",
          backgroundColor: "rgba(207, 127, 47, 0.18)",
          tension: 0.28,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
        },
      },
      scales: {
        y: {
          beginAtZero: true,
        },
      },
    },
  });
}

function renderRunStatusChart(runs) {
  const statusCounts = {};
  for (const row of runs) {
    const key = String(row.status || "unknown").toLowerCase();
    statusCounts[key] = (statusCounts[key] || 0) + 1;
  }

  const labels = Object.keys(statusCounts);
  const values = Object.values(statusCounts);

  renderChart("runStatusChart", {
    type: "doughnut",
    data: {
      labels: labels.length ? labels : ["no-data"],
      datasets: [
        {
          data: values.length ? values : [1],
          backgroundColor: ["#2f7a44", "#cf7f2f", "#a44533", "#1e5a31", "#6d8a66"],
          borderColor: "#fffdf7",
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
        },
      },
    },
  });
}

function renderSchemeTypesChart(topTypes) {
  const labels = topTypes.map((item) => String(item.scheme_type || "other"));
  const values = topTypes.map((item) => toNumber(item.count));

  renderChart("schemeTypesChart", {
    type: "bar",
    data: {
      labels: labels.length ? labels : ["no-data"],
      datasets: [
        {
          label: "Schemes",
          data: values.length ? values : [0],
          backgroundColor: "rgba(47, 122, 68, 0.72)",
          borderRadius: 8,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
      },
      scales: {
        y: {
          beginAtZero: true,
        },
      },
    },
  });
}

function renderMinistryChart(masterOverview) {
  const ministries = (masterOverview.top_ministries || []).slice(0, 8);
  const labels = ministries.map((item) => String(item.nodal_ministry || "Unknown"));
  const values = ministries.map((item) => toNumber(item.count));

  renderChart("ministryChart", {
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
      indexAxis: "y",
      plugins: {
        legend: {
          display: false,
        },
      },
      scales: {
        x: {
          beginAtZero: true,
        },
      },
    },
  });
}

async function refreshDashboard() {
  const { overview, runs, masterOverview } = await loadDashboardData();
  renderOverview(overview);
  renderRunsTable(runs);
  renderYieldTrendChart(runs);
  renderRunStatusChart(runs);
  renderSchemeTypesChart(overview.top_types || []);
  renderMinistryChart(masterOverview);
  el("resultsMeta").textContent = `${runs.length} recent runs loaded`;
}

function showError(error) {
  const message = (error && error.message) || String(error);
  el("latestRunLine").textContent = `Error: ${message}`;
}

async function bootstrap() {
  el("refreshBtn").addEventListener("click", () => {
    refreshDashboard().catch(showError);
  });

  await refreshDashboard();
}

bootstrap().catch(showError);
