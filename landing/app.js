const CATALOG_PATH = "./configs/output_catalog.json";

const state = {
    catalog: null,
    analyses: [],
};

function formatNumber(value, digits = 3) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "n/a";
    }
    return Number(value).toFixed(digits);
}

function formatPercent(value, digits = 1) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "n/a";
    }
    return `${Number(value).toFixed(digits)}%`;
}

function formatInteger(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "n/a";
    }
    return Number(value).toLocaleString();
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}

function normalizeSearch(value) {
    return String(value ?? "")
        .toLowerCase()
        .trim();
}

function outputBasePath(entry) {
    return entry.path.replace(/\/+$/, "");
}

async function loadCatalog() {
    const response = await fetch(CATALOG_PATH);
    if (!response.ok) {
        throw new Error(`Could not load catalog JSON at ${CATALOG_PATH}`);
    }
    return response.json();
}

async function loadSummary(entry) {
    const summaryPath = `${outputBasePath(entry)}/summary.json`;
    try {
        const response = await fetch(summaryPath);
        if (!response.ok) {
            return null;
        }
        return await response.json();
    } catch {
        return null;
    }
}

function derivedEntryData(entry, summary) {
    const firstPeriodKey = summary
        ? Object.keys(summary.periods ?? {})[0]
        : null;
    const firstPeriod = firstPeriodKey ? summary.periods[firstPeriodKey] : null;
    const metrics = firstPeriod?.metrics ?? {};
    const hotspot = firstPeriod?.hotspots?.[0] ?? null;
    const label = entry.title || summary?.metadata?.label || entry.path;
    const description =
        entry.description ||
        summary?.metadata?.district_name ||
        "Generated analysis output";
    const periods =
        entry.periods ||
        summary?.config?.periods?.map((value) => `${value}y`) ||
        [];
    const searchText = [
        label,
        description,
        entry.path,
        ...(entry.tags ?? []),
        summary?.metadata?.state_name,
        summary?.metadata?.district_name,
        summary?.metadata?.country_iso3,
    ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

    return {
        entry,
        summary,
        label,
        description,
        metrics,
        hotspot,
        periods,
        searchText,
    };
}

function renderStats(items) {
    const statsRoot = document.getElementById("libraryStats");
    const featuredCount = items.filter((item) => item.entry.featured).length;
    const countryCount = new Set(
        items.map(
            (item) =>
                item.summary?.metadata?.country_iso3 ||
                item.entry.country ||
                "Unknown",
        ),
    ).size;
    const wardCount = items.filter(
        (item) =>
            item.summary?.metadata?.ward_overlay_available ||
            item.entry.has_wards,
    ).length;
    const stats = [
        ["Analyses", items.length],
        ["Featured", featuredCount],
        ["Countries", countryCount],
        ["Ward Ready", wardCount],
    ];
    statsRoot.innerHTML = stats
        .map(
            ([label, value]) => `
                <div class="stat-card">
                    <div class="stat-label">${escapeHtml(label)}</div>
                    <div class="stat-value">${escapeHtml(formatInteger(value))}</div>
                </div>
            `,
        )
        .join("");
}

function renderCard(item) {
    const { entry, summary, label, description, metrics, hotspot, periods } =
        item;
    const basePath = outputBasePath(entry);
    const uiPath = entry.ui_path || basePath;
    const reportPath = entry.report_path || `${basePath}/report.md`;
    const summaryPath = `${basePath}/summary.json`;
    const coveragePercent = summary?.metadata?.coverage_percent;
    const wardReady = Boolean(
        summary?.metadata?.ward_overlay_available || entry.has_wards,
    );
    const cardBadge = entry.badge || (entry.featured ? "Featured" : "Output");
    const storyLine = hotspot
        ? `${hotspot.story}. Hotspot near (${hotspot.latitude}, ${hotspot.longitude}) with p95 ${formatNumber(metrics.embedding_change_p95)}.`
        : entry.note ||
          "Open the analysis bundle to inspect the map, report, and summary.";

    return `
        <article class="analysis-card">
            <div class="card-top">
                <div>
                <div class="card-eyebrow">${escapeHtml(entry.group || "Analysis Bundle")}</div>
                <h2 class="analysis-title">${escapeHtml(label)}</h2>
                <p class="analysis-subtitle">${escapeHtml(description)}</p>
                </div>
                <div class="card-badge">${escapeHtml(cardBadge)}</div>
            </div>

            <div class="pill-row">
                ${(entry.tags ?? [])
                    .map(
                        (tag) => `<span class="pill">${escapeHtml(tag)}</span>`,
                    )
                    .join("")}
                ${periods
                    .map(
                        (period) =>
                            `<span class="pill">${escapeHtml(period)}</span>`,
                    )
                    .join("")}
                ${
                    summary?.config?.base_year
                        ? `<span class="pill">Base ${escapeHtml(summary.config.base_year)}</span>`
                        : ""
                }
                ${wardReady ? `<span class="pill">Ward Overlay</span>` : ""}
            </div>

            <div class="meta-grid">
                <div class="meta-block">
                    <div class="meta-label">Coverage</div>
                    <div class="meta-value">${escapeHtml(formatPercent(coveragePercent, 1))}</div>
                </div>
                <div class="meta-block">
                    <div class="meta-label">P95 Change</div>
                    <div class="meta-value">${escapeHtml(formatNumber(metrics.embedding_change_p95, 3))}</div>
                </div>
                <div class="meta-block">
                    <div class="meta-label">Urban Mean</div>
                    <div class="meta-value">${escapeHtml(formatNumber(metrics.urban_delta_mean, 3))}</div>
                </div>
                <div class="meta-block">
                    <div class="meta-label">Overlay Cells</div>
                    <div class="meta-value">${escapeHtml(formatInteger(summary?.feature_count))}</div>
                </div>
            </div>

            <div class="story-line">${escapeHtml(storyLine)}</div>

            <div class="action-row">
                <a class="action-link primary" href="${encodeURI(uiPath)}">Open Map</a>
                <a class="action-link secondary" href="${encodeURI(reportPath)}">Read Report</a>
                <a class="action-link secondary" href="${encodeURI(summaryPath)}">Summary JSON</a>
            </div>
        </article>
        `;
}

function renderCatalog(items) {
    const catalogGrid = document.getElementById("catalogGrid");
    const emptyState = document.getElementById("emptyState");
    if (!items.length) {
        catalogGrid.innerHTML = "";
        emptyState.hidden = false;
        return;
    }
    emptyState.hidden = true;
    catalogGrid.innerHTML = items.map(renderCard).join("");
}

function applySearch() {
    const query = normalizeSearch(document.getElementById("search").value);
    const filtered = !query
        ? state.analyses
        : state.analyses.filter((item) => item.searchText.includes(query));
    renderCatalog(filtered);
}

async function main() {
    const catalog = await loadCatalog();
    state.catalog = catalog;

    document.getElementById("siteTitle").textContent =
        catalog.title || "OlmoEarth Output Library";
    document.getElementById("siteSubtitle").textContent =
        catalog.subtitle ||
        "Browse generated outputs, open map overlays, and inspect summary files from one place.";
    document.getElementById("sectionHint").textContent =
        catalog.hint ||
        "Add a new analysis by appending one entry to the catalog JSON file.";

    const entries = catalog.outputs ?? [];
    const analyses = await Promise.all(
        entries.map(async (entry) =>
            derivedEntryData(entry, await loadSummary(entry)),
        ),
    );
    state.analyses = analyses;
    renderStats(analyses);
    renderCatalog(analyses);
}

document.getElementById("search").addEventListener("input", applySearch);

main().catch((error) => {
    document.getElementById("siteTitle").textContent =
        "Could not load output library";
    document.getElementById("siteSubtitle").textContent =
        `${error.message}. Serve the repository root with a local web server before opening this page.`;
    document.getElementById("catalogGrid").innerHTML = `
    <div class="error-card">
      ${escapeHtml(error.message)}
    </div>
  `;
});
