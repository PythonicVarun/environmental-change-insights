const metricOptions = [
    { key: "embedding_change", label: "OlmoEarth Change" },
    { key: "vegetation_delta", label: "Vegetation Delta" },
    { key: "water_delta", label: "Water Delta" },
    { key: "urban_delta", label: "Urbanization Delta" },
    { key: "bare_soil_delta", label: "Bare Soil Delta" },
    { key: "pollution_delta", label: "Pollution Proxy Delta" },
    { key: "population_delta", label: "Population Delta" },
];

const metricColorStops = {
    embedding_change: ["#f7f2eb", "#f1a661", "#8d2f20"],
    vegetation_delta: ["#8e4f2a", "#f5edd6", "#23643d"],
    water_delta: ["#7b4a26", "#f3efe7", "#1f6e8c"],
    urban_delta: ["#2f5d50", "#f1efe8", "#c65d19"],
    bare_soil_delta: ["#2f6b84", "#f7f1dd", "#8b5e34"],
    pollution_delta: ["#355c7d", "#f2efe7", "#7f2704"],
    population_delta: ["#3f6791", "#f4efe5", "#bc5a2e"],
};

const metricSummaryDefinitions = {
    embedding_change: {
        summaryKey: "embedding_change_median",
        label: "Median change",
        unit: "median score",
        formatter: (value) => formatNumber(value),
    },
    vegetation_delta: {
        summaryKey: "vegetation_delta_mean",
        label: "Mean vegetation delta",
        unit: "mean delta",
        formatter: (value) => formatNumber(value),
    },
    water_delta: {
        summaryKey: "water_delta_mean",
        label: "Mean water delta",
        unit: "mean delta",
        formatter: (value) => formatNumber(value),
    },
    urban_delta: {
        summaryKey: "urban_delta_mean",
        label: "Mean urban delta",
        unit: "mean delta",
        formatter: (value) => formatNumber(value),
    },
    bare_soil_delta: {
        summaryKey: "bare_soil_delta_mean",
        label: "Mean bare-soil delta",
        unit: "mean delta",
        formatter: (value) => formatNumber(value),
    },
    pollution_delta: {
        summaryKey: "pollution_delta_mean",
        label: "Mean pollution proxy",
        unit: "mean delta",
        formatter: (value) => formatNumber(value),
    },
    population_delta: {
        summaryKey: "population_delta_total",
        label: "Population delta",
        unit: "people",
        formatter: (value) => formatPopulation(value),
    },
};

const WAYBACK_CONFIG_URL =
    "https://s3-us-west-2.amazonaws.com/config.maptiles.arcgis.com/waybackconfig.json";
const WAYBACK_DATE_REGEX = /Wayback (\d{4}-\d{2}-\d{2})/;

// Local PMTiles URL (overridden per-city for release-hosted large files)
const OVERLAY_PMTILES_URL = "overlay.pmtiles";
const WARD_OVERLAY_PMTILES_URL = "ward_overlay.pmtiles";

const basemapDefinitions = {
    none: { label: "No Basemap" },
    osm: { label: "OpenStreetMap" },
    carto_light: { label: "Carto Light" },
    esri_imagery: { label: "Esri Imagery" },
};

const maplibreBasemapTiles = {
    osm: {
        tiles: [
            "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
        ],
        tileSize: 256,
        attribution: "© <a href=\"https://www.openstreetmap.org/copyright\">OpenStreetMap</a> contributors",
        maxzoom: 19,
    },
    carto_light: {
        tiles: [
            "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
            "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
            "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
            "https://d.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
        ],
        tileSize: 256,
        attribution: "© <a href=\"https://www.openstreetmap.org/copyright\">OpenStreetMap</a> contributors © <a href=\"https://carto.com/attributions\">CARTO</a>",
        maxzoom: 20,
    },
    esri_imagery: {
        tiles: [
            "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        ],
        tileSize: 256,
        attribution: "Tiles © Esri",
        maxzoom: 18,
    },
};

const historicalPlaybackSpeeds = [2000, 1200, 800, 450];
const MIN_PROGRESS_WIDTH_PERCENT = 2;

// ── PMTiles protocol ──────────────────────────────────────────────────────────
const pmtilesProtocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", pmtilesProtocol.tile.bind(pmtilesProtocol));

// ── URL state parsing ─────────────────────────────────────────────────────────

function parseFiniteNumber(value, { min = -Infinity, max = Infinity } = {}) {
    if (value === null || value === undefined || value === "") return null;
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) return null;
    if (numericValue < min || numericValue > max) return null;
    return numericValue;
}

function normalizeHistoricalMode(value) {
    return ["off", "timeline", "base"].includes(value) ? value : null;
}

function normalizePlaybackSpeedMs(value) {
    const numericValue = parseFiniteNumber(value, { min: 0, max: 10000 });
    return historicalPlaybackSpeeds.includes(numericValue) ? numericValue : null;
}

function normalizeSnapshotDateKey(value) {
    if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
    return value;
}

function parseUrlState(params) {
    const basemap = params.get("basemap");
    const metric = params.get("metric");
    const unit = params.get("unit");
    return {
        basemap: basemapDefinitions[basemap] ? basemap : null,
        metric: metricOptions.some((option) => option.key === metric) ? metric : null,
        unit: ["cells", "wards"].includes(unit) ? unit : null,
        period: params.get("period"),
        opacity: parseFiniteNumber(params.get("opacity"), { min: 0.15, max: 0.95 }),
        historicalMode: normalizeHistoricalMode(params.get("historical")),
        historicalSnapshotDate: normalizeSnapshotDateKey(params.get("historicalDate")),
        speed: normalizePlaybackSpeedMs(params.get("speed")),
        lat: parseFiniteNumber(params.get("lat"), { min: -90, max: 90 }),
        lng: parseFiniteNumber(params.get("lng"), { min: -180, max: 180 }),
        zoom: parseFiniteNumber(params.get("zoom"), { min: 0, max: 22 }),
    };
}

const searchParams = new URLSearchParams(window.location.search);
const fileProtocol = window.location.protocol === "file:";
const initialUrlState = parseUrlState(searchParams);
const requestedBasemapMode = initialUrlState.basemap;
const initialBasemapMode = fileProtocol ? "none" : requestedBasemapMode ?? "osm";

function inferLocationSlug(pathname = window.location.pathname) {
    const segments = pathname.split("/").filter(Boolean);
    if (!segments.length) return "";
    const outputsIndex = segments.lastIndexOf("outputs");
    const lastSegment = segments[segments.length - 1];
    if (outputsIndex >= 0 && outputsIndex + 1 < segments.length) {
        return decodeURIComponent(segments[outputsIndex + 1]);
    }
    if (/\.html?$/i.test(lastSegment) && segments.length > 1) {
        return decodeURIComponent(segments[segments.length - 2]);
    }
    return decodeURIComponent(lastSegment);
}

function humanizeLocationSlug(slug) {
    return slug
        .replace(/[_-]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, (character) => character.toUpperCase());
}

const fallbackLocationLabel = humanizeLocationSlug(inferLocationSlug()) || "Location";

// ── MapLibre GL JS map ────────────────────────────────────────────────────────

const map = new maplibregl.Map({
    container: "map",
    style: { version: 8, sources: {}, layers: [] },
    center: [78.9629, 20.5937],
    zoom: 5,
    attributionControl: { compact: false, customAttribution: "OlmoEarth" },
    maxZoom: 20,
    minZoom: 3,
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

const mapReadyPromise = new Promise((resolve) => map.once("load", resolve));

// Tooltip element, attached to map container after map ready
let tooltipEl = null;

// ── App state ─────────────────────────────────────────────────────────────────

const state = {
    summary: null,
    overlayMeta: { cells: null, wards: null },
    boundsByProperty: { cells: {}, wards: {} },
    historicalMode: "off",
    historicalSnapshots: [],
    activeHistoricalSnapshotKey: null,
    playbackTimerId: null,
    playbackFrameYear: null,
    selectedHistoricalSnapshotIndex: null,
    playbackSpeedMs: 1200,
    basemapMode: initialBasemapMode,
    preferredBasemapMode: initialBasemapMode,
    urlSyncEnabled: false,
    periods: [],
    hasWards: false,
    hoveredCellsId: null,
    hoveredWardsId: null,
};

// ── Format helpers ────────────────────────────────────────────────────────────

function formatNumber(value, digits = 3) {
    return Number(value ?? 0).toFixed(digits);
}

function formatPopulation(value, digits = 1) {
    return Number(value ?? 0).toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    });
}

function formatPercent(value, digits = 1) {
    if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
    return `${Number(value).toFixed(digits)}%`;
}

function formatBytesWithMbUnit(bytes) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// ── Period / metric key helpers ───────────────────────────────────────────────

function currentPeriodKey() {
    const slider = document.getElementById("period");
    if (!slider) return state.periods[0];
    return state.periods[Number(slider.value)] ?? state.periods[0];
}

function currentMetricKey() {
    return document.getElementById("metric").value;
}

function currentUnitKey() {
    return document.getElementById("unit").value || "cells";
}

function activeBoundsByProperty() {
    return state.boundsByProperty[currentUnitKey()] ?? {};
}

function propertyKey() {
    return `${currentMetricKey()}_${currentPeriodKey()}`;
}

function currentMetricOption() {
    return metricOptions.find((metric) => metric.key === currentMetricKey()) ?? metricOptions[0];
}

function summaryMetricValue(periodKey = currentPeriodKey(), metricKey = currentMetricKey()) {
    const config = metricSummaryDefinitions[metricKey];
    if (!config) return null;
    return state.summary?.periods?.[periodKey]?.metrics?.[config.summaryKey] ?? null;
}

function availableMetricOptions() {
    const boundsByProperty = activeBoundsByProperty();
    return metricOptions.filter((metric) =>
        state.periods.some((period) => boundsByProperty[`${metric.key}_${period}`]),
    );
}

function availableUnitOptions() {
    const options = [{ key: "cells", label: "Cells" }];
    if (state.hasWards) options.push({ key: "wards", label: "Wards" });
    return options;
}

// ── Color expression builder ──────────────────────────────────────────────────

function buildColorExpression() {
    const key = propertyKey();
    const metric = currentMetricKey();
    const bounds = activeBoundsByProperty()[key];
    const colors = metricColorStops[metric] ?? metricColorStops.embedding_change;

    if (!bounds) return "rgba(0,0,0,0)";

    const { min, max } = bounds;

    // All values identical → use middle color
    if (min === max) {
        return [
            "case",
            ["==", ["typeof", ["get", key]], "number"],
            colors[1],
            "rgba(0,0,0,0)",
        ];
    }

    let stops;
    if (metric === "embedding_change") {
        stops = [min, colors[0], (min + max) / 2, colors[1], max, colors[2]];
    } else {
        const span = Math.max(Math.abs(min), Math.abs(max), 1e-9);
        stops = [-span, colors[0], 0, colors[1], span, colors[2]];
    }

    return [
        "case",
        ["==", ["typeof", ["get", key]], "number"],
        ["interpolate", ["linear"], ["get", key], ...stops],
        "rgba(0,0,0,0)",
    ];
}

function buildLineColorExpression(isWard) {
    return [
        "case",
        ["boolean", ["feature-state", "hovered"], false],
        "rgba(23,34,45,0.72)",
        isWard ? "rgba(23,34,45,0.22)" : "rgba(23,34,45,0)",
    ];
}

function buildLineWidthExpression(isWard) {
    return [
        "case",
        ["boolean", ["feature-state", "hovered"], false],
        isWard ? 1.5 : 0.9,
        isWard ? 0.7 : 0,
    ];
}

// ── URL sync ──────────────────────────────────────────────────────────────────

function setUrlParam(params, key, value) {
    if (value === null || value === undefined || value === "") {
        params.delete(key);
        return;
    }
    params.set(key, String(value));
}

function syncUrlState() {
    if (!state.urlSyncEnabled || !state.summary) return;

    const params = new URLSearchParams(window.location.search);
    const center = map.getCenter();
    const currentSnapshot = currentHistoricalSnapshot();

    setUrlParam(params, "basemap", state.preferredBasemapMode);
    setUrlParam(params, "metric", currentMetricKey());
    setUrlParam(params, "unit", currentUnitKey());
    setUrlParam(params, "period", currentPeriodKey());
    setUrlParam(params, "opacity", Number(document.getElementById("opacity").value).toFixed(2));
    setUrlParam(params, "historical", state.historicalMode);
    setUrlParam(
        params,
        "historicalDate",
        state.historicalMode === "off" &&
            state.selectedHistoricalSnapshotIndex === null &&
            state.playbackFrameYear === null
            ? null
            : snapshotDateKey(currentSnapshot),
    );
    setUrlParam(params, "speed", state.playbackSpeedMs);
    setUrlParam(params, "lat", center.lat.toFixed(5));
    setUrlParam(params, "lng", center.lng.toFixed(5));
    setUrlParam(params, "zoom", map.getZoom().toFixed(2));

    const nextSearch = params.toString();
    const nextUrl =
        `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ""}` +
        `${window.location.hash}`;
    window.history.replaceState({}, "", nextUrl);
}

// ── Loading UI ────────────────────────────────────────────────────────────────

function setLoadingState(isLoading, message) {
    if (message) document.getElementById("loadingMessage").textContent = message;
    document.body.classList.toggle("is-loading", isLoading);
}

function setLoadingProgress(progressFraction) {
    const fill = document.querySelector(".loading-bar-fill");
    if (!fill) return;
    if (!Number.isFinite(progressFraction)) {
        fill.classList.remove("is-determinate");
        fill.style.width = "";
        return;
    }
    const clamped = Math.max(0, Math.min(1, progressFraction));
    fill.classList.add("is-determinate");
    fill.style.width = `${Math.max(MIN_PROGRESS_WIDTH_PERCENT, Math.round(clamped * 100))}%`;
}

// ── Location chrome ───────────────────────────────────────────────────────────

function currentAreaLabel() {
    return state.summary?.metadata?.label || fallbackLocationLabel;
}

function updateLocationChrome() {
    const areaLabel = currentAreaLabel();
    document.title = `OlmoEarth Change Overlay · ${areaLabel}`;
    document.getElementById("loadingTitle").textContent = `Preparing ${areaLabel} Monitor`;
    if (!state.summary) {
        document.getElementById("title").textContent = `Loading ${areaLabel} analysis...`;
        document.getElementById("subtitle").textContent =
            `Reading summary and overlay layers for ${areaLabel}.`;
        document.getElementById("frameBadgeSubtitle").textContent =
            `${areaLabel} change analysis`;
    }
}

function buildSubtitle(summary) {
    const coveragePercent = summary.metadata.coverage_percent;
    const coverageLabel =
        coveragePercent !== undefined
            ? ` Coverage ${Number(coveragePercent).toFixed(1)}% of boundary.`
            : "";
    const wardLabel = summary.metadata.ward_overlay_available
        ? ` Ward overlay available for ${summary.metadata.ward_count} wards.`
        : "";
    const baseText =
        `Base year ${summary.config.base_year}. ${summary.feature_count} cell overlays. ` +
        `Display cell size ${summary.config.display_cell_size_m} m.` +
        coverageLabel +
        wardLabel;

    if (!fileProtocol && state.basemapMode !== "none") return baseText;

    return (
        `${baseText} Basemap is off because this page is running from ` +
        "`file://` or was opened with `?basemap=none`. " +
        "Serve the folder over http(s), like `python -m http.server`, to use remote basemaps with a valid Referer."
    );
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function buildTooltip(properties) {
    const period = currentPeriodKey();
    const populationDelta = properties[`population_delta_${period}`];
    const populationPct = properties[`population_pct_change_${period}`];
    const pollutionDelta = properties[`pollution_delta_${period}`];
    const coverageLine =
        properties.coverage_percent !== undefined
            ? `Coverage: ${formatPercent(properties.coverage_percent, 1)}<br />`
            : "";
    const pollutionLine =
        pollutionDelta === undefined || pollutionDelta === null
            ? ""
            : `Pollution Proxy: ${formatNumber(pollutionDelta)}<br />`;
    const populationLine =
        populationDelta === undefined || populationDelta === null
            ? ""
            : `Population: ${formatPopulation(populationDelta)} (${formatPercent(populationPct, 1)})<br />`;
    const wardHeader = properties.ward_name
        ? `<strong>${properties.ward_name}</strong><br />`
        : "";
    const storyLine = properties[`story_${period}`] ?? "Change area";

    return `
    <div class="map-tooltip">
      ${wardHeader}
      <strong>${storyLine}</strong><br />
      ${coverageLine}
      OlmoEarth: ${formatNumber(properties[`embedding_change_${period}`] ?? 0)}<br />
      Vegetation: ${formatNumber(properties[`vegetation_delta_${period}`] ?? 0)}<br />
      Water: ${formatNumber(properties[`water_delta_${period}`] ?? 0)}<br />
      Urban: ${formatNumber(properties[`urban_delta_${period}`] ?? 0)}<br />
      Bare Soil: ${formatNumber(properties[`bare_soil_delta_${period}`] ?? 0)}<br />
      ${pollutionLine}
      ${populationLine}
    </div>
  `;
}

function showTooltip(point, properties) {
    if (!tooltipEl) return;
    tooltipEl.innerHTML = buildTooltip(properties);
    tooltipEl.style.display = "block";
    positionTooltip(point);
}

function hideTooltip() {
    if (!tooltipEl) return;
    tooltipEl.style.display = "none";
}

function positionTooltip(point) {
    if (!tooltipEl || tooltipEl.style.display === "none") return;
    const container = map.getContainer();
    const cw = container.clientWidth;
    const ch = container.clientHeight;
    const tw = tooltipEl.offsetWidth || 220;
    const th = tooltipEl.offsetHeight || 120;
    const x = point.x + 16 + tw > cw ? point.x - tw - 10 : point.x + 16;
    const y = point.y + 16 + th > ch ? point.y - th - 10 : point.y + 16;
    tooltipEl.style.left = `${x}px`;
    tooltipEl.style.top = `${y}px`;
}

// ── Map telemetry ─────────────────────────────────────────────────────────────

function updateMapTelemetry() {
    const center = map.getCenter();
    document.getElementById("telemetryLat").textContent = center.lat.toFixed(4);
    document.getElementById("telemetryLng").textContent = center.lng.toFixed(4);
    document.getElementById("telemetryZoom").textContent = map.getZoom().toFixed(1);
    syncUrlState();
}

// ── Legend / summary / chrome updaters ───────────────────────────────────────

function updateLegend() {
    const key = propertyKey();
    const metric = currentMetricKey();
    const bounds = activeBoundsByProperty()[key] ?? { min: 0, max: 0 };
    const legend = document.getElementById("legendScale");
    const colors = metricColorStops[metric] ?? metricColorStops.embedding_change;
    legend.style.background = `linear-gradient(90deg, ${colors[0]} 0%, ${colors[1]} 50%, ${colors[2]} 100%)`;
    document.getElementById("legendMin").textContent =
        metric === "population_delta" ? formatPopulation(bounds.min, 1) : formatNumber(bounds.min);
    document.getElementById("legendMax").textContent =
        metric === "population_delta" ? formatPopulation(bounds.max, 1) : formatNumber(bounds.max);
}

function updateSummaryCards() {
    const periodKey = currentPeriodKey();
    const periodSummary = state.summary.periods[periodKey];
    const grid = document.getElementById("summaryGrid");
    const metrics = periodSummary?.metrics ?? {};
    const entries = [
        ["Median Change", formatNumber(metrics.embedding_change_median ?? 0)],
        ["P95 Change", formatNumber(metrics.embedding_change_p95 ?? 0)],
        ["Mean Vegetation", formatNumber(metrics.vegetation_delta_mean ?? 0)],
        ["Mean Urban", formatNumber(metrics.urban_delta_mean ?? 0)],
    ];
    if (metrics.pollution_delta_mean !== undefined) {
        entries.push(["Mean Pollution", formatNumber(metrics.pollution_delta_mean ?? 0)]);
    }
    if (metrics.population_delta_total !== undefined) {
        entries.push(["Population Delta", formatPopulation(metrics.population_delta_total ?? 0)]);
        entries.push(["Population %", formatPercent(metrics.population_pct_change_total, 1)]);
    }
    if (state.summary.metadata.ward_overlay_available) {
        entries.push(["Ward Units", String(state.summary.metadata.ward_count ?? 0)]);
    }
    grid.innerHTML = entries
        .map(
            ([label, value]) => `
        <div class="stat">
          <div class="stat-label">${label}</div>
          <div class="stat-value">${value}</div>
        </div>`,
        )
        .join("");

    const hotspots = document.getElementById("hotspots");
    hotspots.innerHTML = (periodSummary?.hotspots ?? [])
        .slice(0, 5)
        .map(
            (spot) => `
        <div class="hotspot">
          <div class="hotspot-title">${spot.story}</div>
          <div class="hotspot-meta">
            (${spot.latitude}, ${spot.longitude})<br />
            Change ${formatNumber(spot.embedding_change)} |
            Veg ${formatNumber(spot.vegetation_delta)} |
            Water ${formatNumber(spot.water_delta)} |
            Urban ${formatNumber(spot.urban_delta)}
            ${
                spot.pollution_delta !== undefined && spot.pollution_delta !== null
                    ? `<br />Pollution ${formatNumber(spot.pollution_delta)}`
                    : ""
            }
            ${
                spot.population_delta !== undefined
                    ? `<br />Pop ${formatPopulation(spot.population_delta)} | ${formatPercent(spot.population_pct_change, 1)}`
                    : ""
            }
          </div>
        </div>`,
        )
        .join("");
}

function updateFocusPanel() {
    if (!state.summary) return;
    const config = metricSummaryDefinitions[currentMetricKey()];
    const value = summaryMetricValue();
    document.getElementById("focusValue").textContent =
        value === null ? "--" : config.formatter(value);
    document.getElementById("focusUnit").textContent = config?.unit ?? "summary";
    document.getElementById("focusCaption").textContent =
        `${config?.label ?? "Selected metric"} for the ${currentPeriodKey()} lookback window.`;
}

function updateTrendChart() {
    const container = document.getElementById("trendChart");
    if (!state.summary) { container.innerHTML = ""; return; }

    const config = metricSummaryDefinitions[currentMetricKey()];
    const series = state.periods.map((period) => ({ period, value: summaryMetricValue(period) }));
    const values = series.map((e) => e.value).filter((v) => v !== null && !Number.isNaN(v));

    if (!values.length) {
        container.innerHTML = `<div class="helper-label">Trend unavailable for ${config.label.toLowerCase()}.</div>`;
        return;
    }

    const width = 300;
    const height = 96;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const xStep = series.length > 1 ? width / (series.length - 1) : 0;
    const yFor = (value) => {
        if (max === min) return height / 2;
        return height - ((value - min) / (max - min)) * (height - 10) - 5;
    };

    const polyline = series.map((e, i) => `${i * xStep},${yFor(e.value ?? min)}`).join(" ");
    const area = `0,${height} ${polyline} ${width},${height}`;
    const points = series
        .map((e, i) => {
            const x = i * xStep;
            const y = yFor(e.value ?? min);
            const isActive = e.period === currentPeriodKey();
            return `<circle cx="${x}" cy="${y}" r="${isActive ? 4.5 : 3}" fill="${isActive ? "#00f0c8" : "#13d4ff"}" />`;
        })
        .join("");
    const labels = series
        .map((e, i) => {
            const x = i * xStep;
            return `<text x="${x}" y="${height + 16}" text-anchor="${i === 0 ? "start" : i === series.length - 1 ? "end" : "middle"}" fill="#70829f" font-size="10">${e.period}</text>`;
        })
        .join("");

    container.innerHTML = `
        <svg viewBox="0 0 ${width} ${height + 20}" preserveAspectRatio="none" role="img" aria-label="${config.label} trend">
            <path d="M 0 ${height / 2} H ${width}" stroke="rgba(129, 147, 176, 0.16)" stroke-width="1" />
            <polygon points="${area}" fill="rgba(19, 212, 255, 0.12)" />
            <polyline points="${polyline}" fill="none" stroke="#13d4ff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />
            ${points}
            ${labels}
        </svg>
    `;
}

function updateFrameDetails() {
    if (!state.summary) return;
    const snapshot = currentHistoricalSnapshot();
    const frameYear = currentHistoricalYear();
    const baseSnapshot = snapshotForYear(state.summary.config.base_year);
    const frameLabel =
        state.historicalMode === "off"
            ? "Off"
            : snapshot
              ? `${frameYear} · ${formatSnapshotDate(snapshot)}`
              : `${frameYear ?? "--"}`;

    document.getElementById("baseYearLabel").textContent =
        baseSnapshot
            ? `${formatSnapshotDate(baseSnapshot)} (Base Year)`
            : String(state.summary.config.base_year);
    document.getElementById("timelineYearLabel").textContent = String(timelineMatchYear() ?? "--");
    document.getElementById("activeFrameLabel").textContent = frameLabel;
    document.getElementById("coverageLabel").textContent = formatPercent(
        state.summary.metadata.coverage_percent,
        1,
    );
}

function updateFrameBadge() {
    if (!state.summary) return;
    const snapshot = currentHistoricalSnapshot();
    const metricLabel = currentMetricOption()?.label ?? "Change";
    const areaLabel = currentAreaLabel();
    const baseSnapshot = snapshotForYear(state.summary.config.base_year);
    const baseDateLabel = baseSnapshot
        ? `${formatSnapshotDate(baseSnapshot)} (Base Year)`
        : `${state.summary.config.base_year} (Base Year)`;
    let title = baseDateLabel;
    let subtitle = `${areaLabel} · ${metricLabel}`;

    if (state.historicalMode !== "off" || state.selectedHistoricalSnapshotIndex !== null) {
        title = snapshot
            ? formatSnapshotDate(snapshot)
            : `Wayback ${currentHistoricalYear() ?? ""}`.trim();
        subtitle =
            state.playbackTimerId !== null
                ? `${areaLabel} · Playback to base year`
                : `${areaLabel} · Wayback historical imagery`;
    } else if (snapshot) {
        title = `${formatSnapshotDate(snapshot)} (Base Year)`;
    }

    document.getElementById("frameBadgeTitle").textContent = title;
    document.getElementById("frameBadgeSubtitle").textContent = subtitle;
}

function updateTimelineTicks() {
    const container = document.getElementById("timelineTicks");
    if (!container) return;
    const activePeriod = currentPeriodKey();
    container.innerHTML = state.periods
        .map(
            (period) =>
                `<span class="timeline-tick ${period === activePeriod ? "is-active" : ""}">${period}</span>`,
        )
        .join("");
}

function updateOpacityValue() {
    const opacity = Number(document.getElementById("opacity").value);
    document.getElementById("opacityValue").textContent = `${Math.round(opacity * 100)}%`;
}

function updateTopbarMeta() {
    if (!state.summary) return;
    document.getElementById("statusText").textContent = state.historicalSnapshots.length
        ? "Ready"
        : "Ready · Wayback limited";
    document.getElementById("promptText").textContent =
        `${currentMetricOption()?.label ?? "Change"} · ${currentPeriodKey()} lookback`;
}

function updateDashboardChrome() {
    if (!state.summary) return;
    updateLocationChrome();
    populateHistoricalImagerySelect();
    updateTopbarMeta();
    updateFocusPanel();
    updateTrendChart();
    updateFrameDetails();
    updateFrameBadge();
    updateTimelineTicks();
    updateOpacityValue();
    syncUrlState();
}

// ── Wayback / historical imagery ──────────────────────────────────────────────

function snapshotDateKey(snapshot) {
    if (!snapshot?.date) return null;
    try { return snapshot.date.toISOString().split("T")[0]; } catch { return null; }
}

function buildWaybackTileUrl(snapshot) {
    return snapshot.itemURL
        .replace("{level}", "{z}")
        .replace("{row}", "{y}")
        .replace("{col}", "{x}");
}

function parseWaybackSnapshots(config) {
    return Object.entries(config)
        .map(([releaseNum, info]) => {
            const match = info?.itemTitle?.match(WAYBACK_DATE_REGEX);
            if (!match) return null;
            const date = new Date(match[1]);
            if (Number.isNaN(date.getTime())) return null;
            return {
                releaseNum: Number.parseInt(releaseNum, 10),
                date,
                tileUrl: buildWaybackTileUrl(info),
                title: info.itemTitle,
            };
        })
        .filter(Boolean)
        .sort((a, b) => b.date - a.date);
}

function historicalPlaybackSnapshots() {
    const startYear = timelineMatchYear();
    const endYear = state.summary?.config?.base_year;
    if (
        startYear === null ||
        endYear === undefined ||
        Number.isNaN(startYear) ||
        startYear > endYear
    ) return [];
    return state.historicalSnapshots
        .filter((snapshot) => {
            const year = snapshot.date.getFullYear();
            return year >= startYear && year <= endYear;
        })
        .slice()
        .sort((a, b) => a.date - b.date);
}

function currentHistoricalYear() {
    if (!state.summary) return null;
    const selectedSnapshot = selectedHistoricalSnapshot();
    if (selectedSnapshot) return selectedSnapshot.date.getFullYear();
    if (state.playbackFrameYear !== null) return state.playbackFrameYear;
    const mode = state.historicalMode;
    if (mode === "base") return state.summary.config.base_year;
    if (mode === "timeline")
        return state.summary.config.base_year - Number.parseInt(currentPeriodKey(), 10);
    return null;
}

function timelineMatchYear() {
    if (!state.summary) return null;
    return state.summary.config.base_year - Number.parseInt(currentPeriodKey(), 10);
}

function snapshotForYear(year) {
    if (year === null || year === undefined || !state.historicalSnapshots.length) return null;
    const sameYearSnapshots = state.historicalSnapshots.filter(
        (snapshot) => snapshot.date.getFullYear() === year,
    );
    if (sameYearSnapshots.length) return sameYearSnapshots[0];
    const targetDate = new Date(year, 6, 1);
    return state.historicalSnapshots.reduce((best, snapshot) => {
        if (!best) return snapshot;
        return Math.abs(snapshot.date - targetDate) < Math.abs(best.date - targetDate)
            ? snapshot
            : best;
    }, null);
}

function formatSnapshotDate(snapshot) {
    if (!snapshot) return null;
    return snapshot.date.toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
    });
}

function selectedHistoricalSnapshot() {
    const snapshots = historicalPlaybackSnapshots();
    if (
        state.selectedHistoricalSnapshotIndex === null ||
        !snapshots[state.selectedHistoricalSnapshotIndex]
    ) return null;
    return snapshots[state.selectedHistoricalSnapshotIndex];
}

function currentHistoricalSnapshot() {
    if (!state.historicalSnapshots.length) return null;
    const chosenSnapshot = selectedHistoricalSnapshot();
    if (chosenSnapshot) return chosenSnapshot;
    const year = currentHistoricalYear();
    if (year === null) return null;
    return snapshotForYear(year);
}

function currentHistoricalPlayerIndex() {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) return 0;
    if (state.selectedHistoricalSnapshotIndex !== null && snapshots[state.selectedHistoricalSnapshotIndex]) {
        return state.selectedHistoricalSnapshotIndex;
    }
    if (state.historicalMode === "timeline") return 0;
    if (state.historicalMode === "base") return snapshots.length - 1;
    return snapshots.length - 1;
}

function isHistoricalImageryActive() {
    return (
        state.historicalMode !== "off" ||
        state.playbackFrameYear !== null ||
        state.selectedHistoricalSnapshotIndex !== null
    );
}

// ── Player timeline UI ────────────────────────────────────────────────────────

function syncHistoricalPlayerRange(updateValue = true) {
    const snapshots = historicalPlaybackSnapshots();
    const frameInput = document.getElementById("historicalFrame");
    if (!frameInput) return;

    const ticksContainer = document.getElementById("playerTicks");
    const minLabel = document.getElementById("playerMinYear");
    const maxLabel = document.getElementById("playerMaxYear");
    const tooltip = document.getElementById("playerTooltip");

    if (snapshots.length > 1) {
        const minTime = snapshots[0].date.getTime();
        const maxTime = snapshots[snapshots.length - 1].date.getTime();

        frameInput.min = minTime;
        frameInput.max = maxTime;

        const currentIndex = currentHistoricalPlayerIndex();
        const currentSnapshot = snapshots[currentIndex] || snapshots[snapshots.length - 1];

        if (updateValue) {
            const newValue = String(currentSnapshot.date.getTime());
            if (frameInput.value !== newValue) frameInput.value = newValue;
        }

        minLabel.textContent = snapshots[0].date.getFullYear();
        maxLabel.textContent = snapshots[snapshots.length - 1].date.getFullYear();

        if (ticksContainer.children.length !== snapshots.length || frameInput.dataset.mapped !== "true") {
            ticksContainer.innerHTML = "";
            snapshots.forEach((snap, index) => {
                const tick = document.createElement("div");
                tick.className = "player-tick";
                tick.dataset.index = String(index);
                const ratio = maxTime > minTime ? (snap.date.getTime() - minTime) / (maxTime - minTime) : 0;
                tick.style.left = `${ratio * 100}%`;
                try { tick.dataset.date = snap.date.toISOString().split("T")[0]; } catch { tick.dataset.date = ""; }

                tick.addEventListener("click", () => {
                    stopHistoricalPlayback({ refresh: false });
                    setHistoricalFrame(index, { activate: true, refresh: true, updateSliderValue: true });
                });
                tick.addEventListener("mouseenter", () => {
                    const dateText = tick.dataset.date || "";
                    if (dateText) {
                        tooltip.textContent = dateText;
                        tooltip.style.left = `calc(${ratio * 100}% + 8px)`;
                        tooltip.style.opacity = "1";
                    }
                });
                tick.addEventListener("mouseleave", () => {
                    try {
                        const curIndex = currentHistoricalPlayerIndex();
                        const curSnapshot = snapshots[curIndex] || snapshots[snapshots.length - 1];
                        if (curSnapshot) {
                            tooltip.textContent = curSnapshot.date.toISOString().split("T")[0];
                            const curRatio = maxTime > minTime ? (curSnapshot.date.getTime() - minTime) / (maxTime - minTime) : 0;
                            tooltip.style.left = `calc(${curRatio * 100}% + 8px)`;
                            tooltip.style.opacity = "1";
                            return;
                        }
                        tooltip.style.opacity = "0";
                    } catch { tooltip.style.opacity = "0"; }
                });
                ticksContainer.appendChild(tick);
            });
            frameInput.dataset.mapped = "true";
        }

        const ratio = maxTime > minTime ? (currentSnapshot.date.getTime() - minTime) / (maxTime - minTime) : 0;
        tooltip.textContent = currentSnapshot.date.toISOString().split("T")[0];
        tooltip.style.left = `calc(${ratio * 100}% + 8px)`;
        tooltip.style.opacity = "1";

        try {
            Array.from(ticksContainer.children).forEach((child) => {
                const idx = Number(child.dataset.index);
                child.classList.toggle("is-active", !Number.isNaN(idx) && idx === currentIndex);
            });
        } catch {}
    } else {
        frameInput.min = 0;
        frameInput.max = 0;
        frameInput.value = 0;
        ticksContainer.innerHTML = "";
        minLabel.textContent = "";
        maxLabel.textContent = "";
        tooltip.style.opacity = "0";
        frameInput.dataset.mapped = "false";
    }
}

function setHistoricalFrame(index, { activate = true, refresh = true, updateSliderValue = true } = {}) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) return;

    const clampedIndex = Math.max(0, Math.min(index, snapshots.length - 1));
    const selectedSnapshot = snapshots[clampedIndex];
    state.selectedHistoricalSnapshotIndex = clampedIndex;
    state.playbackFrameYear = selectedSnapshot.date.getFullYear();

    if (activate) {
        if (state.historicalMode === "off") {
            state.historicalMode = "timeline";
            document.getElementById("historicalImagery").value = "timeline";
        }
        applyBasemap("esri_imagery");
    } else {
        updateBasemapControlState();
    }

    syncHistoricalPlayerRange(updateSliderValue);
    if (refresh) applyHistoricalImagery(true);
}

function closestHistoricalSnapshotIndex(targetTime) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) return null;
    let closestIndex = 0;
    let minDiff = Infinity;
    snapshots.forEach((snap, idx) => {
        const diff = Math.abs(snap.date.getTime() - targetTime);
        if (diff < minDiff) { minDiff = diff; closestIndex = idx; }
    });
    return closestIndex;
}

function historicalFrameTargetTimeFromPointer(frameInput, clientX) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) return null;
    const minTime = snapshots[0].date.getTime();
    const maxTime = snapshots[snapshots.length - 1].date.getTime();
    const rect = frameInput.getBoundingClientRect();
    if (rect.width <= 0) return minTime;
    const ratio = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    return minTime + ratio * (maxTime - minTime);
}

function resetHistoricalFrameSelection() {
    state.selectedHistoricalSnapshotIndex = null;
    state.playbackFrameYear = null;
    syncHistoricalPlayerRange();
}

function updateHistoricalImageryLabel() {
    const label = document.getElementById("historicalImageryLabel");
    if (!state.historicalSnapshots.length) {
        label.textContent = "Wayback snapshots are loading from the remote ESRI archive.";
        return;
    }
    const year = currentHistoricalYear();
    if (year === null) { label.textContent = "Wayback historical imagery is hidden."; return; }
    const snapshot = currentHistoricalSnapshot();
    if (!snapshot) { label.textContent = `No Wayback snapshot was found for ${year}.`; return; }
    const snapshotLabel = formatSnapshotDate(snapshot);
    if (state.playbackTimerId !== null) {
        label.textContent = `Playback year ${year}. Showing Wayback snapshot ${snapshotLabel}.`;
        return;
    }
    label.textContent = `Showing Wayback snapshot ${snapshotLabel} for ${year}.`;
}

function updateHistoricalPlaybackButton() {
    const playButton = document.getElementById("historicalPlayback");
    const backButton = document.getElementById("historicalStepBack");
    const forwardButton = document.getElementById("historicalStepForward");
    const speedSelect = document.getElementById("historicalSpeed");
    const frameInput = document.getElementById("historicalFrame");
    const frameLabel = document.getElementById("historicalFrameLabel");
    const snapshots = historicalPlaybackSnapshots();
    const canPlay = !fileProtocol && snapshots.length > 1;
    const playerIndex = currentHistoricalPlayerIndex();
    const currentSnapshot = snapshots[playerIndex] ?? currentHistoricalSnapshot();
    const currentLabel = formatSnapshotDate(currentSnapshot) ?? "Historical imagery off";

    playButton.disabled = !canPlay;
    playButton.classList.toggle("is-playing", state.playbackTimerId !== null);
    backButton.disabled = !snapshots.length || playerIndex <= 0;
    forwardButton.disabled = !snapshots.length || playerIndex >= snapshots.length - 1;
    frameInput.disabled = !snapshots.length || fileProtocol;
    speedSelect.disabled = !canPlay;
    speedSelect.value = String(state.playbackSpeedMs);

    if (!canPlay) {
        playButton.textContent = "▶";
        frameLabel.textContent = fileProtocol
            ? "Serve over http(s) to use Wayback playback"
            : currentLabel;
        syncHistoricalPlayerRange();
        return;
    }

    playButton.textContent = state.playbackTimerId !== null ? "❚❚" : "▶";
    frameLabel.textContent =
        state.historicalMode === "off" && state.playbackFrameYear === null
            ? "Historical imagery off"
            : `${currentLabel}${playerIndex === snapshots.length - 1 ? " (Base Year)" : ""}`;
    syncHistoricalPlayerRange();
}

// ── Playback controls ─────────────────────────────────────────────────────────

function stepHistoricalFrame(direction) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) return;
    stopHistoricalPlayback({ refresh: false });
    const nextIndex = Math.max(0, Math.min(currentHistoricalPlayerIndex() + direction, snapshots.length - 1));
    setHistoricalFrame(nextIndex, { activate: true, refresh: true });
}

function stopHistoricalPlayback({ refresh = true, resetSelection = false } = {}) {
    if (state.playbackTimerId !== null) {
        window.clearTimeout(state.playbackTimerId);
        state.playbackTimerId = null;
    }
    if (resetSelection) resetHistoricalFrameSelection();
    if (isHistoricalImageryActive()) {
        updateBasemapControlState();
    } else {
        applyBasemap(state.preferredBasemapMode);
    }
    updateHistoricalPlaybackButton();
    if (refresh) applyHistoricalImagery(true);
}

function queuePlaybackFrame(index) {
    const snapshots = historicalPlaybackSnapshots();
    if (index >= snapshots.length) {
        state.playbackTimerId = null;
        state.historicalMode = "base";
        document.getElementById("historicalImagery").value = "base";
        setHistoricalFrame(snapshots.length - 1, { activate: true, refresh: true });
        updateHistoricalPlaybackButton();
        return;
    }
    setHistoricalFrame(index, { activate: true, refresh: true });
    state.playbackTimerId = window.setTimeout(() => {
        queuePlaybackFrame(index + 1);
        updateHistoricalPlaybackButton();
    }, state.playbackSpeedMs);
    updateHistoricalPlaybackButton();
}

function startHistoricalPlayback() {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length || fileProtocol) { updateHistoricalPlaybackButton(); return; }
    if (state.historicalMode === "off") {
        state.historicalMode = "timeline";
        document.getElementById("historicalImagery").value = "timeline";
    }
    stopHistoricalPlayback({ refresh: false });
    applyBasemap("esri_imagery");
    const startIndex = state.selectedHistoricalSnapshotIndex !== null ? currentHistoricalPlayerIndex() : 0;
    queuePlaybackFrame(startIndex);
}

// ── MapLibre layer management ─────────────────────────────────────────────────

function applyBasemap(mode) {
    const normalizedMode = basemapDefinitions[mode] ? mode : "none";
    const resolvedMode = isHistoricalImageryActive() ? "esri_imagery" : normalizedMode;
    state.basemapMode = fileProtocol ? "none" : resolvedMode;

    if (!map.loaded()) return;

    for (const key of Object.keys(maplibreBasemapTiles)) {
        if (map.getLayer(`basemap-${key}`)) {
            map.setLayoutProperty(
                `basemap-${key}`,
                "visibility",
                key === state.basemapMode ? "visible" : "none",
            );
        }
    }

    map.getContainer().classList.toggle("no-basemap", state.basemapMode === "none");

    if (state.summary) {
        document.getElementById("subtitle").textContent = buildSubtitle(state.summary);
    }

    updateBasemapControlState();
    updateDashboardChrome();
}

function updateBasemapControlState() {
    const basemapSelect = document.getElementById("basemap");
    if (!basemapSelect) return;
    const historicalActive = isHistoricalImageryActive();
    basemapSelect.disabled = historicalActive;
    basemapSelect.value =
        historicalActive && !fileProtocol ? "esri_imagery" : state.basemapMode;
}

function applyHistoricalImagery(force = false) {
    const nextSnapshot = currentHistoricalSnapshot();
    const nextKey = nextSnapshot
        ? `${nextSnapshot.releaseNum}:${nextSnapshot.date.toISOString()}`
        : null;

    if (
        !force &&
        state.activeHistoricalSnapshotKey === nextKey &&
        !(nextKey === null && map.getLayer("historical"))
    ) {
        updateHistoricalImageryLabel();
        return;
    }

    if (!map.loaded()) { updateHistoricalImageryLabel(); return; }

    if (map.getLayer("historical")) map.removeLayer("historical");
    if (map.getSource("historical")) map.removeSource("historical");

    state.activeHistoricalSnapshotKey = nextKey;

    if (!nextSnapshot) {
        updateHistoricalImageryLabel();
        return;
    }

    const insertBefore = map.getLayer("overlay-fill")
        ? "overlay-fill"
        : map.getLayer("ward-fill")
          ? "ward-fill"
          : undefined;

    map.addSource("historical", {
        type: "raster",
        tiles: [nextSnapshot.tileUrl],
        tileSize: 256,
        // this magic date came from analysis of Wayback snapshots
        maxzoom: nextSnapshot.date < new Date("2021-07-01") ? 17 : 18,
        attribution: "© ESRI Wayback · Imagery © respective owners",
    });
    map.addLayer(
        { id: "historical", type: "raster", source: "historical" },
        insertBefore,
    );

    updateHistoricalImageryLabel();
    updateDashboardChrome();
}

function getActiveOverlaySourceId() {
    return currentUnitKey() === "wards" ? "ward-overlay" : "overlay";
}

function getActiveOverlaySourceLayer() {
    return currentUnitKey() === "wards" ? "ward_overlay" : "overlay";
}

function getActiveFillLayerId() {
    return currentUnitKey() === "wards" ? "ward-fill" : "overlay-fill";
}

function getActiveLineLayerId() {
    return currentUnitKey() === "wards" ? "ward-line" : "overlay-line";
}

function renderOverlayLayer() {
    if (!map.loaded()) return;

    const unitKey = currentUnitKey();

    // Toggle cells/wards layer visibility
    if (map.getLayer("overlay-fill")) {
        const v = unitKey === "cells" ? "visible" : "none";
        map.setLayoutProperty("overlay-fill", "visibility", v);
        map.setLayoutProperty("overlay-line", "visibility", v);
    }
    if (map.getLayer("ward-fill")) {
        const v = unitKey === "wards" ? "visible" : "none";
        map.setLayoutProperty("ward-fill", "visibility", v);
        map.setLayoutProperty("ward-line", "visibility", v);
    }

    // Clear hover state on unit switch
    if (state.hoveredCellsId !== null) {
        try {
            map.setFeatureState({ source: "overlay", sourceLayer: "overlay", id: state.hoveredCellsId }, { hovered: false });
        } catch {}
        state.hoveredCellsId = null;
    }
    if (state.hoveredWardsId !== null) {
        try {
            map.setFeatureState({ source: "ward-overlay", sourceLayer: "ward_overlay", id: state.hoveredWardsId }, { hovered: false });
        } catch {}
        state.hoveredWardsId = null;
    }
    hideTooltip();

    updateOverlayPaint();
}

function updateOverlayPaint() {
    const fillLayerId = getActiveFillLayerId();
    const lineLayerId = getActiveLineLayerId();

    if (!map.getLayer(fillLayerId)) return;

    const isWard = currentUnitKey() === "wards";
    const key = propertyKey();
    const opacity = Number(document.getElementById("opacity").value);

    map.setPaintProperty(fillLayerId, "fill-color", buildColorExpression());
    map.setPaintProperty(fillLayerId, "fill-opacity", [
        "case",
        ["==", ["typeof", ["get", key]], "number"],
        opacity,
        0,
    ]);

    map.setPaintProperty(lineLayerId, "line-color", buildLineColorExpression(isWard));
    map.setPaintProperty(lineLayerId, "line-width", buildLineWidthExpression(isWard));
}

function updateLayer() {
    // DOM-only chrome updates run unconditionally
    updateLegend();
    updateSummaryCards();
    updateDashboardChrome();

    if (!map.isStyleLoaded()) return;

    const periodLabel = document.getElementById("periodLabel");
    if (periodLabel) periodLabel.textContent = currentPeriodKey();

    updateTimelineMarkerPosition();
    updateOverlayPaint();
    updateHistoricalPlaybackButton();
    applyHistoricalImagery();
}

// ── Hover interaction ─────────────────────────────────────────────────────────

function setupHoverInteraction() {
    tooltipEl = document.createElement("div");
    tooltipEl.id = "mapTooltip";
    tooltipEl.className = "map-tooltip-container";
    tooltipEl.style.display = "none";
    map.getContainer().appendChild(tooltipEl);

    function onFeatureMove(sourceId, sourceLayer, stateKey, event) {
        if (!event.features || event.features.length === 0) return;
        const feat = event.features[0];
        if (feat.id === undefined || feat.id === null) return;

        if (state[stateKey] !== null && state[stateKey] !== feat.id) {
            try {
                map.setFeatureState({ source: sourceId, sourceLayer, id: state[stateKey] }, { hovered: false });
            } catch {}
        }
        if (state[stateKey] !== feat.id) {
            state[stateKey] = feat.id;
            try {
                map.setFeatureState({ source: sourceId, sourceLayer, id: feat.id }, { hovered: true });
            } catch {}
        }

        showTooltip(event.point, feat.properties);
    }

    function onFeatureLeave(sourceId, sourceLayer, stateKey) {
        if (state[stateKey] !== null) {
            try {
                map.setFeatureState({ source: sourceId, sourceLayer, id: state[stateKey] }, { hovered: false });
            } catch {}
            state[stateKey] = null;
        }
        hideTooltip();
    }

    map.on("mousemove", "overlay-fill", (e) =>
        onFeatureMove("overlay", "overlay", "hoveredCellsId", e));
    map.on("mouseleave", "overlay-fill", () =>
        onFeatureLeave("overlay", "overlay", "hoveredCellsId"));

    map.on("mousemove", "ward-fill", (e) =>
        onFeatureMove("ward-overlay", "ward_overlay", "hoveredWardsId", e));
    map.on("mouseleave", "ward-fill", () =>
        onFeatureLeave("ward-overlay", "ward_overlay", "hoveredWardsId"));

    map.on("mousemove", (e) => positionTooltip(e.point));
}

// ── Select / control population ───────────────────────────────────────────────

function updateMetricSelect() {
    const metricSelect = document.getElementById("metric");
    const previousValue = metricSelect.value;
    const options = availableMetricOptions();
    metricSelect.innerHTML = options
        .map((metric) => `<option value="${metric.key}">${metric.label}</option>`)
        .join("");
    if (options.some((metric) => metric.key === previousValue)) {
        metricSelect.value = previousValue;
    } else if (options.length) {
        metricSelect.value = options[0].key;
    }
}

function populateBasemapSelect() {
    const basemapSelect = document.getElementById("basemap");
    const availableModes = fileProtocol ? ["none"] : ["osm", "carto_light", "esri_imagery", "none"];
    basemapSelect.innerHTML = availableModes
        .map((mode) => `<option value="${mode}">${basemapDefinitions[mode].label}</option>`)
        .join("");
    updateBasemapControlState();
}

function populateHistoricalImagerySelect() {
    const historicalSelect = document.getElementById("historicalImagery");
    const timelineSnapshot = snapshotForYear(timelineMatchYear());
    const baseSnapshot = snapshotForYear(state.summary?.config?.base_year);
    const options = state.historicalSnapshots.length
        ? [
              { key: "off", label: "Off" },
              {
                  key: "timeline",
                  label: timelineSnapshot
                      ? `Timeline Match (${formatSnapshotDate(timelineSnapshot)})`
                      : "Timeline Match",
              },
              {
                  key: "base",
                  label: baseSnapshot
                      ? `${formatSnapshotDate(baseSnapshot)} (Base Year)`
                      : `Base Year (${state.summary.config.base_year})`,
              },
          ]
        : [{ key: "off", label: "Off" }];
    historicalSelect.innerHTML = options
        .map((option) => `<option value="${option.key}">${option.label}</option>`)
        .join("");
    if (!options.some((option) => option.key === state.historicalMode)) {
        state.historicalMode = "off";
    }
    historicalSelect.value = state.historicalMode;
    updateHistoricalPlaybackButton();
    updateHistoricalImageryLabel();
}

function populateUnitSelect() {
    const unitSelect = document.getElementById("unit");
    const options = availableUnitOptions();
    const previousValue = unitSelect.value;
    unitSelect.innerHTML = options
        .map((option) => `<option value="${option.key}">${option.label}</option>`)
        .join("");
    if (!previousValue && options.some((o) => o.key === "cells")) {
        unitSelect.value = "cells";
    } else if (options.some((o) => o.key === previousValue)) {
        unitSelect.value = previousValue;
    } else if (options.length) {
        unitSelect.value = options[0].key;
    }
}

function applyInitialControlState() {
    const unitSelect = document.getElementById("unit");
    const metricSelect = document.getElementById("metric");
    const basemapSelect = document.getElementById("basemap");
    const historicalSelect = document.getElementById("historicalImagery");
    const periodInput = document.getElementById("period");
    const opacityInput = document.getElementById("opacity");
    const speedSelect = document.getElementById("historicalSpeed");

    if (initialUrlState.unit && availableUnitOptions().some((o) => o.key === initialUrlState.unit)) {
        unitSelect.value = initialUrlState.unit;
    }
    if (periodInput && state.periods.includes(initialUrlState.period)) {
        periodInput.value = String(state.periods.indexOf(initialUrlState.period));
    }

    updateMetricSelect();
    if (initialUrlState.metric && availableMetricOptions().some((o) => o.key === initialUrlState.metric)) {
        metricSelect.value = initialUrlState.metric;
    }

    if (initialUrlState.opacity !== null) opacityInput.value = String(initialUrlState.opacity);
    if (initialUrlState.basemap) {
        state.preferredBasemapMode = initialUrlState.basemap;
        basemapSelect.value = initialUrlState.basemap;
    }
    if (initialUrlState.historicalMode) {
        state.historicalMode = initialUrlState.historicalMode;
        historicalSelect.value = initialUrlState.historicalMode;
    }
    if (initialUrlState.speed !== null) state.playbackSpeedMs = initialUrlState.speed;
    speedSelect.value = String(state.playbackSpeedMs);
}

function applyInitialHistoricalState() {
    const historicalSelect = document.getElementById("historicalImagery");
    const availableModes = Array.from(historicalSelect.options).map((o) => o.value);

    if (!availableModes.includes(state.historicalMode)) state.historicalMode = "off";
    historicalSelect.value = state.historicalMode;

    if (state.historicalMode === "off") {
        resetHistoricalFrameSelection();
        applyBasemap(state.preferredBasemapMode);
        applyHistoricalImagery(true);
        return;
    }

    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        applyBasemap("esri_imagery");
        applyHistoricalImagery(true);
        return;
    }

    let targetIndex = state.historicalMode === "base" ? snapshots.length - 1 : 0;
    if (initialUrlState.historicalSnapshotDate) {
        const requestedIndex = snapshots.findIndex(
            (snapshot) => snapshotDateKey(snapshot) === initialUrlState.historicalSnapshotDate,
        );
        if (requestedIndex >= 0) targetIndex = requestedIndex;
    }

    setHistoricalFrame(targetIndex, { activate: true, refresh: true, updateSliderValue: true });
}

// ── Data loading ──────────────────────────────────────────────────────────────

async function fetchJson(url, { optional = false } = {}) {
    try {
        const response = await fetch(url);
        if (!response.ok) {
            if (optional) return null;
            throw new Error(`Failed to load ${url}: ${response.status} ${response.statusText}`);
        }
        return await response.json();
    } catch (error) {
        if (optional) return null;
        throw error;
    }
}

async function loadData() {
    updateLocationChrome();
    setLoadingState(true, "Loading summary, overlays, boundary, and Wayback imagery configuration.");
    setLoadingProgress(0);

    // Fetch lightweight resources + wait for map to be ready
    const [, summary, overlayMeta, boundary, wardOverlayMeta, waybackConfig] = await Promise.all([
        mapReadyPromise,
        fetchJson("summary.json"),
        fetchJson("overlay.meta.json"),
        fetchJson("boundary.geojson", { optional: true }),
        fetchJson("ward_overlay.meta.json", { optional: true }),
        fetchJson(WAYBACK_CONFIG_URL, { optional: true }),
    ]);

    setLoadingProgress(0.5);

    // Populate state
    state.summary = summary;
    state.overlayMeta.cells = overlayMeta;
    state.hasWards = !!wardOverlayMeta;
    state.overlayMeta.wards = wardOverlayMeta;
    state.boundsByProperty.cells = overlayMeta?.boundsByProperty ?? {};
    state.boundsByProperty.wards = wardOverlayMeta?.boundsByProperty ?? {};
    state.historicalSnapshots = waybackConfig ? parseWaybackSnapshots(waybackConfig) : [];
    state.periods = summary.config.periods.map((value) => `${value}y`);

    // Populate UI controls
    populateBasemapSelect();
    populateHistoricalImagerySelect();
    populateUnitSelect();
    applyInitialControlState();

    updateLocationChrome();
    document.getElementById("title").textContent = currentAreaLabel();
    document.getElementById("subtitle").textContent = buildSubtitle(summary);

    const periodSlider = document.getElementById("period");
    if (periodSlider) {
        periodSlider.max = Math.max(state.periods.length - 1, 0);
    }
    updateTimelineMarkerPosition();
    updateMetricSelect();

    setLoadingProgress(0.65);

    // ── Set up MapLibre layers ────────────────────────────────────────────────

    // Dark background layer (always present so map is never blank)
    map.addLayer({
        id: "background",
        type: "background",
        paint: { "background-color": "#09111a" },
    });

    // Basemap raster layers (all added; only one visible at a time)
    for (const [key, srcSpec] of Object.entries(maplibreBasemapTiles)) {
        map.addSource(`basemap-${key}`, { type: "raster", ...srcSpec });
        map.addLayer({
            id: `basemap-${key}`,
            type: "raster",
            source: `basemap-${key}`,
            layout: { visibility: "none" },
        });
    }

    // Overlay (cells) PMTiles source + fill/line layers
    map.addSource("overlay", {
        type: "vector",
        url: `pmtiles://${OVERLAY_PMTILES_URL}`,
    });

    const initOpacity = Number(document.getElementById("opacity").value);
    const initKey = propertyKey();
    const initColors = buildColorExpression();
    const isInitWard = currentUnitKey() === "wards";

    map.addLayer({
        id: "overlay-fill",
        type: "fill",
        source: "overlay",
        "source-layer": "overlay",
        paint: {
            "fill-color": initColors,
            "fill-opacity": [
                "case",
                ["==", ["typeof", ["get", initKey]], "number"],
                initOpacity,
                0,
            ],
        },
        layout: { visibility: isInitWard ? "none" : "visible" },
    });
    map.addLayer({
        id: "overlay-line",
        type: "line",
        source: "overlay",
        "source-layer": "overlay",
        paint: {
            "line-color": buildLineColorExpression(false),
            "line-width": buildLineWidthExpression(false),
        },
        layout: { visibility: isInitWard ? "none" : "visible" },
    });

    // Ward overlay (if available)
    if (state.hasWards) {
        map.addSource("ward-overlay", {
            type: "vector",
            url: `pmtiles://${WARD_OVERLAY_PMTILES_URL}`,
        });
        map.addLayer({
            id: "ward-fill",
            type: "fill",
            source: "ward-overlay",
            "source-layer": "ward_overlay",
            paint: {
                "fill-color": buildColorExpression(),
                "fill-opacity": [
                    "case",
                    ["==", ["typeof", ["get", initKey]], "number"],
                    initOpacity,
                    0,
                ],
            },
            layout: { visibility: isInitWard ? "visible" : "none" },
        });
        map.addLayer({
            id: "ward-line",
            type: "line",
            source: "ward-overlay",
            "source-layer": "ward_overlay",
            paint: {
                "line-color": buildLineColorExpression(true),
                "line-width": buildLineWidthExpression(true),
            },
            layout: { visibility: isInitWard ? "visible" : "none" },
        });
    }

    // Boundary (GeoJSON, interactive: false)
    if (boundary) {
        map.addSource("boundary", { type: "geojson", data: boundary });
        map.addLayer({
            id: "boundary",
            type: "line",
            source: "boundary",
            paint: {
                "line-color": "#17222d",
                "line-width": 2,
                "line-opacity": 0.8,
                "line-dasharray": [7, 5],
            },
        });
    }

    setLoadingProgress(0.8);

    // ── Set up hover interaction ──────────────────────────────────────────────
    setupHoverInteraction();

    // ── Fit map to data ───────────────────────────────────────────────────────
    const meta = state.overlayMeta.cells;
    if (
        initialUrlState.lat !== null &&
        initialUrlState.lng !== null &&
        initialUrlState.zoom !== null
    ) {
        map.jumpTo({
            center: [initialUrlState.lng, initialUrlState.lat],
            zoom: initialUrlState.zoom,
        });
    } else if (meta?.bbox) {
        const [minLng, minLat, maxLng, maxLat] = meta.bbox;
        map.fitBounds([[minLng, minLat], [maxLng, maxLat]], { padding: 48, duration: 0 });
    } else {
        map.jumpTo({ center: [78.9629, 20.5937], zoom: 5 });
    }

    // ── Apply basemap and historical initial state ────────────────────────────
    applyBasemap(state.preferredBasemapMode);
    applyInitialHistoricalState();
    updateMapTelemetry();
    updateLayer();

    state.urlSyncEnabled = true;
    syncUrlState();
    setLoadingProgress(1);
    window.requestAnimationFrame(() => setLoadingState(false));
}

// ── Timeline marker (period slider) ──────────────────────────────────────────

const timelineGradientContainer = document.getElementById("timelineGradientContainer") ||
    document.querySelector(".timeline-gradient-container");
const timelineMarker = document.getElementById("timelineMarker");
const periodInput = document.getElementById("period");

function updateTimelineMarkerPosition() {
    if (!timelineMarker || !periodInput) return;
    const min = parseFloat(periodInput.min) || 0;
    const max = parseFloat(periodInput.max) || 100;
    const value = parseFloat(periodInput.value) || 0;
    const progress = max > min ? ((value - min) / (max - min)) * 100 : 0;
    const containerWidth = timelineGradientContainer?.offsetWidth || 0;
    const markerWidth = 18;
    const leftPx = (containerWidth * progress / 100) - (markerWidth / 2);
    timelineMarker.style.marginLeft = `${leftPx}px`;
}

if (timelineGradientContainer && periodInput) {
    timelineGradientContainer.addEventListener("click", (event) => {
        const containerRect = timelineGradientContainer.getBoundingClientRect();
        const clickX = event.clientX - containerRect.left;
        const containerWidth = containerRect.width;
        const min = parseFloat(periodInput.min) || 0;
        const max = parseFloat(periodInput.max) || 100;
        const progress = Math.max(0, Math.min(1, clickX / containerWidth));
        const newValue = Math.round(min + (max - min) * progress);
        periodInput.value = newValue;
        periodInput.dispatchEvent(new Event("change", { bubbles: true }));
    });

    timelineGradientContainer.addEventListener("keydown", (event) => {
        const min = parseFloat(periodInput.min) || 0;
        const max = parseFloat(periodInput.max) || 100;
        const currentValue = parseFloat(periodInput.value) || 0;
        let newValue = currentValue;
        if (event.key === "ArrowLeft") { event.preventDefault(); newValue = Math.max(min, currentValue - 1); }
        else if (event.key === "ArrowRight") { event.preventDefault(); newValue = Math.min(max, currentValue + 1); }
        else if (event.key === "Home") { event.preventDefault(); newValue = min; }
        else if (event.key === "End") { event.preventDefault(); newValue = max; }
        if (newValue !== currentValue) {
            periodInput.value = newValue;
            periodInput.dispatchEvent(new Event("change", { bubbles: true }));
        }
    });

    timelineGradientContainer.setAttribute("tabindex", "0");
    timelineGradientContainer.setAttribute("role", "slider");
    timelineGradientContainer.setAttribute("aria-label", "Timeline window");
}

// ── Range input progress styling ──────────────────────────────────────────────

function updateRangeProgress(input) {
    const min = parseFloat(input.min) || 0;
    const max = parseFloat(input.max) || 100;
    const value = parseFloat(input.value) || 0;
    const progress = max > min ? ((value - min) / (max - min)) * 100 : 0;
    input.style.setProperty("--range-progress", `${progress}%`);
}

document.querySelectorAll('input[type="range"]').forEach((input) => {
    if (input.id === "period") return;
    updateRangeProgress(input);
    input.addEventListener("input", () => updateRangeProgress(input));

    const descriptor = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value");
    if (descriptor) {
        const originalSet = descriptor.set;
        Object.defineProperty(input, "value", {
            set: function (val) {
                const result = originalSet.call(this, val);
                updateRangeProgress(this);
                return result;
            },
            get: descriptor.get,
        });
    }
    const maxDescriptor = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "max");
    if (maxDescriptor) {
        const originalSetMax = maxDescriptor.set;
        Object.defineProperty(input, "max", {
            set: function (val) {
                const result = originalSetMax.call(this, val);
                updateRangeProgress(this);
                return result;
            },
            get: maxDescriptor.get,
        });
    }
});

// ── Map event listeners ───────────────────────────────────────────────────────

map.on("moveend", updateMapTelemetry);
map.on("zoomend", updateMapTelemetry);

// ── Control event listeners ───────────────────────────────────────────────────

document.getElementById("unit").addEventListener("change", () => {
    updateMetricSelect();
    renderOverlayLayer();
    updateLayer();
});

document.getElementById("metric").addEventListener("change", () => {
    updateMetricSelect();
    updateLayer();
});

document.getElementById("basemap").addEventListener("change", (event) => {
    state.preferredBasemapMode = event.target.value;
    applyBasemap(event.target.value);
});

document.getElementById("historicalImagery").addEventListener("change", (event) => {
    state.historicalMode = event.target.value;
    stopHistoricalPlayback({ refresh: false, resetSelection: state.historicalMode === "off" });
    if (state.historicalMode === "off") {
        applyBasemap(state.preferredBasemapMode);
        applyHistoricalImagery(true);
    } else {
        const snapshots = historicalPlaybackSnapshots();
        const targetIndex = state.historicalMode === "base" ? snapshots.length - 1 : 0;
        if (targetIndex >= 0) {
            setHistoricalFrame(targetIndex, { activate: true, refresh: true, updateSliderValue: true });
        } else {
            applyBasemap("esri_imagery");
            applyHistoricalImagery(true);
        }
    }
    updateHistoricalPlaybackButton();
    updateLayer();
});

document.getElementById("historicalPlayback").addEventListener("click", () => {
    if (state.playbackTimerId !== null) { stopHistoricalPlayback(); return; }
    startHistoricalPlayback();
});

document.getElementById("historicalStepBack").addEventListener("click", () => stepHistoricalFrame(-1));
document.getElementById("historicalStepForward").addEventListener("click", () => stepHistoricalFrame(1));

document.getElementById("historicalFrame").addEventListener("pointerdown", (event) => {
    if (event.button !== 0 && event.pointerType !== "touch") return;
    const frameInput = event.currentTarget;
    const targetTime = historicalFrameTargetTimeFromPointer(frameInput, event.clientX);
    if (targetTime === null) return;
    const closestIndex = closestHistoricalSnapshotIndex(targetTime);
    if (closestIndex === null) return;
    stopHistoricalPlayback({ refresh: false });
    setHistoricalFrame(closestIndex, { activate: true, refresh: true, updateSliderValue: true });
});

document.getElementById("historicalFrame").addEventListener("input", (event) => {
    stopHistoricalPlayback({ refresh: false });
    const targetTime = Number(event.target.value);
    const closestIndex = closestHistoricalSnapshotIndex(targetTime);
    if (closestIndex === null) return;
    setHistoricalFrame(closestIndex, { activate: true, refresh: true, updateSliderValue: true });
});

document.getElementById("historicalSpeed").addEventListener("input", (event) => {
    state.playbackSpeedMs = normalizePlaybackSpeedMs(event.target.value) ?? state.playbackSpeedMs;
    if (state.playbackTimerId !== null) { stopHistoricalPlayback({ refresh: false }); startHistoricalPlayback(); }
    syncUrlState();
});

if (periodInput) {
    periodInput.addEventListener("change", () => {
        stopHistoricalPlayback({ refresh: false, resetSelection: true });
        updateLayer();
        updateTimelineMarkerPosition();
    });
}

const opacityInput = document.getElementById("opacity");
opacityInput.addEventListener("input", updateLayer);
opacityInput.addEventListener("change", updateLayer);

// ── Bootstrap ─────────────────────────────────────────────────────────────────

loadData().catch((error) => {
    updateLocationChrome();
    setLoadingState(false, `The ${currentAreaLabel()} analysis could not be loaded.`);
    document.getElementById("title").textContent = `Could not load ${currentAreaLabel()} analysis`;
    document.getElementById("subtitle").textContent =
        `${error.message}. Serve the output directory with a local web server, such as ` +
        "`python -m http.server`, instead of opening this page directly from disk.";
    console.error(error);
});

updateLocationChrome();
