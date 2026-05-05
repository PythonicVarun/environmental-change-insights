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

const basemapDefinitions = {
    none: {
        label: "No Basemap",
    },
    osm: {
        label: "OpenStreetMap",
        url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        options: {
            attribution: "&copy; OpenStreetMap contributors",
            maxZoom: 19,
        },
    },
    carto_light: {
        label: "Carto Light",
        url: "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        options: {
            attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
            maxZoom: 20,
            subdomains: "abcd",
        },
    },
    esri_imagery: {
        label: "Esri Imagery",
        url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        options: {
            attribution: "Tiles &copy; Esri",
            maxZoom: 19,
        },
    },
};

const searchParams = new URLSearchParams(window.location.search);
const fileProtocol = window.location.protocol === "file:";
const requestedBasemapMode = searchParams.get("basemap");
const initialBasemapMode = fileProtocol
    ? "none"
    : basemapDefinitions[requestedBasemapMode]
      ? requestedBasemapMode
      : "osm";

function inferLocationSlug(pathname = window.location.pathname) {
    const segments = pathname.split("/").filter(Boolean);
    if (!segments.length) {
        return "";
    }

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

const fallbackLocationLabel =
    humanizeLocationSlug(inferLocationSlug()) || "Location";

const map = L.map("map", { zoomControl: true, preferCanvas: true });
map.createPane("historicalPane");
map.getPane("historicalPane").style.zIndex = 250;
map.getPane("historicalPane").style.pointerEvents = "none";

const state = {
    summary: null,
    overlays: {
        cells: null,
        wards: null,
    },
    geoLayer: null,
    boundaryLayer: null,
    basemapLayer: null,
    historicalLayer: null,
    historicalMode: "off",
    historicalSnapshots: [],
    activeHistoricalSnapshotKey: null,
    playbackTimerId: null,
    playbackFrameYear: null,
    selectedHistoricalSnapshotIndex: null,
    playbackSpeedMs: 1200,
    basemapMode: initialBasemapMode,
    preferredBasemapMode: initialBasemapMode,
    boundsByProperty: {
        cells: {},
        wards: {},
    },
    periods: [],
};

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

function setLoadingState(isLoading, message) {
    if (message) {
        document.getElementById("loadingMessage").textContent = message;
    }
    document.body.classList.toggle("is-loading", isLoading);
}

function interpolateColor(colors, t) {
    const clamp = Math.max(0, Math.min(1, t));
    const [c1, c2, c3] = colors.map(hexToRgb);
    if (clamp <= 0.5) {
        return rgbToHex(lerpColor(c1, c2, clamp * 2));
    }
    return rgbToHex(lerpColor(c2, c3, (clamp - 0.5) * 2));
}

function lerpColor(a, b, t) {
    return {
        r: Math.round(a.r + (b.r - a.r) * t),
        g: Math.round(a.g + (b.g - a.g) * t),
        b: Math.round(a.b + (b.b - a.b) * t),
    };
}

function hexToRgb(hex) {
    const clean = hex.replace("#", "");
    return {
        r: parseInt(clean.slice(0, 2), 16),
        g: parseInt(clean.slice(2, 4), 16),
        b: parseInt(clean.slice(4, 6), 16),
    };
}

function rgbToHex({ r, g, b }) {
    return `#${[r, g, b].map((v) => v.toString(16).padStart(2, "0")).join("")}`;
}

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
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "n/a";
    }
    return `${Number(value).toFixed(digits)}%`;
}

function currentPeriodKey() {
    const slider = document.getElementById("period");
    if (!slider) {
        return state.periods[0];
    }
    return state.periods[Number(slider.value)] ?? state.periods[0];
}

function currentMetricKey() {
    return document.getElementById("metric").value;
}

function currentUnitKey() {
    return document.getElementById("unit").value || "cells";
}

function activeOverlay() {
    return state.overlays[currentUnitKey()] ?? state.overlays.cells;
}

function activeBoundsByProperty() {
    return state.boundsByProperty[currentUnitKey()] ?? {};
}

function propertyKey() {
    return `${currentMetricKey()}_${currentPeriodKey()}`;
}

function computeFeatureStyle(feature, hovered = false) {
    const key = propertyKey();
    const metric = currentMetricKey();
    const value = feature.properties[key];
    const isWard = currentUnitKey() === "wards";
    if (value === null || value === undefined || Number.isNaN(value)) {
        return {
            color: isWard ? "rgba(23,34,45,0.12)" : "transparent",
            fillColor: "transparent",
            fillOpacity: 0,
            weight: isWard ? 0.6 : 0,
        };
    }

    const opacity = Number(document.getElementById("opacity").value);
    const bounds = activeBoundsByProperty()[key];
    const min = bounds?.min ?? 0;
    const max = bounds?.max ?? 1;
    let t;
    if (metric === "embedding_change") {
        t = (value - min) / Math.max(max - min, 1e-9);
    } else {
        const span = Math.max(Math.abs(min), Math.abs(max), 1e-9);
        t = (value + span) / (2 * span);
    }

    return {
        color: hovered
            ? "rgba(23,34,45,0.72)"
            : isWard
              ? "rgba(23,34,45,0.22)"
              : "rgba(23,34,45,0)",
        weight: hovered ? (isWard ? 1.5 : 0.9) : isWard ? 0.7 : 0,
        fillColor: interpolateColor(metricColorStops[metric], t),
        fillOpacity: opacity,
    };
}

function updateLegend() {
    const key = propertyKey();
    const metric = currentMetricKey();
    const bounds = activeBoundsByProperty()[key] ?? { min: 0, max: 0 };
    const legend = document.getElementById("legendScale");
    legend.style.background = `linear-gradient(90deg, ${metricColorStops[metric][0]} 0%, ${metricColorStops[metric][1]} 50%, ${metricColorStops[metric][2]} 100%)`;
    document.getElementById("legendMin").textContent =
        metric === "population_delta"
            ? formatPopulation(bounds.min, 1)
            : formatNumber(bounds.min);
    document.getElementById("legendMax").textContent =
        metric === "population_delta"
            ? formatPopulation(bounds.max, 1)
            : formatNumber(bounds.max);
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
        entries.push([
            "Mean Pollution",
            formatNumber(metrics.pollution_delta_mean ?? 0),
        ]);
    }
    if (metrics.population_delta_total !== undefined) {
        entries.push([
            "Population Delta",
            formatPopulation(metrics.population_delta_total ?? 0),
        ]);
        entries.push([
            "Population %",
            formatPercent(metrics.population_pct_change_total, 1),
        ]);
    }
    if (state.summary.metadata.ward_overlay_available) {
        entries.push([
            "Ward Units",
            String(state.summary.metadata.ward_count ?? 0),
        ]);
    }
    grid.innerHTML = entries
        .map(
            ([label, value]) => `
        <div class="stat">
          <div class="stat-label">${label}</div>
          <div class="stat-value">${value}</div>
        </div>
      `,
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
                spot.pollution_delta !== undefined &&
                spot.pollution_delta !== null
                    ? `<br />Pollution ${formatNumber(spot.pollution_delta)}`
                    : ""
            }
            ${
                spot.population_delta !== undefined
                    ? `<br />Pop ${formatPopulation(spot.population_delta)} | ${formatPercent(spot.population_pct_change, 1)}`
                    : ""
            }
          </div>
        </div>
      `,
        )
        .join("");
}

function computeBoundsByProperty(features) {
    const propertyBounds = {};
    for (const metric of metricOptions) {
        for (const period of state.periods) {
            const key = `${metric.key}_${period}`;
            const values = features
                .map((feature) => feature.properties[key])
                .filter(
                    (value) =>
                        value !== null &&
                        value !== undefined &&
                        !Number.isNaN(value),
                );
            if (!values.length) {
                continue;
            }
            propertyBounds[key] = {
                min: Math.min(...values),
                max: Math.max(...values),
            };
        }
    }
    return propertyBounds;
}

function availableMetricOptions() {
    const boundsByProperty = activeBoundsByProperty();
    return metricOptions.filter((metric) =>
        state.periods.some(
            (period) => boundsByProperty[`${metric.key}_${period}`],
        ),
    );
}

function currentMetricOption() {
    return (
        metricOptions.find((metric) => metric.key === currentMetricKey()) ??
        metricOptions[0]
    );
}

function summaryMetricValue(periodKey = currentPeriodKey(), metricKey = currentMetricKey()) {
    const config = metricSummaryDefinitions[metricKey];
    if (!config) {
        return null;
    }

    return state.summary?.periods?.[periodKey]?.metrics?.[config.summaryKey] ?? null;
}

function availableUnitOptions() {
    const options = [{ key: "cells", label: "Cells" }];
    if (state.overlays.wards?.features?.length) {
        options.push({ key: "wards", label: "Wards" });
    }
    return options;
}

function isHistoricalImageryActive() {
    return (
        state.historicalMode !== "off" ||
        state.playbackFrameYear !== null ||
        state.selectedHistoricalSnapshotIndex !== null
    );
}

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

    if (!fileProtocol && state.basemapMode !== "none") {
        return baseText;
    }

    return (
        `${baseText} Basemap is off because this page is running from ` +
        `\`file://\` or was opened with \`?basemap=none\`. ` +
        `Serve the folder over http(s), like \`python -m http.server\`, to use remote basemaps with a valid Referer.`
    );
}

function currentHistoricalYear() {
    if (!state.summary) {
        return null;
    }
    const selectedSnapshot = selectedHistoricalSnapshot();
    if (selectedSnapshot) {
        return selectedSnapshot.date.getFullYear();
    }
    if (state.playbackFrameYear !== null) {
        return state.playbackFrameYear;
    }
    const mode = state.historicalMode;
    if (mode === "base") {
        return state.summary.config.base_year;
    }
    if (mode === "timeline") {
        return state.summary.config.base_year - Number.parseInt(currentPeriodKey(), 10);
    }
    return null;
}

function timelineMatchYear() {
    if (!state.summary) {
        return null;
    }
    return state.summary.config.base_year - Number.parseInt(currentPeriodKey(), 10);
}

function snapshotForYear(year) {
    if (year === null || year === undefined || !state.historicalSnapshots.length) {
        return null;
    }

    const sameYearSnapshots = state.historicalSnapshots.filter(
        (snapshot) => snapshot.date.getFullYear() === year,
    );
    if (sameYearSnapshots.length) {
        return sameYearSnapshots[0];
    }

    const targetDate = new Date(year, 6, 1);
    return state.historicalSnapshots.reduce((best, snapshot) => {
        if (!best) {
            return snapshot;
        }
        return Math.abs(snapshot.date - targetDate) < Math.abs(best.date - targetDate)
            ? snapshot
            : best;
    }, null);
}

function formatSnapshotDate(snapshot) {
    if (!snapshot) {
        return null;
    }
    return snapshot.date.toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
    });
}

function historicalPlaybackSnapshots() {
    const startYear = timelineMatchYear();
    const endYear = state.summary?.config?.base_year;
    if (
        startYear === null ||
        endYear === undefined ||
        Number.isNaN(startYear) ||
        startYear > endYear
    ) {
        return [];
    }

    return state.historicalSnapshots
        .filter((snapshot) => {
            const year = snapshot.date.getFullYear();
            return year >= startYear && year <= endYear;
        })
        .slice()
        .sort((a, b) => a.date - b.date);
}

function selectedHistoricalSnapshot() {
    const snapshots = historicalPlaybackSnapshots();
    if (
        state.selectedHistoricalSnapshotIndex === null ||
        !snapshots[state.selectedHistoricalSnapshotIndex]
    ) {
        return null;
    }
    return snapshots[state.selectedHistoricalSnapshotIndex];
}

function currentHistoricalPlayerIndex() {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        return 0;
    }
    if (
        state.selectedHistoricalSnapshotIndex !== null &&
        snapshots[state.selectedHistoricalSnapshotIndex]
    ) {
        return state.selectedHistoricalSnapshotIndex;
    }

    if (state.historicalMode === "timeline") {
        return 0;
    }
    if (state.historicalMode === "base") {
        return snapshots.length - 1;
    }
    return snapshots.length - 1;
}

function syncHistoricalPlayerRange(updateValue = true) {
    const snapshots = historicalPlaybackSnapshots();
    const frameInput = document.getElementById("historicalFrame");
    if (!frameInput) {
        return;
    }

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
            if (frameInput.value !== newValue) {
                frameInput.value = newValue;
            }
        }

        minLabel.textContent = snapshots[0].date.getFullYear();
        maxLabel.textContent = snapshots[snapshots.length - 1].date.getFullYear();

        if (ticksContainer.children.length !== snapshots.length || frameInput.dataset.mapped !== "true") {
            ticksContainer.innerHTML = '';
            snapshots.forEach((snap, index) => {
                const tick = document.createElement("div");
                tick.className = "player-tick";
                tick.dataset.index = String(index);
                const ratio = maxTime > minTime ? (snap.date.getTime() - minTime) / (maxTime - minTime) : 0;
                tick.style.left = `${ratio * 100}%`;

                try {
                    tick.dataset.date = snap.date.toISOString().split('T')[0];
                } catch (e) {
                    tick.dataset.date = '';
                }

                tick.addEventListener("click", () => {
                    stopHistoricalPlayback({ refresh: false });
                    setHistoricalFrame(index, {
                        activate: true,
                        refresh: true,
                        updateSliderValue: true
                    });
                });

                tick.addEventListener('mouseenter', (evt) => {
                    const dateText = tick.dataset.date || '';
                    if (dateText) {
                        tooltip.textContent = dateText;
                        const left = `calc(${ratio * 100}% + 8px)`;
                        tooltip.style.left = left;
                        tooltip.style.opacity = '1';
                    }
                });
                tick.addEventListener('mouseleave', () => {
                    try {
                        const curIndex = currentHistoricalPlayerIndex();
                        const curSnapshot = snapshots[curIndex] || snapshots[snapshots.length - 1];
                        if (curSnapshot) {
                            tooltip.textContent = curSnapshot.date.toISOString().split('T')[0];
                            const curRatio = maxTime > minTime ? (curSnapshot.date.getTime() - minTime) / (maxTime - minTime) : 0;
                            tooltip.style.left = `calc(${curRatio * 100}% + 8px)`;
                            tooltip.style.opacity = '1';
                            return;
                        }
                        tooltip.style.opacity = '0';
                    } catch (e) {
                        tooltip.style.opacity = '0';
                    }
                });

                ticksContainer.appendChild(tick);
            });
            frameInput.dataset.mapped = "true";
        }

        const ratio = maxTime > minTime ? (currentSnapshot.date.getTime() - minTime) / (maxTime - minTime) : 0;
        tooltip.textContent = currentSnapshot.date.toISOString().split('T')[0];
        tooltip.style.left = `calc(${ratio * 100}% + 8px)`;
        tooltip.style.opacity = "1";

        try {
            const currentIndex = currentHistoricalPlayerIndex();
            Array.from(ticksContainer.children).forEach((child) => {
                const idx = Number(child.dataset.index);
                if (!Number.isNaN(idx) && idx === currentIndex) {
                    child.classList.add('is-active');
                } else {
                    child.classList.remove('is-active');
                }
            });
        } catch (e) {
            // ignore if ticksContainer not present or dataset values unexpected
        }
    } else {
        frameInput.min = 0;
        frameInput.max = 0;
        frameInput.value = 0;
        ticksContainer.innerHTML = '';
        minLabel.textContent = '';
        maxLabel.textContent = '';
        tooltip.style.opacity = "0";
        frameInput.dataset.mapped = "false";
    }
}

function setHistoricalFrame(index, { activate = true, refresh = true, updateSliderValue = true } = {}) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        return;
    }

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
    if (refresh) {
        applyHistoricalImagery(true);
    }
}

function closestHistoricalSnapshotIndex(targetTime) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        return null;
    }

    let closestIndex = 0;
    let minDiff = Infinity;
    snapshots.forEach((snap, idx) => {
        const diff = Math.abs(snap.date.getTime() - targetTime);
        if (diff < minDiff) {
            minDiff = diff;
            closestIndex = idx;
        }
    });

    return closestIndex;
}

function historicalFrameTargetTimeFromPointer(frameInput, clientX) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        return null;
    }

    const minTime = snapshots[0].date.getTime();
    const maxTime = snapshots[snapshots.length - 1].date.getTime();
    const rect = frameInput.getBoundingClientRect();
    if (rect.width <= 0) {
        return minTime;
    }

    const ratio = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    return minTime + ratio * (maxTime - minTime);
}

function resetHistoricalFrameSelection() {
    state.selectedHistoricalSnapshotIndex = null;
    state.playbackFrameYear = null;
    syncHistoricalPlayerRange();
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
            if (!match) {
                return null;
            }

            const date = new Date(match[1]);
            if (Number.isNaN(date.getTime())) {
                return null;
            }

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

function currentHistoricalSnapshot() {
    if (!state.historicalSnapshots.length) {
        return null;
    }

    const chosenSnapshot = selectedHistoricalSnapshot();
    if (chosenSnapshot) {
        return chosenSnapshot;
    }

    const year = currentHistoricalYear();
    if (year === null) {
        return null;
    }
    return snapshotForYear(year);
}

function updateHistoricalImageryLabel() {
    const label = document.getElementById("historicalImageryLabel");
    if (!state.historicalSnapshots.length) {
        label.textContent = "Wayback snapshots are loading from the remote ESRI archive.";
        return;
    }

    const year = currentHistoricalYear();
    if (year === null) {
        label.textContent = "Wayback historical imagery is hidden.";
        return;
    }

    const snapshot = currentHistoricalSnapshot();
    if (!snapshot) {
        label.textContent = `No Wayback snapshot was found for ${year}.`;
        return;
    }

    const snapshotLabel = formatSnapshotDate(snapshot);
    if (state.playbackTimerId !== null) {
        label.textContent =
            `Playback year ${year}. Showing Wayback snapshot ${snapshotLabel}.`;
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
    forwardButton.disabled =
        !snapshots.length || playerIndex >= snapshots.length - 1;
    frameInput.disabled = !snapshots.length || fileProtocol;
    speedSelect.disabled = !canPlay;
    speedSelect.value = String(state.playbackSpeedMs);

    if (!canPlay) {
        playButton.textContent = "\u25b6";
        frameLabel.textContent = fileProtocol
            ? "Serve over http(s) to use Wayback playback"
            : currentLabel;
        syncHistoricalPlayerRange();
        return;
    }

    playButton.textContent = state.playbackTimerId !== null ? "\u275a\u275a" : "\u25b6";
    frameLabel.textContent =
        state.historicalMode === "off" && state.playbackFrameYear === null
            ? "Historical imagery off"
            : `${currentLabel}${playerIndex === snapshots.length - 1 ? " (Base Year)" : ""}`;
    syncHistoricalPlayerRange();
}

function updateFocusPanel() {
    if (!state.summary) {
        return;
    }

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
    if (!state.summary) {
        container.innerHTML = "";
        return;
    }

    const config = metricSummaryDefinitions[currentMetricKey()];
    const series = state.periods.map((period) => ({
        period,
        value: summaryMetricValue(period),
    }));
    const values = series
        .map((entry) => entry.value)
        .filter((value) => value !== null && !Number.isNaN(value));

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
        if (max === min) {
            return height / 2;
        }
        return height - ((value - min) / (max - min)) * (height - 10) - 5;
    };

    const polyline = series
        .map((entry, index) => `${index * xStep},${yFor(entry.value ?? min)}`)
        .join(" ");
    const area = `0,${height} ${polyline} ${width},${height}`;
    const points = series
        .map((entry, index) => {
            const x = index * xStep;
            const y = yFor(entry.value ?? min);
            const isActive = entry.period === currentPeriodKey();
            return `<circle cx="${x}" cy="${y}" r="${isActive ? 4.5 : 3}" fill="${isActive ? "#00f0c8" : "#13d4ff"}" />`;
        })
        .join("");
    const labels = series
        .map((entry, index) => {
            const x = index * xStep;
            return `<text x="${x}" y="${height + 16}" text-anchor="${index === 0 ? "start" : index === series.length - 1 ? "end" : "middle"}" fill="#70829f" font-size="10">${entry.period}</text>`;
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
    if (!state.summary) {
        return;
    }

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
    document.getElementById("timelineYearLabel").textContent =
        String(timelineMatchYear() ?? "--");
    document.getElementById("activeFrameLabel").textContent = frameLabel;
    document.getElementById("coverageLabel").textContent = formatPercent(
        state.summary.metadata.coverage_percent,
        1,
    );
}

function updateFrameBadge() {
    if (!state.summary) {
        return;
    }

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
    if (!container) {
        return;
    }
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
    document.getElementById("opacityValue").textContent =
        `${Math.round(opacity * 100)}%`;
}

function updateTopbarMeta() {
    if (!state.summary) {
        return;
    }

    document.getElementById("statusText").textContent = state.historicalSnapshots.length
        ? "Ready"
        : "Ready · Wayback limited";
    document.getElementById("promptText").textContent =
        `${currentMetricOption()?.label ?? "Change"} · ${currentPeriodKey()} lookback`;
}

function updateDashboardChrome() {
    if (!state.summary) {
        return;
    }

    updateLocationChrome();
    populateHistoricalImagerySelect();
    updateTopbarMeta();
    updateFocusPanel();
    updateTrendChart();
    updateFrameDetails();
    updateFrameBadge();
    updateTimelineTicks();
    updateOpacityValue();
}

function updateMapTelemetry() {
    const center = map.getCenter();
    document.getElementById("telemetryLat").textContent = center.lat.toFixed(4);
    document.getElementById("telemetryLng").textContent = center.lng.toFixed(4);
    document.getElementById("telemetryZoom").textContent = map.getZoom().toFixed(1);
}

function stepHistoricalFrame(direction) {
    const snapshots = historicalPlaybackSnapshots();
    if (!snapshots.length) {
        return;
    }

    stopHistoricalPlayback({ refresh: false });
    const nextIndex = Math.max(
        0,
        Math.min(currentHistoricalPlayerIndex() + direction, snapshots.length - 1),
    );
    setHistoricalFrame(nextIndex, { activate: true, refresh: true });
}

function stopHistoricalPlayback({ refresh = true, resetSelection = false } = {}) {
    if (state.playbackTimerId !== null) {
        window.clearTimeout(state.playbackTimerId);
        state.playbackTimerId = null;
    }
    if (resetSelection) {
        resetHistoricalFrameSelection();
    }
    if (isHistoricalImageryActive()) {
        updateBasemapControlState();
    } else {
        applyBasemap(state.preferredBasemapMode);
    }
    updateHistoricalPlaybackButton();
    if (refresh) {
        applyHistoricalImagery(true);
    }
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
    if (!snapshots.length || fileProtocol) {
        updateHistoricalPlaybackButton();
        return;
    }

    if (state.historicalMode === "off") {
        state.historicalMode = "timeline";
        document.getElementById("historicalImagery").value = "timeline";
    }
    stopHistoricalPlayback({ refresh: false });
    applyBasemap("esri_imagery");
    const startIndex =
        state.selectedHistoricalSnapshotIndex !== null
            ? currentHistoricalPlayerIndex()
            : 0;
    queuePlaybackFrame(startIndex);
}

function handleFeatureMouseOver(event) {
    event.target.setStyle(computeFeatureStyle(event.target.feature, true));
    if (event.target.bringToFront) {
        event.target.bringToFront();
    }
}

function handleFeatureMouseOut(event) {
    if (state.geoLayer) {
        state.geoLayer.resetStyle(event.target);
    }
}

function applyBasemap(mode) {
    const normalizedMode = basemapDefinitions[mode] ? mode : "none";
    const resolvedMode = isHistoricalImageryActive() ? "esri_imagery" : normalizedMode;
    state.basemapMode = fileProtocol ? "none" : resolvedMode;
    map.attributionControl.removeAttribution("Basemap disabled");

    if (state.basemapLayer) {
        map.removeLayer(state.basemapLayer);
        state.basemapLayer = null;
    }

    if (state.basemapMode === "none") {
        map.getContainer().classList.add("no-basemap");
        map.attributionControl.addAttribution("Basemap disabled");
    } else {
        map.getContainer().classList.remove("no-basemap");
        const definition = basemapDefinitions[state.basemapMode];
        state.basemapLayer = L.tileLayer(
            definition.url,
            definition.options,
        ).addTo(map);
    }

    if (state.summary) {
        document.getElementById("subtitle").textContent = buildSubtitle(
            state.summary,
        );
    }

    updateBasemapControlState();
    updateDashboardChrome();
}

function updateBasemapControlState() {
    const basemapSelect = document.getElementById("basemap");
    if (!basemapSelect) {
        return;
    }

    const historicalActive = isHistoricalImageryActive();
    basemapSelect.disabled = historicalActive;
    basemapSelect.value =
        historicalActive && !fileProtocol
            ? "esri_imagery"
            : state.basemapMode;
}

function applyHistoricalImagery(force = false) {
    const nextSnapshot = currentHistoricalSnapshot();
    const nextKey = nextSnapshot
        ? `${nextSnapshot.releaseNum}:${nextSnapshot.date.toISOString()}`
        : null;
    if (
        !force &&
        state.activeHistoricalSnapshotKey === nextKey &&
        !(nextKey === null && state.historicalLayer)
    ) {
        updateHistoricalImageryLabel();
        return;
    }

    if (state.historicalLayer) {
        map.removeLayer(state.historicalLayer);
        state.historicalLayer = null;
    }

    state.activeHistoricalSnapshotKey = nextKey;
    if (!nextSnapshot) {
        updateHistoricalImageryLabel();
        return;
    }

    state.historicalLayer = L.tileLayer(nextSnapshot.tileUrl, {
        pane: "historicalPane",
        maxZoom: 19,
        crossOrigin: true,
        attribution: "© ESRI Wayback · Imagery © respective owners",
    }).addTo(map);
    updateHistoricalImageryLabel();
    updateDashboardChrome();
}

function renderOverlayLayer() {
    const overlay = activeOverlay();
    if (!overlay) {
        return;
    }

    if (state.geoLayer) {
        map.removeLayer(state.geoLayer);
    }

    state.geoLayer = L.geoJSON(overlay, {
        style: (feature) => computeFeatureStyle(feature, false),
        onEachFeature: (feature, layer) => {
            layer.bindTooltip(() => buildTooltip(feature.properties), {
                sticky: true,
            });
            layer.on({
                mouseover: handleFeatureMouseOver,
                mouseout: handleFeatureMouseOut,
            });
        },
    }).addTo(map);
}

function updateMetricSelect() {
    const metricSelect = document.getElementById("metric");
    const previousValue = metricSelect.value;
    const options = availableMetricOptions();
    metricSelect.innerHTML = options
        .map(
            (metric) =>
                `<option value="${metric.key}">${metric.label}</option>`,
        )
        .join("");
    if (options.some((metric) => metric.key === previousValue)) {
        metricSelect.value = previousValue;
    } else if (options.length) {
        metricSelect.value = options[0].key;
    }
}



function updateLayer() {
    if (!state.geoLayer) {
        return;
    }
    const periodLabel = document.getElementById("periodLabel");
    if (periodLabel) {
        periodLabel.textContent = currentPeriodKey();
    }
    updateTimelineMarkerPosition();
    state.geoLayer.setStyle((feature) => computeFeatureStyle(feature, false));
    updateHistoricalPlaybackButton();
    applyHistoricalImagery();
    updateLegend();
    updateSummaryCards();
    updateDashboardChrome();
}

function populateBasemapSelect() {
    const basemapSelect = document.getElementById("basemap");
    const availableModes = fileProtocol
        ? ["none"]
        : ["osm", "carto_light", "esri_imagery", "none"];
    basemapSelect.innerHTML = availableModes
        .map(
            (mode) =>
                `<option value="${mode}">${basemapDefinitions[mode].label}</option>`,
        )
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
        .map(
            (option) =>
                `<option value="${option.key}">${option.label}</option>`,
        )
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
        .map(
            (option) =>
                `<option value="${option.key}">${option.label}</option>`,
        )
        .join("");
    if (!previousValue && options.some((option) => option.key === "cells")) {
        unitSelect.value = "cells";
    } else if (options.some((option) => option.key === previousValue)) {
        unitSelect.value = previousValue;
    } else if (options.length) {
        unitSelect.value = options[0].key;
    }
}

async function loadData() {
    updateLocationChrome();
    setLoadingState(
        true,
        "Loading summary, overlays, boundary, and Wayback imagery configuration.",
    );
    const [summary, overlay, boundary, wardOverlay, waybackConfig] = await Promise.all([
        fetch("summary.json").then((response) => response.json()),
        fetch("overlay.geojson").then((response) => response.json()),
        fetch("boundary.geojson")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
        fetch("ward_overlay.geojson")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
        fetch(WAYBACK_CONFIG_URL)
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
    ]);

    state.summary = summary;
    state.overlays.cells = overlay;
    state.overlays.wards = wardOverlay?.features?.length ? wardOverlay : null;
    state.historicalSnapshots = waybackConfig ? parseWaybackSnapshots(waybackConfig) : [];
    state.periods = summary.config.periods.map((value) => `${value}y`);
    state.boundsByProperty.cells = computeBoundsByProperty(overlay.features);
    state.boundsByProperty.wards = state.overlays.wards
        ? computeBoundsByProperty(state.overlays.wards.features)
        : {};

    populateBasemapSelect();
    populateHistoricalImagerySelect();
    populateUnitSelect();

    updateLocationChrome();
    document.getElementById("title").textContent = currentAreaLabel();
    document.getElementById("subtitle").textContent = buildSubtitle(summary);

    const periodSlider = document.getElementById("period");
    if (periodSlider) {
        periodSlider.max = Math.max(state.periods.length - 1, 0);
    }
    updateTimelineMarkerPosition();

    updateMetricSelect();
    applyBasemap(state.basemapMode);
    renderOverlayLayer();
    applyHistoricalImagery(true);

    if (boundary) {
        state.boundaryLayer = L.geoJSON(boundary, {
            interactive: false,
            style: {
                color: "#17222d",
                weight: 2,
                opacity: 0.8,
                dashArray: "7 5",
                fillOpacity: 0,
            },
        }).addTo(map);
    }

    const bounds = state.boundaryLayer
        ? state.boundaryLayer.getBounds()
        : state.geoLayer.getBounds();
    if (bounds.isValid()) {
        map.fitBounds(bounds.pad(0.08));
    } else {
        map.setView([20.5937, 78.9629], 5);
    }

    updateMapTelemetry();
    updateLayer();
    window.requestAnimationFrame(() => setLoadingState(false));
}

map.on("moveend zoomend", updateMapTelemetry);

document.getElementById("metric").addEventListener("change", (event) => {
    updateMetricSelect();
    updateLayer();
});

document.getElementById("basemap").addEventListener("change", (event) => {
    state.preferredBasemapMode = event.target.value;
    applyBasemap(event.target.value);
});

document
    .getElementById("historicalImagery")
    .addEventListener("change", (event) => {
        state.historicalMode = event.target.value;
        stopHistoricalPlayback({ refresh: false, resetSelection: false });
        if (state.historicalMode === "off") {
            applyBasemap(state.preferredBasemapMode);
            applyHistoricalImagery(true);
        } else {
            const snapshots = historicalPlaybackSnapshots();
            const targetIndex =
                state.historicalMode === "base" ? snapshots.length - 1 : 0;

            if (targetIndex >= 0) {
                setHistoricalFrame(targetIndex, {
                    activate: true,
                    refresh: true,
                    updateSliderValue: true,
                });
            } else {
                applyBasemap("esri_imagery");
                applyHistoricalImagery(true);
            }
        }
        updateHistoricalPlaybackButton();
        updateLayer();
    });

document
    .getElementById("historicalPlayback")
    .addEventListener("click", () => {
        if (state.playbackTimerId !== null) {
            stopHistoricalPlayback();
            return;
        }
        startHistoricalPlayback();
    });

document
    .getElementById("historicalStepBack")
    .addEventListener("click", () => {
        stepHistoricalFrame(-1);
    });

document
    .getElementById("historicalStepForward")
    .addEventListener("click", () => {
        stepHistoricalFrame(1);
    });

document
    .getElementById("historicalFrame")
    .addEventListener("pointerdown", (event) => {
        if (event.button !== 0 && event.pointerType !== "touch") {
            return;
        }

        const frameInput = event.currentTarget;
        const targetTime = historicalFrameTargetTimeFromPointer(frameInput, event.clientX);
        if (targetTime === null) {
            return;
        }

        const closestIndex = closestHistoricalSnapshotIndex(targetTime);
        if (closestIndex === null) {
            return;
        }

        stopHistoricalPlayback({ refresh: false });
        setHistoricalFrame(closestIndex, {
            activate: true,
            refresh: true,
            updateSliderValue: true
        });
    });

document
    .getElementById("historicalFrame")
    .addEventListener("input", (event) => {
        stopHistoricalPlayback({ refresh: false });
        const targetTime = Number(event.target.value);
        const closestIndex = closestHistoricalSnapshotIndex(targetTime);
        if (closestIndex === null) return;

        setHistoricalFrame(closestIndex, {
            activate: true,
            refresh: true,
            updateSliderValue: true
        });
    });

document
    .getElementById("historicalSpeed")
    .addEventListener("input", (event) => {
        const value = Number(event.target.value);
        if (value === 0) {
            state.historicalPlaybackSpeedMs = 1200;
        } else if (value === 1) {
            state.historicalPlaybackSpeedMs = 800;
        } else if (value === 2) {
            state.historicalPlaybackSpeedMs = 400;
        } else if (value === 3) {
            state.historicalPlaybackSpeedMs = 100;
        }
        if (state.playbackTimerId !== null) {
            stopHistoricalPlayback({ refresh: false });
            startHistoricalPlayback();
        }
    });

const periodInput = document.getElementById("period");
if (periodInput) {
    periodInput.addEventListener("change", () => {
        stopHistoricalPlayback({ refresh: false, resetSelection: true });
        updateLayer();
        updateTimelineMarkerPosition();
    });
}

// Gradient timeline line interaction
const timelineGradientContainer = document.getElementById("timelineGradientContainer") ||
    document.querySelector(".timeline-gradient-container");
const timelineMarker = document.getElementById("timelineMarker");

function updateTimelineMarkerPosition() {
    if (!timelineMarker || !periodInput) return;

    const min = parseFloat(periodInput.min) || 0;
    const max = parseFloat(periodInput.max) || 100;
    const value = parseFloat(periodInput.value) || 0;
    const progress = max > min ? ((value - min) / (max - min)) * 100 : 0;

    // Position marker from left, accounting for marker width
    const containerWidth = timelineGradientContainer?.offsetWidth || 0;
    const markerWidth = 18; // marker width in pixels
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

    // Keyboard navigation
    timelineGradientContainer.addEventListener("keydown", (event) => {
        const min = parseFloat(periodInput.min) || 0;
        const max = parseFloat(periodInput.max) || 100;
        const currentValue = parseFloat(periodInput.value) || 0;
        let newValue = currentValue;

        if (event.key === "ArrowLeft") {
            event.preventDefault();
            newValue = Math.max(min, currentValue - 1);
        } else if (event.key === "ArrowRight") {
            event.preventDefault();
            newValue = Math.min(max, currentValue + 1);
        } else if (event.key === "Home") {
            event.preventDefault();
            newValue = min;
        } else if (event.key === "End") {
            event.preventDefault();
            newValue = max;
        }

        if (newValue !== currentValue) {
            periodInput.value = newValue;
            periodInput.dispatchEvent(new Event("change", { bubbles: true }));
        }
    });

    // Make container focusable for keyboard navigation
    timelineGradientContainer.setAttribute("tabindex", "0");
    timelineGradientContainer.setAttribute("role", "slider");
    timelineGradientContainer.setAttribute("aria-label", "Timeline window");
}

const opacityInput = document.getElementById("opacity");
opacityInput.addEventListener("input", updateLayer);
opacityInput.addEventListener("change", updateLayer);

function updateRangeProgress(input) {
    const min = parseFloat(input.min) || 0;
    const max = parseFloat(input.max) || 100;
    const value = parseFloat(input.value) || 0;
    const progress = max > min ? ((value - min) / (max - min)) * 100 : 0;

    input.style.setProperty("--range-progress", `${progress}%`);
}

document.querySelectorAll('input[type="range"]').forEach(input => {
    if (input.id === "period") return; // Skip period input, use gradient line instead

    updateRangeProgress(input);
    input.addEventListener('input', () => updateRangeProgress(input));

    const descriptor = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
    if (descriptor) {
        const originalSet = descriptor.set;
        Object.defineProperty(input, 'value', {
            set: function(val) {
                const result = originalSet.call(this, val);
                updateRangeProgress(this);
                return result;
            },
            get: descriptor.get
        });
    }
    const maxDescriptor = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'max');
    if (maxDescriptor) {
        const originalSetMax = maxDescriptor.set;
        Object.defineProperty(input, 'max', {
            set: function(val) {
                const result = originalSetMax.call(this, val);
                updateRangeProgress(this);
                return result;
            },
            get: maxDescriptor.get
        });
    }
});

loadData().catch((error) => {
    updateLocationChrome();
    setLoadingState(
        false,
        `The ${currentAreaLabel()} analysis could not be loaded. See the details in the page message below.`,
    );
    document.getElementById("title").textContent =
        `Could not load ${currentAreaLabel()} analysis`;
    document.getElementById("subtitle").textContent =
        `${error.message}. Serve the output directory with a local web server, such as ` +
        "`python -m http.server`, instead of opening this page directly from disk.";
    console.error(error);
});

updateLocationChrome();
