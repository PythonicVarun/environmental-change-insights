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

const map = L.map("map", { zoomControl: true, preferCanvas: true });
map.createPane("historicalPane");
map.getPane("historicalPane").style.zIndex = 250;

const state = {
    summary: null,
    overlays: {
        cells: null,
        wards: null,
    },
    geoLayer: null,
    boundaryLayer: null,
    basemapLayer: null,
    historicalImagery: null,
    historicalLayer: null,
    historicalMode: "off",
    activeHistoricalYear: null,
    basemapMode: initialBasemapMode,
    boundsByProperty: {
        cells: {},
        wards: {},
    },
    periods: [],
};

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

function availableUnitOptions() {
    const options = [{ key: "cells", label: "Cells" }];
    if (state.overlays.wards?.features?.length) {
        options.unshift({ key: "wards", label: "Wards" });
    }
    return options;
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
    const mode = state.historicalMode;
    if (mode === "base") {
        return state.summary.config.base_year;
    }
    if (mode === "timeline") {
        return state.summary.config.base_year - Number.parseInt(currentPeriodKey(), 10);
    }
    return null;
}

function currentHistoricalTiles() {
    const year = currentHistoricalYear();
    if (year === null || !state.historicalImagery?.years) {
        return [];
    }
    return state.historicalImagery.years[String(year)]?.tiles ?? [];
}

function updateHistoricalImageryLabel() {
    const label = document.getElementById("historicalImageryLabel");
    if (!state.historicalImagery?.available) {
        label.textContent = "Historical previews appear here after regenerating this output folder with the updated pipeline.";
        return;
    }
    const year = currentHistoricalYear();
    if (year === null) {
        label.textContent = "Historical imagery is hidden.";
        return;
    }
    const tiles = currentHistoricalTiles();
    if (!tiles.length) {
        label.textContent = `No historical image preview was exported for ${year}.`;
        return;
    }
    const sceneCount = tiles.reduce(
        (total, tile) => total + Number(tile.scene_count ?? 0),
        0,
    );
    label.textContent = `Showing ${year} annual composite imagery across ${tiles.length} tiles (${sceneCount} source scenes).`;
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
    state.basemapMode = fileProtocol ? "none" : normalizedMode;

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
}

function applyHistoricalImagery(force = false) {
    const nextYear = currentHistoricalYear();
    if (
        !force &&
        state.activeHistoricalYear === nextYear &&
        !(
            nextYear === null &&
            state.historicalLayer
        )
    ) {
        updateHistoricalImageryLabel();
        return;
    }

    if (state.historicalLayer) {
        map.removeLayer(state.historicalLayer);
        state.historicalLayer = null;
    }

    state.activeHistoricalYear = nextYear;
    const tiles = currentHistoricalTiles();
    if (nextYear === null || !tiles.length) {
        updateHistoricalImageryLabel();
        return;
    }

    state.historicalLayer = L.layerGroup(
        tiles.map((tile) =>
            L.imageOverlay(`${tile.image}`, tile.bounds, {
                pane: "historicalPane",
                opacity: 0.95,
                interactive: false,
            }),
        ),
    ).addTo(map);
    updateHistoricalImageryLabel();
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
    document.getElementById("periodLabel").textContent = currentPeriodKey();
    state.geoLayer.setStyle((feature) => computeFeatureStyle(feature, false));
    applyHistoricalImagery();
    updateLegend();
    updateSummaryCards();
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
    basemapSelect.value = state.basemapMode;
}

function populateHistoricalImagerySelect() {
    const historicalSelect = document.getElementById("historicalImagery");
    const options = state.historicalImagery?.available
        ? [
              { key: "off", label: "Off" },
              { key: "timeline", label: "Timeline Match" },
              { key: "base", label: `Base Year (${state.summary.config.base_year})` },
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
    if (options.some((option) => option.key === previousValue)) {
        unitSelect.value = previousValue;
    } else if (options.length) {
        unitSelect.value = options[0].key;
    }
}

async function loadData() {
    const [summary, overlay, boundary, wardOverlay, historicalImagery] = await Promise.all([
        fetch("summary.json").then((response) => response.json()),
        fetch("overlay.geojson").then((response) => response.json()),
        fetch("boundary.geojson")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
        fetch("ward_overlay.geojson")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
        fetch("historical_imagery.json")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
    ]);

    state.summary = summary;
    state.overlays.cells = overlay;
    state.overlays.wards = wardOverlay?.features?.length ? wardOverlay : null;
    state.historicalImagery =
        historicalImagery ?? summary.historical_imagery ?? null;
    state.periods = summary.config.periods.map((value) => `${value}y`);
    state.boundsByProperty.cells = computeBoundsByProperty(overlay.features);
    state.boundsByProperty.wards = state.overlays.wards
        ? computeBoundsByProperty(state.overlays.wards.features)
        : {};

    populateBasemapSelect();
    populateHistoricalImagerySelect();
    populateUnitSelect();

    document.getElementById("title").textContent = summary.metadata.label;
    document.getElementById("subtitle").textContent = buildSubtitle(summary);

    const periodSlider = document.getElementById("period");
    periodSlider.max = Math.max(state.periods.length - 1, 0);

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

    updateLayer();
}

document.getElementById("basemap").addEventListener("change", (event) => {
    applyBasemap(event.target.value);
});

document
    .getElementById("historicalImagery")
    .addEventListener("change", (event) => {
        state.historicalMode = event.target.value;
        applyHistoricalImagery(true);
    });

document.getElementById("unit").addEventListener("change", () => {
    updateMetricSelect();
    renderOverlayLayer();
    updateLayer();
});

document.getElementById("metric").addEventListener("change", updateLayer);
document.getElementById("period").addEventListener("input", updateLayer);
document.getElementById("opacity").addEventListener("input", updateLayer);

loadData().catch((error) => {
    document.getElementById("title").textContent = "Could not load analysis";
    document.getElementById("subtitle").textContent =
        `${error.message}. Serve the output directory with a local web server, such as ` +
        "`python -m http.server`, instead of opening `ui/index.html` directly from disk.";
    console.error(error);
});
