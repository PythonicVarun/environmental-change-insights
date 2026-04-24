const metricOptions = [
    { key: "embedding_change", label: "OlmoEarth Change" },
    { key: "vegetation_delta", label: "Vegetation Delta" },
    { key: "water_delta", label: "Water Delta" },
    { key: "urban_delta", label: "Urbanization Delta" },
    { key: "bare_soil_delta", label: "Bare Soil Delta" },
];

const metricColorStops = {
    embedding_change: ["#f7f2eb", "#f1a661", "#8d2f20"],
    vegetation_delta: ["#8e4f2a", "#f5edd6", "#23643d"],
    water_delta: ["#7b4a26", "#f3efe7", "#1f6e8c"],
    urban_delta: ["#2f5d50", "#f1efe8", "#c65d19"],
    bare_soil_delta: ["#2f6b84", "#f7f1dd", "#8b5e34"],
};

const searchParams = new URLSearchParams(window.location.search);
const basemapMode =
    searchParams.get("basemap") ??
    (window.location.protocol === "file:" ? "none" : "osm");

const map = L.map("map", { zoomControl: true, preferCanvas: true });
if (basemapMode === "osm") {
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "&copy; OpenStreetMap contributors",
        maxZoom: 19,
    }).addTo(map);
} else {
    map.getContainer().classList.add("no-basemap");
    map.attributionControl.addAttribution("Basemap disabled");
}

const state = {
    summary: null,
    overlay: null,
    geoLayer: null,
    boundaryLayer: null,
    boundsByProperty: {},
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

function currentPeriodKey() {
    const slider = document.getElementById("period");
    return state.periods[Number(slider.value)];
}

function currentMetricKey() {
    return document.getElementById("metric").value;
}

function propertyKey() {
    return `${currentMetricKey()}_${currentPeriodKey()}`;
}

function styleForFeature(feature) {
    const key = propertyKey();
    const metric = currentMetricKey();
    const value = feature.properties[key];
    if (value === null || value === undefined || Number.isNaN(value)) {
        return {
            color: "transparent",
            fillColor: "transparent",
            fillOpacity: 0,
            weight: 0,
        };
    }

    const opacity = Number(document.getElementById("opacity").value);
    const bounds = state.boundsByProperty[key];
    const min = bounds?.min ?? 0;
    const max = bounds?.max ?? 1;
    const midpoint = metric === "embedding_change" ? min : 0;
    let t;
    if (metric === "embedding_change") {
        t = (value - min) / Math.max(max - min, 1e-9);
    } else {
        const span = Math.max(Math.abs(min), Math.abs(max), 1e-9);
        t = (value + span) / (2 * span);
    }

    return {
        color: "rgba(23,34,45,0)",
        weight: 0,
        fillColor: interpolateColor(metricColorStops[metric], t),
        fillOpacity: opacity,
    };
}

function updateLegend() {
    const key = propertyKey();
    const metric = currentMetricKey();
    const bounds = state.boundsByProperty[key] ?? { min: 0, max: 0 };
    const legend = document.getElementById("legendScale");
    legend.style.background = `linear-gradient(90deg, ${metricColorStops[metric][0]} 0%, ${metricColorStops[metric][1]} 50%, ${metricColorStops[metric][2]} 100%)`;
    document.getElementById("legendMin").textContent = bounds.min.toFixed(3);
    document.getElementById("legendMax").textContent = bounds.max.toFixed(3);
}

function updateSummaryCards() {
    const periodKey = currentPeriodKey();
    const periodSummary = state.summary.periods[periodKey];
    const grid = document.getElementById("summaryGrid");
    const metrics = periodSummary?.metrics ?? {};
    const entries = [
        ["Median Change", metrics.embedding_change_median ?? 0],
        ["P95 Change", metrics.embedding_change_p95 ?? 0],
        ["Mean Vegetation", metrics.vegetation_delta_mean ?? 0],
        ["Mean Urban", metrics.urban_delta_mean ?? 0],
    ];
    grid.innerHTML = entries
        .map(
            ([label, value]) => `
        <div class="stat">
          <div class="stat-label">${label}</div>
          <div class="stat-value">${Number(value).toFixed(3)}</div>
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
                    Change ${Number(spot.embedding_change).toFixed(3)} |
                    Veg ${Number(spot.vegetation_delta).toFixed(3)} |
                    Water ${Number(spot.water_delta).toFixed(3)} |
                    Urban ${Number(spot.urban_delta).toFixed(3)}
                </div>
                </div>
            `,
        )
        .join("");
}

function updateLayer() {
    if (!state.geoLayer) {
        return;
    }
    document.getElementById("periodLabel").textContent = currentPeriodKey();
    state.geoLayer.setStyle(styleForFeature);
    updateLegend();
    updateSummaryCards();
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

function buildTooltip(properties) {
    const period = currentPeriodKey();
    return `
    <div class="map-tooltip">
      <strong>${properties[`story_${period}`] ?? "Change cell"}</strong><br />
      OlmoEarth: ${Number(properties[`embedding_change_${period}`] ?? 0).toFixed(3)}<br />
      Vegetation: ${Number(properties[`vegetation_delta_${period}`] ?? 0).toFixed(3)}<br />
      Water: ${Number(properties[`water_delta_${period}`] ?? 0).toFixed(3)}<br />
      Urban: ${Number(properties[`urban_delta_${period}`] ?? 0).toFixed(3)}<br />
      Bare Soil: ${Number(properties[`bare_soil_delta_${period}`] ?? 0).toFixed(3)}
    </div>
  `;
}

function buildSubtitle(summary) {
    const coveragePercent = summary.metadata.coverage_percent;
    const coverageLabel =
        coveragePercent !== undefined
            ? ` Coverage ${Number(coveragePercent).toFixed(1)}% of boundary.`
            : "";
    const baseText =
        `Base year ${summary.config.base_year}. ${summary.feature_count} overlay cells. ` +
        `Display cell size ${summary.config.display_cell_size_m} m.` +
        coverageLabel;

    if (basemapMode === "osm") {
        return baseText;
    }

    return (
        `${baseText} Basemap is off because this page is running from ` +
        `\`file://\` or was opened with \`?basemap=none\`. ` +
        `Serve the folder over http(s), like \`python -m http.server\`, to use OpenStreetMap tiles with a valid Referer.`
    );
}

function handleFeatureMouseOver(event) {
    event.target.setStyle({
        color: "rgba(23,34,45,0.68)",
        weight: 0.9,
    });
    if (event.target.bringToFront) {
        event.target.bringToFront();
    }
}

function handleFeatureMouseOut(event) {
    if (state.geoLayer) {
        state.geoLayer.resetStyle(event.target);
    }
}

async function loadData() {
    const [summary, overlay, boundary] = await Promise.all([
        fetch("summary.json").then((response) => response.json()),
        fetch("overlay.geojson").then((response) => response.json()),
        fetch("boundary.geojson")
            .then((response) => (response.ok ? response.json() : null))
            .catch(() => null),
    ]);

    state.summary = summary;
    state.overlay = overlay;
    state.periods = summary.config.periods.map((value) => `${value}y`);
    state.boundsByProperty = computeBoundsByProperty(overlay.features);

    document.getElementById("title").textContent = summary.metadata.label;
    document.getElementById("subtitle").textContent = buildSubtitle(summary);

    const metricSelect = document.getElementById("metric");
    metricSelect.innerHTML = metricOptions
        .map(
            (metric) =>
                `<option value="${metric.key}">${metric.label}</option>`,
        )
        .join("");

    const periodSlider = document.getElementById("period");
    periodSlider.max = Math.max(state.periods.length - 1, 0);

    state.geoLayer = L.geoJSON(overlay, {
        style: styleForFeature,
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

document.getElementById("metric").addEventListener("change", updateLayer);
document.getElementById("period").addEventListener("input", updateLayer);
document.getElementById("opacity").addEventListener("input", updateLayer);

loadData().catch((error) => {
    document.getElementById("title").textContent = "Could not load analysis";
    console.error(error);
});
