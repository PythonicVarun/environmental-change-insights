// To add a new insight card, append a new object to this array.
// Each insight has:
//   - title:    headline of the insight
//   - location: badge label (city / district)
//   - content:  article body (plain text or simple HTML)
//   - then:     { date, image, url } — historical snapshot (left image)
//   - now:      { date, image, url } — current snapshot (right image)
//   - references: [{ label, url }] — supporting news / map links
// If `image` is empty (""), a placeholder is rendered in its place.

const INSIGHTS = [
    {
        title: "Deluge at the Border: Sahibi River Basin Swallows Farmlands Whole",
        location: "Gurgaon",
        content:
            "Recent geospatial data paints a grim picture at the Delhi-Haryana border, where massive inundation has completely rewritten the landscape along the Sahibi river since 2020. Satellite tracking reveals a staggering vegetation loss of -1.067, directly corresponding to an alarming surface water spike of 0.897 that has submerged vast tracts of former agricultural land. What were once distinctly patterned green fields just five years ago are now entirely drowned beneath a murky, stagnant expanse. This severe ecological disruption raises immediate concerns about blocked drainage channels, altered natural basins, and the escalating flood risks threatening the border's fragile ecosystem.",
        then: {
            date: "31st Aug, 2022",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=5y&opacity=0.30&historical=timeline&speed=1200&lat=28.50535&lng=76.93170&zoom=16.00&historicalDate=2022-08-31",
        },
        now: {
            date: "21st Sep, 2022",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=5y&opacity=0.30&historical=timeline&speed=1200&lat=28.50535&lng=76.93170&zoom=16.00&historicalDate=2022-09-21",
        },
        references: [
            { label: "India Today", url: "https://www.indiatoday.in/india/story/gurugram-gurgaon-urban-floods-waterlogging-sahibi-river-lost-aravalli-hills-delhi-haryana-hydrology-natural-drainage-2781896-2025-09-08" },
            { label: "Indian Express", url: "https://indianexpress.com/article/explained/why-even-moderate-rainfall-leads-to-flooding-in-gurgaon-10131520/" },
        ],
    },
    {
        title: "Vanishing Oasis: Yamuna's Retreat Leaves Okhla Bird Sanctuary Parched",
        location: "Gautam Buddha Nagar",
        content:
            "Once renowned as a vital haven for over 300 bird species, the Okhla Bird Sanctuary in Gautam Buddha Nagar is facing a severe ecological crisis as its primary water source rapidly vanishes. Geospatial analysis comparing 2015 to 2025 reveals a drastic water retreat across the Yamuna riverbed, marked by a massive negative water delta of -1.137. Where a sprawling lake once stood, satellite imagery now exposes a combination of encroaching urban development (0.688) and expanding tracts of bare, exposed soil (0.645). This devastating ten-year drying trend not only tarnishes the sanctuary's former charm but threatens to permanently destroy a crucial ecological wetland beneath the pressure of regional concrete sprawl.",
        then: {
            date: "10th Aug, 2017",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=28.55676&lng=77.32302&zoom=15.00&historicalDate=2017-08-10",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.55&historical=timeline&speed=1200&lat=28.55676&lng=77.32302&zoom=15.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "New Indian Express", url: "https://www.newindianexpress.com/states/delhi/2025/Jun/06/from-feathered-paradise-to-forgotten-dump-okhla-sanctuary-loses-charm" },
        ],
    },
    {
        title: "Engineered Ecology: Noida's 'Wetland Park' Masks Severe Natural Water Retreat",
        location: "Gautam Buddha Nagar",
        content:
            "Recent geospatial tracking of Noida's City Park between 2015 and 2025 reveals a startling ecological paradox behind the much-touted 'Waste to Wealth' wetland project. While aerial imagery showcases new manicured ponds and intricate walking paths, the site has actually suffered a massive natural surface water deficit of -1.076. This severe water retreat is directly tied to the heavy recreational landscaping of the area, which is marked by an urban expansion surge of 0.717. Ultimately, beneath the eco-friendly branding, aggressive concretization has paved over a raw natural basin to create a purely cosmetic, highly engineered recreational space.",
        then: {
            date: "21st Jan, 2015",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.35&historical=base&speed=1200&lat=28.59664&lng=77.35729&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.35&historical=base&speed=1200&lat=28.59664&lng=77.35729&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/7TQGL4jjF5YmvLJMA" },
        ],
    },
    {
        title: "Flight Path Over Farmland: Jewar's Mega-Airport Eradicates Agrarian Green Belt",
        location: "Gautam Buddha Nagar",
        content:
            "Recent satellite imagery from 2015 to 2025 captures the sheer, unapologetic scale of infrastructural terraforming at the Noida International Airport site in Jewar. What was once an uninterrupted stretch of fertile agricultural land has been completely paved over, marked by a significant vegetation drop of -0.426. In its place, massive runways and terminal complexes have aggressively taken root, driving a sharp urban expansion spike of 0.287 and leaving vast tracts of bare, stripped soil (0.277) in their wake. While this aviation mega-project promises unprecedented economic capacity for the region, the geospatial data bluntly illustrates the absolute, irreversible sacrifice of the local agrarian ecosystem required to get it off the ground.",
        then: {
            date: "21st Jan, 2015",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.25&historical=base&speed=1200&lat=28.17355&lng=77.61789&zoom=15.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.55&historical=base&speed=1200&lat=28.17355&lng=77.61789&zoom=15.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/HgYnoMjBkXQBpgHj9" },
        ],
    },
    {
        title: "Solar Sprawl: Maruti's ‘Green' Expansion Paves Over Manesar's Natural Cover",
        location: "Gurgaon",
        content:
            "Recent satellite tracking of IMT Manesar between 2020 and 2025 exposes the harsh ecological trade-off behind Maruti Suzuki's sprawling plant and sports complex development. While the installation of massive new solar arrays paints a picture of sustainable modernization, geospatial data reveals this infrastructure boom has driven a severe vegetation plunge of -0.943. The aggressive land grab for these panel installations and expanded facilities has triggered an urban surge of 0.474, systematically stripping away the area's remaining scrublands. Ultimately, the corporate push for renewable power and expanded operational capacity has been achieved by permanently suffocating the local, living ecosystem beneath glass, metal, and concrete.",
        then: {
            date: "8th Jan, 2020",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.45&historical=timeline&speed=1200&lat=28.36679&lng=76.89807&zoom=16.00&historicalDate=2020-01-08",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.45&historical=timeline&speed=1200&lat=28.36679&lng=76.89807&zoom=16.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/5bKL5rnrKbe5iajK8" },
            { label: "Wikipedia", url: "https://en.wikipedia.org/wiki/Maruti_Suzuki#:~:text=In%20November%202021%20Maruti%20Suzuki%20announce%20to%20set%20up%20of%20a%20big%20plant%20in%20IMT%20Kharkhoda%20in%20Sonipat%20district%20across%20900%20acres%20with%20an%20investment%20of%20%E2%82%B918%2C000%20crores.%5B42%5D" },
        ],
    },
    {
        title: "Supply Chain Chokehold: Mega-Warehouse Bulldozes Pataudi's Farmland",
        location: "Gurgaon",
        content:
            "Recent geospatial imagery from 2020 to 2025 lays bare the ruthless bulldozing of Gurugram's rural outskirts, where massive logistics warehouses have entirely swallowed once-lush agricultural plots in Ransika. Satellite tracking of this brutal industrial sprawl reveals a catastrophic vegetation plunge of -0.979 as gigantic commercial sheds aggressively pave over former orchards and crop fields. This unyielding terraforming has triggered a stark urban expansion spike of 0.415, replacing fertile, living soil with dead concrete and exposed dirt. Ultimately, this unapologetic land grab prioritizes corporate supply chains over environmental survival, permanently suffocating the local ecosystem to feed a relentless appetite for storage infrastructure.",
        then: {
            date: "8th Jan, 2020",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.30&historical=base&speed=1200&lat=28.31117&lng=76.74496&zoom=18.00&historicalDate=2020-01-08",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.55&historical=base&speed=1200&lat=28.31117&lng=76.74496&zoom=18.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/xyGfAwA3weNfWXVx6" },
        ],
    },
    {
        title: "Commercial Sprawl: Gurugram's Retail Expansion Scrapes Away Green Cover",
        location: "Gurgaon",
        content:
            "Recent satellite imagery between 2020 and 2025 highlights the stark environmental cost of commercial development along Gurugram's Sheetla Mata Road. Geospatial data confirms that the construction of new commercial compounds — including the site housing the Orientbell Tiles Boutique — has driven a severe vegetation loss of -0.845 across the immediate area. This retail and warehousing expansion has directly replaced dense, natural scrubland with concrete infrastructure, triggering an urban spike of 0.145 and stripping the topsoil bare (0.203). While serving as a hub for local home development, the satellite feed exposes how rapidly these commercial clusters are erasing Gurugram's peripheral green patches.",
        then: {
            date: "8th Jan, 2020",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.30&historical=timeline&speed=1200&lat=28.48704&lng=77.04336&zoom=17.00&historicalDate=2020-01-08",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.50&historical=timeline&speed=1200&lat=28.48704&lng=77.04336&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/6JQzv74cSkdZjhoy6" },
        ],
    },
    {
        title: "Concrete Harvest: 'Olive Greens' Construction Bulldozes Gurugram's Natural Belt",
        location: "Gurgaon",
        content:
            "Recent satellite tracking from 2020 to 2025 exposes the ironic ecological reality behind Gurugram's new 'Olive Greens' housing complex. What was once a sprawling, uninterrupted tract of raw agricultural land has been completely stripped, resulting in a severe vegetation loss of -0.601. The aggressive site clearance and ongoing construction have paved over the earth, driving an urban expansion surge of 0.148 and artificially trapping surface water (0.523) in disrupted drainage paths. Beneath the promise of a luxury residential hub, the geospatial data clearly illustrates the total, systematic eradication of the sector's authentic green cover to make way for high-end concrete.",
        then: {
            date: "8th Jan, 2020",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.15&historical=timeline&speed=1200&lat=28.47140&lng=76.99055&zoom=18.00&historicalDate=2020-01-08",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gurgaon/?basemap=osm&metric=vegetation_delta&unit=cells&period=5y&opacity=0.60&historical=timeline&speed=1200&lat=28.47140&lng=76.99055&zoom=18.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Google Maps", url: "https://maps.app.goo.gl/8g8UtovAPHeUdySk9" },
        ],
    },
    {
        title: "Toxic Terrain: Noida's Green Lung Butchered for a Massive Trash Mountain",
        location: "Gautam Buddha Nagar",
        content:
            "Geospatial tracking from 2015 to 2025 exposes a brutal environmental degradation in Mubarikpur, where a dense natural forest has been unapologetically transformed into a colossal civic dumping ground. Satellite data records a devastating vegetation wipeout of -0.640, as the lush canopy was systematically bulldozed and replaced by a rapidly expanding wasteland (Urban: 0.533). This gross mismanagement has deeply scarred the landscape, driving a massive spike in bare, contaminated soil (0.532) and triggering a sharp rise in the local pollution proxy (0.212). Instead of preserving vital green buffers, authorities have essentially choked a living ecosystem to death under thousands of tons of mounting garbage.",
        then: {
            date: "21st Jan, 2015",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=28.48053&lng=77.45639&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.55&historical=timeline&speed=1200&lat=28.48053&lng=77.45639&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Times of India", url: "https://timesofindia.indiatimes.com/city/ghaziabad/noida-begins-temporary-dumping-of-waste-in-mubarakpur-village/articleshow/64770413.cms" },
        ],
    },
    {
        title: "Concrete Over Conservation: Urban Sprawl Chokes Dhanauri Wetlands Dry",
        location: "Gautam Buddha Nagar",
        content:
            "In a devastating ecological blow, recent satellite tracking from 2015 to 2025 reveals that the once-teeming Dhanauri Wetlands are rapidly collapsing under the weight of unregulated regional development. Geospatial data exposes a massive water retreat (-1.119 delta), reducing the sprawling bird sanctuary to fragmented, muddy puddles. This severe drying is directly fueled by aggressive, unchecked urban encroachment in the vicinity, which has recorded a sharp expansion surge of 0.592. As concrete relentlessly paves over natural drainage basins and exposed soil expands (0.389), this crucial natural heritage site is being systematically eradicated.",
        then: {
            date: "21st Jan, 2015",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=base&speed=1200&lat=28.33872&lng=77.62137&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/gautam_buddha_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.45&historical=base&speed=1200&lat=28.33872&lng=77.62137&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "The Print", url: "https://theprint.in/ground-reports/jewar-airport-has-fuelled-a-fight-for-dhanauri-wetlands-up-now-has-to-balance-3-priorities/1816393/" },
            { label: "Times of India", url: "https://timesofindia.indiatimes.com/city/noida/sarus-habitat-dhanauri-a-step-closer-to-being-notified-a-wetland/articleshow/92049188.cms" },
        ],
    },
    // ---- New insights ----
    {
        title: "Bellandur's Missing Water: Not a Drought, But a Deep Clean",
        location: "Bengaluru",
        content:
            "Satellite imagery spanning a decade reveals a chilling transformation at Bellandur Lake — once Bengaluru's largest and most pristine water body, it is now almost entirely smothered under a thick mat of water hyacinth and toxic sludge, with its open water surface shrinking dramatically between 2015 and 2025. Every day, nearly 260 million litres of untreated sewage — roughly 40–50% of the city's total output — pours into the lake, creating nutrient-rich conditions in which invasive water hyacinth thrives and native aquatic life collapses. Despite a government promise that the lake's rejuvenation would be completed by December 2024, the Bangalore Development Authority was forced to halt desilting work in May 2024 due to a complete exhaustion of funds, leaving the project unfinished and the NGT's court orders in limbo. A damning CPCB report filed with the NGT in April 2025 confirmed that over 90% of water bodies in Karnataka — most of them in Bengaluru — are in a state of neglect and fail to meet the pollution board's prescribed water quality standards. On the ground today, residents describe a foul-smelling, fenced-off lake bed where grass and hyacinth have replaced open water — a far cry from the thriving ecosystem visible in satellite images just ten years ago, and a stark warning of what unchecked urban growth can do to a city's natural lifeline.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/bengaluru/bellandur_lake/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=12.93479&lng=77.66817&zoom=16.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/bengaluru/bellandur_lake/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=12.93479&lng=77.66817&zoom=16.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Reddit Discussion", url: "https://www.reddit.com/r/bangalore/comments/1qpyqmg/whats_happening_to_bellandur_lake/" },
            { label: "YouTube Report", url: "https://youtu.be/Y4bW1QerzOE" },
            { label: "Asian Scientist", url: "https://www.asianscientist.com/2023/09/in-the-lab/why-does-this-indian-lake-foam-so-much/" },
            { label: "Earth5R", url: "https://earth5r.org/bellandur-lake-urban-restoration/" },
        ],
    },
    {
        title: "Multi-Crore Revival Fills TG Halli, But Bengaluru Gets Not a Drop",
        location: "Bengaluru",
        content:
            "Satellite imagery reveals a dramatic transformation at the Thippagondanahalli (TG Halli) Reservoir on the Arkavathi River, which is currently brimming with surface water after years of resembling a dry, barren basin. This visible surge is the direct result of recent favorable monsoons and an over Rs 230 crore rejuvenation project by the BWSSB that installed brand-new purification and pumping infrastructure. Yet, in a harsh twist of bureaucratic irony, actual water supply from TG Halli to the city has been completely halted. With the highly anticipated Yettinahole project link delayed and authorities now pivoting entirely to the Cauvery 5th Stage for city supply, this newly filled reservoir currently stands as a massive white elephant, holding water that remains utterly elusive to the taxpayers who funded its revival.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/bengaluru/arkavathi_river/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=12.97681&lng=77.34495&zoom=15.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/bengaluru/arkavathi_river/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=12.97681&lng=77.34495&zoom=15.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "New Indian Express", url: "https://www.newindianexpress.com/cities/bengaluru/2026/Apr/22/bengaluru-gets-cauvery-water-rs-230-crore-spent-on-thippagondanahalli-to-go-waste" },
            { label: "Bangalore Mirror", url: "https://bangaloremirror.indiatimes.com/bangalore/others/thippagondanahalli-halli-fully-revived-but-water-remains-elusive/articleshow/120415761.cms" },
        ],
    },
    {
        title: "City's Treated Sewage Is Now Nelamangala's Lifeline",
        location: "Bengaluru",
        content:
            "Satellite data confirms a massive shift at Nelamangala's Sondekoppa Lake, which has visibly transformed from a barren depression into a brimming waterbody. This sudden surge isn't a natural monsoon miracle, but the direct result of the state government's recently commissioned Vrishabhavathi Lift Irrigation Project. Authorities are now aggressively pumping over 240 MLD of secondary-treated wastewater from Bengaluru city to forcibly fill 70 parched lakes across the drought-prone Nelamangala and Tumakuru belts. While experts remain highly skeptical about the long-term ecological impact and potential heavy metal contamination from the treated sewage, the undeniable reality on the ground is that the region's decades-dry aquifers are finally beginning to recharge.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/bengaluru/sondekoppa_lake/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=13.01772&lng=77.38748&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/bengaluru/sondekoppa_lake/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/bengaluru/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=base&speed=1200&lat=13.01772&lng=77.38748&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "Bangalore Mirror", url: "https://bangaloremirror.indiatimes.com/bangalore/civic/flowing-life-revitalising-bengalurus-thirsty-lakes/articleshow/103894854.cms" },
            { label: "New Indian Express", url: "https://www.newindianexpress.com/cities/bengaluru/2025/May/09/treated-water-to-flow-into-nelamangala-lakes" },
        ],
    },
    {
        title: "Adyar's Missing Water Is Actually Chennai's Biggest Green Victory",
        location: "Chennai",
        content:
            "Satellite data indicating a massive \"retreat\" of surface water at the Adyar Estuary isn't a sign of drying aquifers, but the spectacular result of a decade-long ecological overhaul. Driven by the Chennai Rivers Restoration Trust (CRRT), millions of tonnes of toxic sludge were dredged from the riverbed and engineered into elevated habitat islands. These newly formed mudflats were then aggressively seeded with over 57,000 mangrove saplings and native coastal vegetation. What algorithms flag as a severe \"water deficit\" is actually a deliberate, multi-crore engineering feat that has successfully converted a once-dead, sewage-choked waterway into a thriving, flood-buffering mangrove forest.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/chennai/adyar_river/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=timeline&speed=1200&lat=13.01385&lng=80.26315&zoom=16.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/chennai/adyar_river/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.15&historical=base&speed=1200&lat=13.01385&lng=80.26315&zoom=16.00&historicalDate=2025-12-18",
        },
        references: [],
    },
    {
        title: "Concrete Replaces Canopy in Anna Nagar's Rs 560-Crore Govt Revamp",
        location: "Chennai",
        content:
            "Satellite imagery reveals a stark ecological trade-off at Anna Nagar's Customs and GST Quarters, where dense, decades-old urban canopy has been wiped out to make way for the new \"Nandavanam\" Central Revenue complex. To construct 10 modern high-rise towers for tax officials — a massive Rs 560 crore redevelopment project — authorities entirely bulldozed the lush green cover that once defined the old low-density residential campus. What spatial algorithms flag as a severe plunge in the vegetation index is the undeniable, harsh reality of Chennai's vertical expansion. Modern infrastructure and centralized housing for bureaucrats has arrived, but it has come at the direct and permanent expense of one of the neighborhood's most vital green lungs.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/chennai/anicham/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=timeline&speed=1200&lat=13.08889&lng=80.20582&zoom=18.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/chennai/anicham/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=timeline&speed=1200&lat=13.08889&lng=80.20582&zoom=18.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "PIB India", url: "https://www.pib.gov.in/PressReleasePage.aspx?PRID=1884616" },
            { label: "Chennai Customs", url: "https://chennaicustoms.gov.in/wp-content/uploads/2023/05/Circular-regarding-allotment-of-quarters-at-C-R-Quarters-Anna-Nagar.pdf" },
        ],
    },
    {
        title: "Mountain of Trash Turns into Urban Forest at Kodungaiyur",
        location: "Chennai",
        content:
            "Satellite data revealing a dramatic, sprawling green canopy at the railway land near Captain Cotton Canal isn't just random weed growth — it's the exact site of Chennai's notorious Kodungaiyur dump yard undergoing a massive Rs 648-crore biomining and reclamation project. For decades, this 250-acre tract was a toxic, smoldering mountain of legacy waste that severely plagued North Chennai's air and groundwater. Driven by the Greater Chennai Corporation, heavy machinery has finally cleared millions of tonnes of garbage to actively pave the way for a lush, engineered urban forest and eco-park. What spatial algorithms flag as a sudden surge in the vegetation index is actually a historic environmental victory, transforming a hazardous wasteland into a vital green lung for the city.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/chennai/railway_land/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=timeline&speed=1200&lat=13.12504&lng=80.26990&zoom=18.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/chennai/railway_land/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/chennai/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=timeline&speed=1200&lat=13.12504&lng=80.26990&zoom=18.00&historicalDate=2025-12-18",
        },
        references: [
            { label: "DT Next", url: "https://www.dtnext.in/news/chennai/chennai-corporation-raises-rs-20559-crore-through-green-municipal-bond-for-kodungaiyur-bio-mining" },
        ],
    },
    {
        title: "Massive Artificial Lake Appears in Kanpur as Rs 21,780-Crore Power Project Goes Live",
        location: "Kanpur Nagar",
        content:
            "Satellite imagery over Aswarmau reveals a massive new waterbody where dry farmland stood just a decade ago. This sudden inundation is not a flood, but the deliberate filling of the sprawling 46-lakh cubic meter raw water reservoir for the newly commissioned Ghatampur Thermal Power Project (NUPPL). Fed by a 44-kilometer pipeline drawing from the West Allahabad Branch Canal, this colossal twin-basin acts as the essential cooling lifeline for the Rs 21,780-crore, 1,980 MW supercritical thermal plant. As Unit-1 finally goes live to meet Uttar Pradesh's soaring electricity demands, spatial algorithms are flagging the site as a massive \"water gain\" — a stark, undeniable visualization of how national mega-infrastructure permanently redraws the local ecological map.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/kanpur_nagar/nuppl_lake/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=25.97461&lng=80.17256&zoom=16.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/kanpur_nagar/nuppl_lake/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=25.97461&lng=80.17256&zoom=16.00&historicalDate=2025-12-18",
        },
        references: [],
    },
    {
        title: "Panki's Dormant Wasteland Floods to Power Kanpur's New 660-MW Mega Plant",
        location: "Kanpur Nagar",
        content:
            "Satellite data revealing a massive new waterbody over the industrial belts of Panki isn't a flood or a sudden wetland regeneration — it's the deliberate reactivation of the site's legacy fly ash pond. Following the retirement of the older units in 2018, this massive basin sat largely dry and barren. However, with the newly commissioned 660 MW supercritical Panki Thermal Power Station finally going live, millions of liters of water are once again being pumped in to drive the massive ash handling and recovery systems. What spatial algorithms flag as a drastic \"water gain\" is simply the undeniable, ground-level footprint of Uttar Pradesh firing up one of its oldest power hubs for a modern era.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/kanpur_nagar/panki_fly_ash/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=26.47114&lng=80.23154&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/kanpur_nagar/panki_fly_ash/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=water_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=26.47114&lng=80.23154&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [],
    },
    {
        title: "Concrete Replaces Crops for Kanpur's Rs 21,780-Crore Mega Power Plant",
        location: "Kanpur Nagar",
        content:
            "Satellite imagery reveals a stark ecological trade-off at Aswarmau in Kanpur Nagar, where nearly 1,000 hectares of lush agricultural land has been entirely wiped out by heavy industry. What spatial algorithms flag as a severe plunge in the vegetation index is the sprawling, concrete footprint of the newly commissioned Ghatampur Thermal Power Project (NUPPL). To secure Uttar Pradesh's energy future with this massive 1,980 MW supercritical coal plant, authorities had to permanently bulldoze the region's established farming canopy to make way for massive boiler units and cooling towers. This dramatic green deficit serves as the undeniable, ground-level cost of rapid national infrastructure expansion.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/kanpur_nagar/nuppl_power_plant/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=25.97254&lng=80.19043&zoom=16.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/kanpur_nagar/nuppl_power_plant/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=25.97254&lng=80.19043&zoom=16.00&historicalDate=2025-12-18",
        },
        references: [],
    },
    {
        title: "Concrete Replaces Canopy for Panki's New 660-MW Mega Power Plant",
        location: "Kanpur Nagar",
        content:
            "The dramatic disappearance of open green space at the Panki Power House site is the sprawling concrete footprint of Uttar Pradesh's new 660 MW supercritical thermal plant. Built directly over the vacant premises of the defunct 1970s-era station, this multi-crore UPRVUNL mega-project has permanently replaced acres of campus vegetation with a colossal cooling tower, massive boiler units, and heavy switchyards. What algorithms register as a drastic ecological deficit is the undeniable, ground-level trade-off required to secure Kanpur's modernized energy future.",
        then: {
            date: "21st Jan, 2015",
            image: "./outputs/kanpur_nagar/panki_power_point/2015.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=26.47406&lng=80.23985&zoom=17.00&historicalDate=2015-01-21",
        },
        now: {
            date: "18th Dec, 2025",
            image: "./outputs/kanpur_nagar/panki_power_point/2025.png",
            url: "https://pythonicvarun.github.io/olmoearth-change-insights/outputs/kanpur_nagar/?basemap=esri_imagery&metric=vegetation_delta&unit=cells&period=10y&opacity=0.00&historical=base&speed=1200&lat=26.47406&lng=80.23985&zoom=17.00&historicalDate=2025-12-18",
        },
        references: [],
    },
];

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
    })[c]);
}

function renderSnapshot(snapshot, side) {
    const hasImage = snapshot.image && snapshot.image.length > 0;
    const imageHtml = hasImage
        ? `<img src="${escapeHtml(snapshot.image)}" alt="${escapeHtml(side)} — ${escapeHtml(snapshot.date)}" loading="lazy" />`
        : `<div class="snapshot-placeholder"><span>Image coming soon</span></div>`;
    return `
        <a class="snapshot" href="${escapeHtml(snapshot.url)}" target="_blank" rel="noopener" title="Open ${escapeHtml(snapshot.date)}">
            <div class="snapshot-frame">
                ${imageHtml}
                <span class="snapshot-side">${escapeHtml(side)}</span>
            </div>
            <span class="snapshot-date">${escapeHtml(snapshot.date)}</span>
        </a>
    `;
}

function renderInsight(insight, index) {
    const refs = (insight.references || [])
        .map(
            (r) =>
                `<a class="action-link secondary" href="${escapeHtml(r.url)}" target="_blank" rel="noopener">${escapeHtml(r.label)}</a>`,
        )
        .join("");
    const refsRow = refs ? `<div class="action-row">${refs}</div>` : "";
    return `
        <article class="analysis-card">
            <div class="snapshot-pair">
                ${renderSnapshot(insight.then, "Then")}
                ${renderSnapshot(insight.now, "Now")}
            </div>
            <div class="card-top">
                <div>
                    <div class="card-eyebrow">Insight ${index + 1}</div>
                    <h2 class="analysis-title">${escapeHtml(insight.title)}</h2>
                </div>
                <div class="card-badge">${escapeHtml(insight.location)}</div>
            </div>
            <div class="insight-content">${escapeHtml(insight.content)}</div>
            ${refsRow}
        </article>
    `;
}

document.addEventListener("DOMContentLoaded", () => {
    const list = document.getElementById("insights-list");
    if (!list) return;
    list.innerHTML = INSIGHTS.map(renderInsight).join("");
});
