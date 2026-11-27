import os
import json
import asyncio
import datetime
import requests
import websockets
from supabase import create_client

AISSTREAM_KEY = os.environ.get("AISSTREAM_API_KEY", "").strip()
FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

SLASH = chr(47)
DBL = SLASH * 2

if SUPABASE_URL and not SUPABASE_URL.startswith("http"):
    SUPABASE_URL = f"https:{DBL}{SUPABASE_URL}"

PORTS = {
    "PORT_NOLA": {
        "name": "Port of New Orleans",
        "region": "Gulf Coast",
        "lat": 29.93,
        "lon": -90.06,
        # Expanded from Kenner/St. Rose bend down through Chalmette & Belle Chasse anchorages
        "box": [[29.80, -90.35], [30.02, -89.88]]
    },
    "PORT_BTR": {
        "name": "Port of Greater Baton Rouge",
        "region": "Lower Mississippi",
        "lat": 30.43,
        "lon": -91.20,
        # Covers Plaquemine, Port Allen locks, Baton Rouge grain/petrochemical docks & Wilkinson Bridge
        "box": [[30.28, -91.26], [30.52, -91.15]]
    },
    "PORT_SOUTH_LA": {
        "name": "Port of South Louisiana (Grain & Petrochemical Corridor)",
        "region": "Lower Mississippi",
        "lat": 30.05,
        "lon": -90.58,
        # Covers Burnside, Geismar, Gramercy, Garyville, Reserve & LaPlace export elevators/refineries
        "box": [[30.00, -91.05], [30.26, -90.42]]
    },
    "PORT_MIA": {
        "name": "Port of Miami",
        "region": "Southeast",
        "lat": 25.77,
        "lon": -80.17,
        "box": [[25.72, -80.22], [25.81, -80.11]]
    }
}

AIRPORTS = {
    "MSY": {
        "name": "Louis Armstrong New Orleans Intl",
        "region": "Gulf Coast",
        "icao": "KMSY",
        "lat": 29.99,
        "lon": -90.25
    },
    "SDF": {
        "name": "Louisville Muhammad Ali Intl (UPS Worldport)",
        "region": "Ohio Valley",
        "icao": "KSDF",
        "lat": 38.17,
        "lon": -85.73
    }
}

# Louisiana & U.S. Macroeconomic / Energy / Agricultural Commodity Series via FRED
COMMODITY_SERIES = {
    "us_retail_diesel_usd_gal": ("GASDESW", "U.S. No. 2 Retail Diesel ($/gal)"),
    "gulf_coast_ulsd_usd_gal": ("DDFUELUSGULF", "Gulf Coast Ultra-Low-Sulfur Diesel ($/gal)"),
    "henry_hub_natgas_usd_mmbtu": ("DHHNGSP", "Henry Hub Natural Gas ($/MMBtu)"),
    "wti_crude_oil_usd_bbl": ("DCOILWTICO", "WTI Crude Oil ($/bbl)"),
    "freight_tsi_index": ("TSIFRGHT", "BTS Freight Transportation Services Index"),
    "global_corn_usd_mt": ("PMAIZMTUSDM", "Global Corn Benchmark ($/MT)"),
    "global_soybeans_usd_mt": ("PSOYBUSDM", "Global Soybeans Benchmark ($/MT)"),
    "global_rice_usd_mt": ("PRICENPQUSDM", "Global Rice Benchmark ($/MT)"),
    "global_sugar_cents_lb": ("PSUGAISAUSDM", "Global Sugar Benchmark (cents/lb)"),
    "us_refinery_utilization_pct": ("WPULEUS3", "U.S. / Gulf Coast Refinery Utilization (%)"),
    "us_commercial_crude_stocks_kbbl": ("WCESTUS1", "Commercial Crude Oil Inventories (Thousand Bbl)")
}

def classify_vessel(ship_type_code):
    if ship_type_code is None:
        return "Commercial Vessel"
    if 70 <= ship_type_code <= 79:
        return "Cargo / Bulk Grain & Ag"
    if 80 <= ship_type_code <= 89:
        return "Crude / Petrochemical Tanker"
    if ship_type_code in (31, 32, 52):
        return "Tug / Barge Tow"
    if 50 <= ship_type_code <= 55:
        return "Port Pilot / Dredge / Service"
    return "Commercial Vessel"

def get_fred_latest(series_id):
    if not FRED_KEY:
        return None
    url = f"https:{DBL}api.stlouisfed.org{SLASH}fred{SLASH}series{SLASH}observations"
    params = {"series_id": series_id, "api_key": FRED_KEY, "file_type": "json", "sort_order": "desc", "limit": 5}
    try:
        r = requests.get(url, params=params, headers={"Accept": "application/json"}, timeout=15).json()
        for obs in r.get("observations", []):
            if obs.get("value") not in (None, "."):
                return float(obs["value"])
    except Exception as e:
        print(f"FRED error ({series_id}): {e}")
    return None

def get_noaa_river_gauge(lid):
    """Fetch live Mississippi River stage (ft) & flow from NOAA National Water Prediction Service."""
    url = f"https:{DBL}api.water.noaa.gov{SLASH}nwps{SLASH}v1{SLASH}gauges{SLASH}{lid}"
    try:
        r = requests.get(url, headers={"Accept": "application/json"}, timeout=12).json()
        obs = r.get("status", {}).get("observed", {})
        primary = obs.get("primary")
        secondary = obs.get("secondary")
        flood_cat = obs.get("floodCategory", "normal")
        valid_time = obs.get("validTime")
        stage_ft = round(float(primary), 2) if primary is not None else None

        # Deep-draft ocean vessels on the Lower Mississippi maintain ~45-48 ft project channel depth;
        # stages below 4.0 ft at Carrollton signal low-water draft/saltwater sill restrictions.
        if stage_ft is None:
            draft_status = "Gauge Unavailable"
            max_safe_draft_ft = None
        elif stage_ft < 3.5:
            draft_status = "Low-Water Draft Restriction Risk"
            max_safe_draft_ft = round(43.5 + stage_ft, 1)
        elif stage_ft >= 15.0:
            draft_status = "High-Water Current / Tow Restriction"
            max_safe_draft_ft = 47.0
        else:
            draft_status = "Optimal Deep-Draft Channel"
            max_safe_draft_ft = 47.0

        return {
            "gauge_id": lid,
            "name": r.get("name", lid),
            "stage_ft": stage_ft,
            "flow_kcfs": round(float(secondary), 1) if secondary is not None else None,
            "flood_category": flood_cat,
            "draft_status": draft_status,
            "max_safe_draft_ft": max_safe_draft_ft,
            "observed_at": valid_time
        }
    except Exception as e:
        print(f"NOAA river gauge error ({lid}): {e}")
        return {"gauge_id": lid, "stage_ft": None, "draft_status": "Gauge Unavailable"}

def get_noaa_weather(lat, lon):
    headers = {"User-Agent": "ArcturusTerminalTelemetry/1.0 (contact@arcturusterminal.com)", "Accept": "application/geo+json"}
    try:
        pt = requests.get(f"https:{DBL}api.weather.gov{SLASH}points{SLASH}{lat},{lon}", headers=headers, timeout=10).json()
        fc = requests.get(pt["properties"]["forecastHourly"], headers=headers, timeout=10).json()
        periods = fc["properties"]["periods"]
        if periods:
            cur = periods[0]
            return f"{cur['temperature']}°F, {cur['shortForecast']}, Wind {cur['windSpeed']}"
    except Exception:
        pass
    return "Telemetry Nominal"

def get_nws_marine_river_alerts():
    """Fetch active NWS marine, river flood, dense fog, or gale advisories in Louisiana."""
    url = f"https:{DBL}api.weather.gov{SLASH}alerts{SLASH}active?area=LA"
    headers = {"User-Agent": "ArcturusTerminalTelemetry/1.0 (contact@arcturusterminal.com)", "Accept": "application/geo+json"}
    relevant_keywords = ("Flood", "Marine", "Fog", "Gale", "Small Craft", "River", "Storm", "Hurricane", "Wind")
    alerts = []
    try:
        r = requests.get(url, headers=headers, timeout=10).json()
        for feat in r.get("features", []):
            props = feat.get("properties", {})
            event = props.get("event", "")
            if any(k.lower() in event.lower() for k in relevant_keywords):
                alerts.append({
                    "event": event,
                    "severity": props.get("severity", "Unknown"),
                    "area": (props.get("areaDesc") or "")[:120],
                    "headline": (props.get("headline") or event)[:160]
                })
    except Exception as e:
        print(f"NWS alerts error: {e}")
    return alerts[:8]

def get_faa_delays():
    delays = {}
    try:
        text = requests.get(f"https:{DBL}nasstatus.faa.gov{SLASH}api{SLASH}airport-status-information", timeout=10).text
        for code in ["MSY", "SDF"]:
            delays[code] = "Delay Advisory" if f">{code}<" in text else "Normal Operations"
    except Exception:
        for code in ["MSY", "SDF"]:
            delays[code] = "Normal Operations"
    return delays

def get_live_flights(lat, lon, radius_nm=15):
    flights = []
    try:
        url = f"https:{DBL}api.adsb.lol{SLASH}v2{SLASH}lat{SLASH}{lat}{SLASH}lon{SLASH}{lon}{SLASH}dist{SLASH}{radius_nm}"
        r = requests.get(url, timeout=10).json()
        for ac in r.get("ac", []):
            callsign = (ac.get("flight") or "").strip()
            raw_alt = ac.get("alt_baro")
            if not callsign or raw_alt is None:
                continue
            if str(raw_alt).lower() == "ground":
                alt_num = 0
            else:
                try:
                    alt_num = int(float(raw_alt))
                except ValueError:
                    continue
            if alt_num <= 10000:
                v_rate = ac.get("baro_rate") or ac.get("geom_rate") or 0
                try:
                    v_rate = int(float(v_rate))
                except ValueError:
                    v_rate = 0

                if alt_num == 0:
                    phase = "Ground/Taxi"
                elif v_rate < -250:
                    phase = "Approach"
                elif v_rate > 250:
                    phase = "Departure"
                else:
                    phase = "Terminal Level"

                flights.append({
                    "callsign": callsign,
                    "type": ac.get("t", "N/A"),
                    "alt_ft": alt_num,
                    "speed_kt": round(float(ac.get("gs") or 0), 1),
                    "vert_rate_fpm": v_rate,
                    "phase": phase,
                    "lat": round(float(ac["lat"]), 4) if ac.get("lat") is not None else None,
                    "lon": round(float(ac["lon"]), 4) if ac.get("lon") is not None else None
                })
    except Exception as e:
        print(f"ADS-B flight error ({lat},{lon}): {e}")
    return flights

COMMERCIAL_SHIP_TYPES = set(range(70, 90)) | {31, 32, 50, 51, 52, 53, 54, 55}

async def scan_ais_ports(duration_sec=185):
    """Listen for 185s so stationary/berthed grain & oil vessels (which report every 180s) are captured."""
    results = {code: {"vessels": {}, "anchored": set()} for code in PORTS}
    vessel_types = {}
    vessel_names = {}
    if not AISSTREAM_KEY:
        return results
    boxes = [cfg["box"] for cfg in PORTS.values()]
    sub_msg = {
        "APIKey": AISSTREAM_KEY,
        "BoundingBoxes": boxes,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
    }
    ws_url = f"wss:{DBL}stream.aisstream.io{SLASH}v0{SLASH}stream"
    try:
        async with websockets.connect(ws_url, open_timeout=15) as ws:
            await ws.send(json.dumps(sub_msg))
            end_time = asyncio.get_event_loop().time() + duration_sec
            while asyncio.get_event_loop().time() < end_time:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msg = json.loads(raw)
                    msg_type = msg.get("MessageType", "")
                    meta = msg.get("MetaData", {})
                    mmsi = str(meta.get("MMSI", ""))
                    if not mmsi:
                        continue

                    if msg_type == "ShipStaticData":
                        static = msg.get("Message", {}).get("ShipStaticData", {})
                        ship_type = static.get("Type")
                        static_name = (static.get("Name") or meta.get("ShipName") or "").strip()
                        if ship_type is not None:
                            vessel_types[mmsi] = int(ship_type)
                        if static_name:
                            vessel_names[mmsi] = static_name
                        continue

                    if msg_type == "PositionReport":
                        pos = msg.get("Message", {}).get("PositionReport", {})
                        lat = meta.get("latitude")
                        lon = meta.get("longitude")
                        sog = pos.get("Sog", 0)
                        cog = pos.get("Cog")
                        known_type = vessel_types.get(mmsi)

                        if known_type is not None and known_type not in COMMERCIAL_SHIP_TYPES:
                            continue

                        ship_name = (vessel_names.get(mmsi) or meta.get("ShipName") or f"COMMERCIAL_VESSEL_{mmsi}").strip()
                        if lat is not None and lon is not None:
                            for code, cfg in PORTS.items():
                                (min_lat, min_lon), (max_lat, max_lon) = cfg["box"]
                                if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                                    results[code]["vessels"][mmsi] = {
                                        "mmsi": mmsi,
                                        "name": ship_name,
                                        "ship_type_code": known_type,
                                        "vessel_category": classify_vessel(known_type),
                                        "speed_kt": round(float(sog or 0), 1),
                                        "status": "Berthed/Idle" if (sog is not None and sog < 0.5) else "Underway",
                                        "heading_deg": round(float(cog), 1) if cog is not None else None,
                                        "lat": round(float(lat), 4),
                                        "lon": round(float(lon), 4)
                                    }
                                    if sog is not None and sog < 0.5:
                                        results[code]["anchored"].add(mmsi)
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        print(f"AISStream error: {e}")
    return results

def compute_corridor_congestion_index(nola_vessels, btr_vessels, south_la_vessels, carrollton_stage, msy_flights):
    """Proprietary 0-100 Arcturus Lower Mississippi Corridor Congestion & Bottleneck Score."""
    total_river_vessels = nola_vessels + btr_vessels + south_la_vessels
    vessel_score = min(45.0, (total_river_vessels / 45.0) * 45.0)

    if carrollton_stage is None:
        river_penalty = 10.0
    elif carrollton_stage < 3.5:
        river_penalty = 35.0
    elif carrollton_stage < 5.0:
        river_penalty = 20.0
    elif carrollton_stage >= 15.0:
        river_penalty = 25.0
    else:
        river_penalty = 5.0

    air_score = min(20.0, (msy_flights / 15.0) * 20.0)
    return round(min(100.0, vessel_score + river_penalty + air_score), 1)

async def main():
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 1. Fetch Louisiana & National Commodity / Energy Tape
    commodities = {}
    for key, (series_id, label) in COMMODITY_SERIES.items():
        commodities[key] = {
            "series_id": series_id,
            "label": label,
            "value": get_fred_latest(series_id)
        }

    diesel = commodities["us_retail_diesel_usd_gal"]["value"]
    freight_idx = commodities["freight_tsi_index"]["value"]

    # 2. Fetch NOAA Baton Rouge (BTRL1) & Carrollton/NOLA (NORL1) Mississippi River Stage Gauges
        river_gauges = {
        "NORL1": get_noaa_river_gauge("NORL1"),
        "BTRL1": get_noaa_river_gauge("BTRL1")
    }
    marine_alerts = get_nws_marine_river_alerts()


    # 3. Fetch FAA Status & 185s Maritime AIS Sweep across Baton Rouge -> South LA -> NOLA + Miami
    faa_status = get_faa_delays()
    ais_data = await scan_ais_ports(duration_sec=185)

    rows = []
    for code, cfg in PORTS.items():
        vessel_list = list(ais_data[code]["vessels"].values())
        v_count = len(vessel_list)
        a_count = len(ais_data[code]["anchored"])

        # Breakdown by commodity/vessel category
        category_counts = {}
        for v in vessel_list:
            cat = v.get("vessel_category", "Commercial Vessel")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        weather = get_noaa_weather(cfg["lat"], cfg["lon"])
        status = "High Activity" if v_count >= 12 else ("Moderate Traffic" if v_count >= 4 else "Active / Low Density")
        rows.append({
            "recorded_at": now_iso,
            "hub_code": code,
            "hub_name": cfg["name"],
            "hub_type": "seaport",
            "region": cfg["region"],
            "status": status,
            "primary_metric": v_count,
            "secondary_metric": a_count,
            "weather_summary": weather,
            "diesel_price_usd": diesel,
            "freight_index": freight_idx,
            "raw_payload": {
                "active_vessels": v_count,
                "stationary_or_berthed": a_count,
                "category_breakdown": category_counts,
                "tracked_ships": vessel_list[:25]
            }
        })

    msy_flight_count = 0
    for code, cfg in AIRPORTS.items():
        status = faa_status.get(code, "Normal Operations")
        weather = get_noaa_weather(cfg["lat"], cfg["lon"])
        live_flights = get_live_flights(cfg["lat"], cfg["lon"], radius_nm=15)
        if code == "MSY":
            msy_flight_count = len(live_flights)
        rows.append({
            "recorded_at": now_iso,
            "hub_code": code,
            "hub_name": cfg["name"],
            "hub_type": "airport",
            "region": cfg["region"],
            "status": status,
            "primary_metric": len(live_flights),
            "secondary_metric": 1 if "Delay" in status else 0,
            "weather_summary": weather,
            "diesel_price_usd": diesel,
            "freight_index": freight_idx,
            "raw_payload": {
                "faa_status": status,
                "icao": cfg["icao"],
                "tracked_flights": live_flights
            }
        })

    # 4. Append Proprietary Lower Mississippi Corridor Economics & River Intelligence Row
    congestion_index = compute_corridor_congestion_index(
        nola_vessels=len(ais_data["PORT_NOLA"]["vessels"]),
        btr_vessels=len(ais_data["PORT_BTR"]["vessels"]),
        south_la_vessels=len(ais_data["PORT_SOUTH_LA"]["vessels"]),
        carrollton_stage=river_gauges["NORL1"].get("stage_ft"),
        msy_flights=msy_flight_count
    )

    rows.append({
        "recorded_at": now_iso,
        "hub_code": "LA_CORRIDOR_ECON",
        "hub_name": "Lower Mississippi River & Louisiana Commodity Intelligence",
        "hub_type": "corridor_index",
        "region": "Lower Mississippi",
        "status": river_gauges["NORL1"].get("draft_status", "Telemetry Nominal"),
        "primary_metric": int(round(congestion_index)),
        "secondary_metric": int(round((river_gauges["NORL1"].get("stage_ft") or 0) * 10)),
        "weather_summary": f"Carrollton Stage: {river_gauges['NORL1'].get('stage_ft')} ft | Baton Rouge Stage: {river_gauges['BTRL1'].get('stage_ft')} ft",
        "diesel_price_usd": diesel,
        "freight_index": freight_idx,
        "raw_payload": {
            "arcturus_congestion_index": congestion_index,
            "river_gauges": river_gauges,
            "marine_river_alerts": marine_alerts,
            "commodities": commodities,
            "corridor_vessel_totals": {
                "PORT_NOLA": len(ais_data["PORT_NOLA"]["vessels"]),
                "PORT_BTR": len(ais_data["PORT_BTR"]["vessels"]),
                "PORT_SOUTH_LA": len(ais_data["PORT_SOUTH_LA"]["vessels"])
            }
        }
    })

    if SUPABASE_URL and SUPABASE_KEY:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        sb.table("hub_snapshots").insert(rows).execute()
        print("Inserted upgraded corridor, river gauge & commodity rows into Supabase.")

        live_output = {
        "updated_at": now_iso,
        "macro": {
            "diesel_price_usd": diesel,
            "freight_tsi": freight_idx,
            "arcturus_congestion_index": congestion_index
        },
        "river_gauges": river_gauges,
        "marine_river_alerts": marine_alerts,
        "commodities": commodities,
        "hubs": rows
    }

    with open("live.json", "w") as f:
        json.dump(live_output, f, indent=2)
    print("Updated live.json successfully.")

if __name__ == "__main__":
    asyncio.run(main())

