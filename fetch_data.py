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
        "box": [[29.88, -90.28], [30.00, -89.94]]
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

def get_noaa_weather(lat, lon):
    headers = {"User-Agent": "PayloadTrackingTelemetry/1.0 (contact@example.com)", "Accept": "application/geo+json"}
    try:
        pt = requests.get(f"https:{DBL}api.weather.gov{SLASH}points{SLASH}{lat},{lon}", headers=headers, timeout=10).json()
        fc = requests.get(pt["properties"]["forecastHourly"], headers=headers, timeout=10).json()
        cur = fc["properties"]["periods"]
        return f"{cur['temperature']}°F, {cur['shortForecast']}, Wind {cur['windSpeed']}"
    except Exception:
        return "Telemetry Nominal"

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
                    "phase": phase
                })
    except Exception as e:
        print(f"ADS-B flight error ({lat},{lon}): {e}")
    return flights


async def scan_ais_ports(duration_sec=45):
    results = {code: {"vessels": {}, "anchored": set()} for code in PORTS}
    if not AISSTREAM_KEY:
        return results
    boxes = [cfg["box"] for cfg in PORTS.values()]
    sub_msg = {
        "APIKey": AISSTREAM_KEY,
        "BoundingBoxes": boxes,
        "FilterMessageTypes": ["PositionReport"]
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
                    meta = msg.get("MetaData", {})
                    pos = msg.get("Message", {}).get("PositionReport", {})
                    mmsi = str(meta.get("MMSI", ""))
                    ship_name = (meta.get("ShipName") or f"VESSEL_{mmsi}").strip()
                    lat = meta.get("latitude")
                    lon = meta.get("longitude")
                    sog = pos.get("Sog", 0)
                    if mmsi and lat is not None and lon is not None:
                        for code, cfg in PORTS.items():
                            (min_lat, min_lon), (max_lat, max_lon) = cfg["box"]
                            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                                results[code]["vessels"][mmsi] = {
                                    "mmsi": mmsi,
                                    "name": ship_name,
                                    "speed_kt": round(float(sog or 0), 1),
                                    "status": "Berthed/Idle" if (sog is not None and sog < 0.5) else "Underway"
                                }
                                if sog is not None and sog < 0.5:
                                    results[code]["anchored"].add(mmsi)
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        print(f"AISStream error: {e}")
    return results

async def main():
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    diesel = get_fred_latest("GASDESW")
    freight_idx = get_fred_latest("TSIFRGHT")
    faa_status = get_faa_delays()
    ais_data = await scan_ais_ports(duration_sec=20)

    rows = []
    for code, cfg in PORTS.items():
        vessel_list = list(ais_data[code]["vessels"].values())
        v_count = len(vessel_list)
        a_count = len(ais_data[code]["anchored"])
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
                "tracked_ships": vessel_list[:10]
            }
        })

    for code, cfg in AIRPORTS.items():
        status = faa_status.get(code, "Normal Operations")
        weather = get_noaa_weather(cfg["lat"], cfg["lon"])
        live_flights = get_live_flights(cfg["lat"], cfg["lon"], radius_nm=15)
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

    if SUPABASE_URL and SUPABASE_KEY:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        sb.table("hub_snapshots").insert(rows).execute()
        print("Inserted rows with individual ship & flight telemetry into Supabase.")

    live_output = {
        "updated_at": now_iso,
        "macro": {"diesel_price_usd": diesel, "freight_tsi": freight_idx},
        "hubs": rows
    }
    with open("live.json", "w") as f:
        json.dump(live_output, f, indent=2)
    print("Updated live.json successfully.")

if __name__ == "__main__":
    asyncio.run(main())
