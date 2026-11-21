import os
import json
import asyncio
import datetime
import requests
import websockets
from supabase import create_client

AISSTREAM_KEY = os.environ.get("AISSTREAM_API_KEY", "").strip()
FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

PORTS = {
    "PORT_NOLA": {
        "name": "Port of New Orleans",
        "region": "Gulf Coast",
        "lat": 29.93,
        "lon": -90.06,
        "box": [[29.85, -90.20], [30.02, -89.90]]
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
    url = "https://stlouisfed.org"
    params = {
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5
    }
    try:
        r = requests.get(url, params=params, timeout=15).json()
        for obs in r.get("observations", []):
            if obs.get("value") not in (None, "."):
                return float(obs["value"])
    except Exception as e:
        print(f"FRED error ({series_id}): {e}")
    return None

def get_noaa_weather(lat, lon):
    headers = {"User-Agent": "WaybillIndexTelemetry/1.0 (contact@example.com)"}
    try:
        pt_url = f"https://weather.gov{lat},{lon}"
        pt = requests.get(pt_url, headers=headers, timeout=10).json()
        forecast_url = pt["properties"]["forecastHourly"]
        fc = requests.get(forecast_url, headers=headers, timeout=10).json()
        current = fc["properties"]["periods"][0]
        return f"{current['temperature']}°F, {current['shortForecast']}, Wind {current['windSpeed']}"
    except Exception as e:
        print(f"NOAA error ({lat},{lon}): {e}")
        return "Telemetry Nominal"

def get_faa_delays():
    delays = {}
    try:
        r = requests.get("https://faa.gov", timeout=10)
        text = r.text
        for code in ["MSY", "SDF"]:
            delays[code] = "Delay Advisory" if f">{code}<" in text else "Normal Operations"
    except Exception as e:
        print(f"FAA error: {e}")
        for code in ["MSY", "SDF"]:
            delays[code] = "Normal Operations"
    return delays

async def scan_ais_ports(duration_sec=20):
    results = {code: {"vessels": set(), "anchored": set()} for code in PORTS}
    if not AISSTREAM_KEY:
        return results
    boxes = [cfg["box"] for cfg in PORTS.values()]
    sub_msg = {
        "APIKey": AISSTREAM_KEY,
        "BoundingBoxes": boxes,
        "FilterMessageTypes": ["PositionReport"]
    }
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=15) as ws:
            await ws.send(json.dumps(sub_msg))
            end_time = asyncio.get_event_loop().time() + duration_sec
            while asyncio.get_event_loop().time() < end_time:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msg = json.loads(raw)
                    meta = msg.get("MetaData", {})
                    pos = msg.get("Message", {}).get("PositionReport", {})
                    mmsi = meta.get("MMSI")
                    lat = meta.get("latitude")
                    lon = meta.get("longitude")
                    sog = pos.get("Sog", 0)
                    if mmsi and lat is not None and lon is not None:
                        for code, cfg in PORTS.items():
                            (min_lat, min_lon), (max_lat, max_lon) = cfg["box"]
                            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                                results[code]["vessels"].add(mmsi)
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
        v_count = len(ais_data[code]["vessels"])
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
            "raw_payload": {"active_vessels": v_count, "stationary_or_berthed": a_count}
        })

    for code, cfg in AIRPORTS.items():
        status = faa_status.get(code, "Normal Operations")
        weather = get_noaa_weather(cfg["lat"], cfg["lon"])
        delay_flag = 1 if "Delay" in status else 0
        rows.append({
            "recorded_at": now_iso,
            "hub_code": code,
            "hub_name": cfg["name"],
            "hub_type": "airport",
            "region": cfg["region"],
            "status": status,
            "primary_metric": delay_flag,
            "secondary_metric": 0,
            "weather_summary": weather,
            "diesel_price_usd": diesel,
            "freight_index": freight_idx,
            "raw_payload": {"faa_status": status, "icao": cfg["icao"]}
        })

    if SUPABASE_URL and SUPABASE_KEY:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        sb.table("hub_snapshots").insert(rows).execute()
        print("Inserted rows into Supabase.")

    live_output = {
        "updated_at": now_iso,
        "macro": {
            "diesel_price_usd": diesel,
            "freight_tsi": freight_idx
        },
        "hubs": rows
    }
    with open("live.json", "w") as f:
        json.dump(live_output, f, indent=2)
    print("Updated live.json successfully.")

if __name__ == "__main__":
    asyncio.run(main())
