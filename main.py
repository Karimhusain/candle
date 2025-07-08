import requests
import datetime
import time
import json
import math
import logging
import os
import threading
from tenacity import retry, wait_exponential, stop_after_attempt, after_log
import websocket # pip install websocket-client
import pytz # pip install pytz

# --- Konfigurasi Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATIONS ---
SYMBOL = "BTCUSDT" # Ganti sesuai kebutuhan Anda, contoh: "ETHUSDT"
INTERVALS = ["1h", "4h", "1d"] # Timeframes yang akan dipantau
CANDLE_LIMIT = { # Batas candle yang diambil untuk analisis per interval
    "1h": 300, # Diperluas
    "4h": 200, # Diperluas
    "1d": 500 # Sangat diperluas untuk deteksi chart pattern dan SMC yang lebih baik
}
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1392208407943184486/rTi7iaRdIKn_PW-_u2jZknXX4Dmty5m6kiKqttpueYsrpaj9pQi7-tYsSkAjqU3Dc6DM" # <<< PENTING: GANTI DENGAN URL WEBHOOK DISCORD ANDA

# Frekuensi analisis real-time (dalam detik) untuk live candle dan fallback
REAL_TIME_SCAN_INTERVAL_SECONDS = 30 * 60 # Setiap 30 menit

# Base URL untuk WebSocket Binance (Production)
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"

# Zona waktu untuk output
TARGET_TIMEZONE = pytz.timezone('Asia/Jakarta') # WIB (UTC+7)

# Cache untuk menyimpan data klines lengkap (historis + update live) untuk analisis
# Format: {interval: [candle_obj, ...]}
_candle_data_cache = {interval: [] for interval in INTERVALS}
# Cache untuk menyimpan candle yang sedang berjalan/live dari WebSocket
_live_websocket_candle_data = {interval: None for interval in INTERVALS}
# Cache untuk menyimpan waktu penutupan candle terakhir yang sudah diproses secara final
last_processed_final_candle_time = {interval: None for interval in INTERVALS}

# Lock untuk mengakses _candle_data_cache agar aman dari race condition antara thread
data_cache_lock = threading.Lock()

# --- BINANCE KLINE API ---
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), after=after_log(logger, logging.WARNING))
def get_klines_rest(symbol: str, interval: str, limit: int) -> list:
    """Mengambil data klines (candle) dari Binance API REST dengan retry."""
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Akan memunculkan HTTPError untuk status code 4xx/5xx
        data = response.json()
        
        if not data:
            logger.warning(f"Tidak ada data klines yang diterima dari REST API untuk {symbol} - {interval}.")
            return []

        candles = []
        for c in data:
            candle = {
                "time": datetime.datetime.fromtimestamp(c[0] / 1000, tz=pytz.utc), # Waktu pembukaan candle (UTC)
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
                "close_time": datetime.datetime.fromtimestamp(c[6] / 1000, tz=pytz.utc), # Waktu penutupan candle (UTC)
                "is_final_bar": True # Data dari REST API selalu final
            }
            candles.append(candle)
        return candles
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching klines from REST API for {symbol} - {interval}: {e}")
        raise # Re-raise for tenacity to catch

# --- UTILITIES ---
def get_candle_properties(c: dict) -> dict:
    """Menghitung properti dasar candle untuk analisis pola."""
    body_abs = abs(c["close"] - c["open"])
    full_range = c["high"] - c["low"]
    upper_shadow = c["high"] - max(c["open"], c["close"])
    lower_shadow = min(c["open"], c["close"]) - c["low"]
    
    if full_range == 0: # Hindari pembagian dengan nol
        return {
            "body_abs": body_abs,
            "full_range": full_range,
            "upper_shadow": upper_shadow,
            "lower_shadow": lower_shadow,
            "is_bullish": c["close"] > c["open"],
            "is_bearish": c["close"] < c["open"],
            "is_doji_like": True,
            "body_to_range_ratio": 0,
            "upper_shadow_to_range_ratio": 0,
            "lower_shadow_to_range_ratio": 0
        }

    return {
        "body_abs": body_abs,
        "full_range": full_range,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "is_bullish": c["close"] > c["open"],
        "is_bearish": c["close"] < c["open"],
        # Diperbarui: sedikit melonggarkan kriteria doji
        "is_doji_like": body_abs / full_range < 0.15, # Dari 0.1 menjadi 0.15
        "body_to_range_ratio": body_abs / full_range,
        "upper_shadow_to_range_ratio": upper_shadow / full_range,
        "lower_shadow_to_range_ratio": lower_shadow / full_range
    }

def get_trend_direction(candles: list, lookback_period: int = 10) -> str:
    """Menentukan arah tren berdasarkan Simple Moving Average (SMA) yang sederhana."""
    if len(candles) < lookback_period:
        return "Unknown"
    
    closes = [c["close"] for c in candles[-lookback_period:]]
    if len(closes) < lookback_period: # Pastikan ada cukup data
        return "Unknown"

    sma_prev = sum(closes[:-1]) / (lookback_period - 1)
    sma_current = sum(closes) / lookback_period

    if sma_current > sma_prev:
        return "Uptrend"
    elif sma_current < sma_prev:
        return "Downtrend"
    else:
        return "Sideways"

def get_time_progress(candle_open_time: datetime.datetime, interval_str: str) -> float:
    """Menghitung progres waktu candle yang sedang berjalan."""
    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    
    interval_seconds_map = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800,
        "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000 # Approximation for 1M
    }
    
    interval_sec = interval_seconds_map.get(interval_str)
    if not interval_sec:
        return 0.0 # Unknown interval, cannot calculate progress

    elapsed_time = (now - candle_open_time).total_seconds()
    
    if elapsed_time <= 0: return 0.0 # Belum mulai atau waktu tidak valid
    if elapsed_time >= interval_sec: return 1.0 # Sudah melewati waktu penutupan

    return elapsed_time / interval_sec

# --- PRICE ACTION CANDLESTICK DETECTION ---
# Diperbarui: Melonggarkan beberapa rasio body/shadow untuk deteksi lebih luas
def is_bullish_engulfing(c1, c2, confirm_volume=False):
    if not (c1["close"] < c1["open"] and c2["close"] > c2["open"]): return False
    engulfs = c2["close"] > c1["open"] and c2["open"] < c1["close"] and \
              (c2["close"] - c2["open"]) > (c1["open"] - c1["close"]) * 0.9 # Melonggarkan sedikit engulfment
    if confirm_volume: return engulfs and c2["volume"] > c1["volume"]
    return engulfs

def is_bearish_engulfing(c1, c2, confirm_volume=False):
    if not (c1["close"] > c1["open"] and c2["close"] < c2["open"]): return False
    engulfs = c2["close"] < c1["open"] and c2["open"] > c1["close"] and \
              (c2["open"] - c2["close"]) > (c1["close"] - c1["open"]) * 0.9 # Melonggarkan sedikit engulfment
    if confirm_volume: return engulfs and c2["volume"] > c1["volume"]
    return engulfs

def is_pin_bar(c):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.4: return False # Dari 0.35 menjadi 0.4
    return (props["is_bullish"] and props["lower_shadow"] >= 1.8 * props["body_abs"] and props["upper_shadow"] < 0.2 * props["full_range"]) or \
           (props["is_bearish"] and props["upper_shadow"] >= 1.8 * props["body_abs"] and props["lower_shadow"] < 0.2 * props["full_range"])

def is_doji(c):
    props = get_candle_properties(c)
    return props["is_doji_like"] # Disesuaikan oleh is_doji_like di get_candle_properties

def is_hammer(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.4: return False # Dari 0.35 menjadi 0.4
    is_hammer_shape = props["lower_shadow"] >= 1.8 * props["body_abs"] and props["upper_shadow"] < 0.15 * props["full_range"] # Dari 2x menjadi 1.8x, upper shadow dari 0.1 menjadi 0.15
    return is_hammer_shape if trend_context == "Downtrend" else is_hammer_shape

def is_hanging_man(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.4: return False # Dari 0.35 menjadi 0.4
    is_hanging_man_shape = props["lower_shadow"] >= 1.8 * props["body_abs"] and props["upper_shadow"] < 0.15 * props["full_range"] # Dari 2x menjadi 1.8x, upper shadow dari 0.1 menjadi 0.15
    return is_hanging_man_shape if trend_context == "Uptrend" else is_hanging_man_shape

def is_inverted_hammer(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.4: return False # Dari 0.35 menjadi 0.4
    is_inverted_hammer_shape = props["upper_shadow"] >= 1.8 * props["body_abs"] and props["lower_shadow"] < 0.15 * props["full_range"] # Dari 2x menjadi 1.8x, lower shadow dari 0.1 menjadi 0.15
    return is_inverted_hammer_shape if trend_context == "Downtrend" else is_inverted_hammer_shape

def is_shooting_star(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.4: return False # Dari 0.35 menjadi 0.4
    is_shooting_star_shape = props["upper_shadow"] >= 1.8 * props["body_abs"] and props["lower_shadow"] < 0.15 * props["full_range"] # Dari 2x menjadi 1.8x, lower shadow dari 0.1 menjadi 0.15
    return is_shooting_star_shape if trend_context == "Uptrend" else is_shooting_star_shape

def is_morning_star(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bearish"] and props3["is_bullish"] and props1["body_to_range_ratio"] > 0.4 and props3["body_to_range_ratio"] > 0.4): return False # Dari 0.5 menjadi 0.4
    if not (props2["is_doji_like"] or props2["body_to_range_ratio"] < 0.4): return False # Dari 0.3 menjadi 0.4
    return c3["close"] > (c1["open"] + c1["close"]) / 2

def is_evening_star(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bullish"] and props3["is_bearish"] and props1["body_to_range_ratio"] > 0.4 and props3["body_to_range_ratio"] > 0.4): return False # Dari 0.5 menjadi 0.4
    if not (props2["is_doji_like"] or props2["body_to_range_ratio"] < 0.4): return False # Dari 0.3 menjadi 0.4
    return c3["close"] < (c1["open"] + c1["close"]) / 2

def is_inside_bar(c1, c2):
    # Diperbarui: Memastikan c2 body ada di dalam c1 body (lebih ketat tapi esensi inside bar)
    return c2["high"] < c1["high"] and c2["low"] > c1["low"]

def is_outside_bar(c1, c2):
    return c2["high"] > c1["high"] and c2["low"] < c1["low"]

def is_tweezer_top(c1, c2):
    # Diperbarui: Sedikit melonggarkan toleransi puncak
    if not (abs(c1["high"] - c2["high"]) / c1["high"] < 0.001): return False # Dari 0.0005 menjadi 0.001
    return c1["close"] > c1["open"] and c2["close"] < c2["open"]

def is_tweezer_bottom(c1, c2):
    # Diperbarui: Sedikit melonggarkan toleransi dasar
    if not (abs(c1["low"] - c2["low"]) / c1["low"] < 0.001): return False # Dari 0.0005 menjadi 0.001
    return c1["close"] < c1["open"] and c2["close"] > c2["open"]

def is_three_white_soldiers(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bullish"] and props2["is_bullish"] and props3["is_bullish"] and \
            props1["body_to_range_ratio"] > 0.5 and props2["body_to_range_ratio"] > 0.5 and props3["body_to_range_ratio"] > 0.5): return False # Dari 0.6 menjadi 0.5
    if not (c2["close"] > c1["close"] and c3["close"] > c2["close"]): return False
    if not (c2["open"] > c1["open"] and c2["open"] < c1["close"] * 1.01): return False # Membolehkan gap up sedikit
    if not (c3["open"] > c2["open"] and c3["open"] < c2["close"] * 1.01): return False # Membolehkan gap up sedikit
    return True

def is_three_black_crows(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bearish"] and props2["is_bearish"] and props3["is_bearish"] and \
            props1["body_to_range_ratio"] > 0.5 and props2["body_to_range_ratio"] > 0.5 and props3["body_to_range_ratio"] > 0.5): return False # Dari 0.6 menjadi 0.5
    if not (c2["close"] < c1["close"] and c3["close"] < c2["close"]): return False
    if not (c2["open"] < c1["open"] and c2["open"] > c1["close"] * 0.99): return False # Membolehkan gap down sedikit
    if not (c3["open"] < c2["open"] and c3["open"] > c2["close"] * 0.99): return False # Membolehkan gap down sedikit
    return True

def is_dark_cloud_cover(c1, c2):
    if not (c1["close"] > c1["open"] and c2["close"] < c2["open"]): return False
    if not (c2["open"] > c1["high"]): return False
    if not (c2["close"] < (c1["open"] + c1["close"]) / 2 and c2["close"] > c1["open"]): return False
    props2 = get_candle_properties(c2)
    if props2["body_to_range_ratio"] < 0.5: return False # Memastikan candle bearish cukup kuat
    return True

def is_piercing_pattern(c1, c2):
    if not (c1["close"] < c1["open"] and c2["close"] > c2["open"]): return False
    if not (c2["open"] < c1["low"]): return False
    if not (c2["close"] > (c1["open"] + c1["close"]) / 2 and c2["close"] < c1["open"]): return False
    props2 = get_candle_properties(c2)
    if props2["body_to_range_ratio"] < 0.5: return False # Memastikan candle bullish cukup kuat
    return True

def is_bullish_harami(c1, c2):
    props1, props2 = get_candle_properties(c1), get_candle_properties(c2)
    if not (props1["is_bearish"] and props2["is_bullish"] and props1["body_to_range_ratio"] > 0.5 and props2["body_to_range_ratio"] < 0.5): return False # Dari 0.6 & 0.4 menjadi 0.5 & 0.5
    return c2["open"] > c1["close"] and c2["close"] < c1["open"]

def is_bearish_harami(c1, c2):
    props1, props2 = get_candle_properties(c1), get_candle_properties(c2)
    if not (props1["is_bullish"] and props2["is_bearish"] and props1["body_to_range_ratio"] > 0.5 and props2["body_to_range_ratio"] < 0.5): return False # Dari 0.6 & 0.4 menjadi 0.5 & 0.5
    return c2["open"] < c1["close"] and c2["close"] > c1["open"]

def detect_price_action(candles: list, enable_volume_confirmation: bool = False) -> list: # Default False
    patterns = []
    if len(candles) < 3: return patterns
    
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    current_trend = get_trend_direction(candles[:-1])

    # Pola 2 Candle
    if is_bullish_engulfing(c2, c3, confirm_volume=enable_volume_confirmation): patterns.append("Bullish Engulfing")
    if is_bearish_engulfing(c2, c3, confirm_volume=enable_volume_confirmation): patterns.append("Bearish Engulfing")
    if is_inside_bar(c2, c3): patterns.append("Inside Bar")
    if is_outside_bar(c2, c3): patterns.append("Outside Bar")
    if is_tweezer_top(c2, c3): patterns.append("Tweezer Top")
    if is_tweezer_bottom(c2, c3): patterns.append("Tweezer Bottom")
    if is_dark_cloud_cover(c2, c3): patterns.append("Dark Cloud Cover")
    if is_piercing_pattern(c2, c3): patterns.append("Piercing Pattern")
    if is_bullish_harami(c2, c3): patterns.append("Bullish Harami")
    if is_bearish_harami(c2, c3): patterns.append("Bearish Harami")

    # Pola 1 Candle
    if is_pin_bar(c3): patterns.append("Pin Bar")
    if is_doji(c3): patterns.append("Doji")
    # Konteks trend sangat penting untuk Hammer/Shooting Star
    if is_hammer(c3, trend_context=current_trend): patterns.append(f"Hammer (in {current_trend} context)")
    if is_hanging_man(c3, trend_context=current_trend): patterns.append(f"Hanging Man (in {current_trend} context)")
    if is_inverted_hammer(c3, trend_context=current_trend): patterns.append(f"Inverted Hammer (in {current_trend} context)")
    if is_shooting_star(c3, trend_context=current_trend): patterns.append(f"Shooting Star (in {current_trend} context)")

    # Pola 3 Candle
    if len(candles) >= 3:
        if is_morning_star(c1, c2, c3): patterns.append("Morning Star")
        if is_evening_star(c1, c2, c3): patterns.append("Evening Star")
        if is_three_white_soldiers(c1, c2, c3): patterns.append("Three White Soldiers")
        if is_three_black_crows(c1, c2, c3): patterns.append("Three Black Crows")

    return patterns

# --- CHART PATTERN DETECTION ---
def get_significant_swing_points(candles: list, window: int = 30, threshold: float = 0.0075) -> tuple: # Window 30, Threshold 0.0075 (dari 20, 0.01/0.015)
    swing_highs = []
    swing_lows = []
    if len(candles) < window * 2 + 1: return [], []

    for i in range(window, len(candles) - window):
        current_high = candles[i]["high"]
        current_low = candles[i]["low"]
        is_swing_high = True
        for j in range(i - window, i + window + 1):
            if j != i and candles[j]["high"] > current_high: is_swing_high = False; break
        # Diperbarui: Melonggarkan kondisi konfirmasi swing point (harga berikutnya harus jatuh setelah puncak)
        if is_swing_high and (candles[i+window]["low"] < current_high * (1 - threshold)): # Memastikan ada penurunan setelah puncak
            swing_highs.append((i, current_high))

        is_swing_low = True
        for j in range(i - window, i + window + 1):
            if j != i and candles[j]["low"] < current_low: is_swing_low = False; break
        # Diperbarui: Melonggarkan kondisi konfirmasi swing point (harga berikutnya harus naik setelah lembah)
        if is_swing_low and (candles[i+window]["high"] > current_low * (1 + threshold)): # Memastikan ada kenaikan setelah lembah
            swing_lows.append((i, current_low))
            
    return swing_highs, swing_lows

def detect_double_top_bottom(candles: list) -> list:
    patterns = []
    # Diperbarui: Membutuhkan lebih sedikit candle untuk deteksi awal
    if len(candles) < 70: return patterns # Dari 100 menjadi 70
    
    # Diperbarui: Menggunakan window dan threshold yang lebih sensitif
    swing_highs, swing_lows = get_significant_swing_points(candles, window=20, threshold=0.01) # Sensitivitas lebih tinggi dari default

    if len(swing_highs) >= 2:
        h2_idx, h2_price = swing_highs[-1]
        h1_idx, h1_price = swing_highs[-2]
        # Diperbarui: Melonggarkan jarak antar puncak dan toleransi harga
        if h2_idx > h1_idx + 5 and abs(h1_price - h2_price) / h1_price < 0.015: # Dari 10 menjadi 5, dari 0.01 menjadi 0.015 (1.5%)
            valley_lows = [sl_price for sl_idx, sl_price in swing_lows if h1_idx < sl_idx < h2_idx]
            if valley_lows and candles[-1]["close"] < max(valley_lows) * 0.99: # Memastikan penembusan neckline (1%)
                patterns.append("Double Top")

    if len(swing_lows) >= 2:
        l2_idx, l2_price = swing_lows[-1]
        l1_idx, l1_price = swing_lows[-2]
        # Diperbarui: Melonggarkan jarak antar lembah dan toleransi harga
        if l2_idx > l1_idx + 5 and abs(l1_price - l2_price) / l1_price < 0.015: # Dari 10 menjadi 5, dari 0.01 menjadi 0.015 (1.5%)
            peak_highs = [sh_price for sh_idx, sh_price in swing_highs if l1_idx < sh_idx < l2_idx]
            if peak_highs and candles[-1]["close"] > min(peak_highs) * 1.01: # Memastikan penembusan neckline (1%)
                patterns.append("Double Bottom")
    return patterns

def detect_head_and_shoulders(candles: list) -> list:
    patterns = []
    # Diperbarui: Membutuhkan lebih sedikit candle untuk deteksi awal
    if len(candles) < 100: return patterns # Dari 150 menjadi 100
    
    # Diperbarui: Menggunakan window dan threshold yang lebih sensitif
    swing_highs, swing_lows = get_significant_swing_points(candles, window=20, threshold=0.01) # Sensitivitas lebih tinggi dari default

    # Head & Shoulders
    if len(swing_highs) >= 3 and len(swing_lows) >= 2:
        s3_idx, s3_price = swing_highs[-1] # Right Shoulder
        h_idx, h_price = swing_highs[-2]   # Head
        s1_idx, s1_price = swing_highs[-3] # Left Shoulder
        # Diperbarui: Memastikan urutan index dan melonggarkan toleransi shoulder
        if s1_idx < h_idx < s3_idx and h_price > s1_price * 1.01 and h_price > s3_price * 1.01 and \
           abs(s1_price - s3_price) / s1_price < 0.03: # Shoulder within 3% (dari 2%)
            valley1_price = max([sl_price for sl_idx, sl_price in swing_lows if s1_idx < sl_idx < h_idx] or [0])
            valley2_price = max([sl_price for sl_idx, sl_price in swing_lows if h_idx < sl_idx < s3_idx] or [0])
            if valley1_price > 0 and valley2_price > 0:
                neckline_level = (valley1_price + valley2_price) / 2
                # Diperbarui: Memastikan penembusan neckline yang lebih jelas
                if candles[-1]["close"] < neckline_level * 0.995: # Broke decisively (0.5%)
                    patterns.append("Head & Shoulders")

    # Inverse Head & Shoulders
    if len(swing_lows) >= 3 and len(swing_highs) >= 2:
        l3_idx, l3_price = swing_lows[-1] # Right Shoulder (low)
        h_idx, h_price = swing_lows[-2]   # Head (lowest)
        l1_idx, l1_price = swing_lows[-3] # Left Shoulder (low)
        # Diperbarui: Memastikan urutan index dan melonggarkan toleransi shoulder
        if l1_idx < h_idx < l3_idx and h_price < l1_price * 0.99 and h_price < l3_price * 0.99 and \
           abs(l1_price - l3_price) / l1_price < 0.03: # Shoulder within 3% (dari 2%)
            peak1_price = min([sh_price for sh_idx, sh_price in swing_highs if l1_idx < sh_idx < h_idx] or [float('inf')])
            peak2_price = min([sh_price for sh_idx, sh_price in swing_highs if h_idx < sh_idx < l3_idx] or [float('inf')])
            if peak1_price != float('inf') and peak2_price != float('inf'):
                neckline_level = (peak1_price + peak2_price) / 2
                # Diperbarui: Memastikan penembusan neckline yang lebih jelas
                if candles[-1]["close"] > neckline_level * 1.005: # Broke decisively (0.5%)
                    patterns.append("Inverse Head & Shoulders")
    return patterns

def detect_chart_patterns(candles: list) -> list:
    """Menggabungkan semua deteksi pola chart."""
    result = []
    result.extend(detect_double_top_bottom(candles))
    result.extend(detect_head_and_shoulders(candles))
    # Placeholder for more complex pattern detections that require robust trend line detection
    return result

# --- SMART MONEY CONCEPTS (SMC - Dasar) ---
def detect_bos_choch(candles: list) -> list:
    patterns = []
    # Diperbarui: Membutuhkan lebih sedikit candle untuk deteksi awal
    if len(candles) < 30: return patterns # Dari 50 menjadi 30
    
    # Diperbarui: Menggunakan window dan threshold yang lebih sensitif
    swing_highs, swing_lows = get_significant_swing_points(candles, window=10, threshold=0.002) # Window lebih kecil, threshold lebih sensitif

    if len(swing_highs) < 2 or len(swing_lows) < 2: return patterns

    last_high_idx, last_high_price = swing_highs[-1]
    prev_high_idx, prev_high_price = swing_highs[-2]
    
    last_low_idx, last_low_price = swing_lows[-1]
    prev_low_idx, prev_low_price = swing_lows[-2]

    current_close = candles[-1]["close"]
    # Perubahan untuk BOS/CHoCH, periksa penutupan di atas/bawah swing point sebelumnya
    # BOS (Break of Structure)
    if last_high_idx > prev_high_idx and last_low_idx > prev_low_idx: # Uptrend structure (HH, HL)
        # Check for new HH
        if current_close > last_high_price * 1.0005: # Minimal 0.05% break
            patterns.append("Bullish BOS (New HH)")
    elif last_high_idx < prev_high_idx and last_low_idx < prev_low_idx: # Downtrend structure (LH, LL)
        # Check for new LL
        if current_close < last_low_price * 0.9995: # Minimal 0.05% break
            patterns.append("Bearish BOS (New LL)")
    
    # CHoCH (Change of Character)
    if last_high_idx > prev_high_idx and last_low_idx > prev_low_idx: # Current structure is Uptrend
        # If current price breaks the last higher low
        if current_close < last_low_price * 0.9995: # Minimal 0.05% break
            patterns.append("Bearish CHoCH (Uptrend Reversal)")
    elif last_high_idx < prev_high_idx and last_low_idx < prev_low_idx: # Current structure is Downtrend
        # If current price breaks the last lower high
        if current_close > last_high_price * 1.0005: # Minimal 0.05% break
            patterns.append("Bullish CHoCH (Downtrend Reversal)")

    return patterns

def detect_auto_sr(candles: list, price_tolerance_percent: float = 0.001) -> dict: # Dari 0.002 menjadi 0.001 (lebih ketat untuk clustering)
    sr_levels = {"support": [], "resistance": []}
    # Diperbarui: Membutuhkan lebih sedikit candle untuk deteksi awal
    if len(candles) < 50: return sr_levels # Dari 100 menjadi 50

    # Diperbarui: Menggunakan window dan threshold yang lebih sensitif
    swing_highs, swing_lows = get_significant_swing_points(candles, window=15, threshold=0.003) # Window lebih kecil, threshold lebih sensitif

    all_swing_prices = [price for _, price in swing_highs + swing_lows]
    if not all_swing_prices: return sr_levels

    clustered_levels = {}
    for price in sorted(all_swing_prices):
        found_cluster = False
        for level_key in clustered_levels:
            if abs(price - level_key) / level_key < price_tolerance_percent:
                clustered_levels[level_key].append(price)
                found_cluster = True
                break
        if not found_cluster:
            clustered_levels[price] = [price]

    final_sr_levels = []
    # Diperbarui: Hanya mempertimbangkan level dengan setidaknya 2 sentuhan
    for level_key, prices in clustered_levels.items():
        if len(prices) >= 2: # Dari 3 menjadi 2
            final_sr_levels.append(sum(prices) / len(prices)) # Use average price for the level
    
    final_sr_levels.sort()

    current_price = candles[-1]["close"]
    for level in final_sr_levels:
        if level < current_price:
            sr_levels["support"].append(round(level, 2))
        else:
            sr_levels["resistance"].append(round(level, 2))
    
    sr_levels["support"].sort(reverse=True) # Highest support first
    sr_levels["resistance"].sort() # Lowest resistance first

    return sr_levels

def get_multi_tf_confirmations(current_tf_patterns: list, all_tf_data: dict, current_tf_name: str) -> list:
    confirmations = []
    current_trend_main = get_trend_direction(all_tf_data[current_tf_name])

    tf_order = {"1h": 1, "4h": 2, "1d": 3}
    
    for tf_name, tf_candles_list in all_tf_data.items():
        if tf_name == current_tf_name or not tf_candles_list or len(tf_candles_list) < 5: continue
        
        # Diperbarui: Selaraskan pemeriksaan tren dengan TF yang sama atau lebih tinggi
        if tf_order.get(tf_name, 0) >= tf_order.get(current_tf_name, 0): # Juga periksa TF yang sama
            higher_tf_trend = get_trend_direction(tf_candles_list)
            
            if higher_tf_trend == "Uptrend" and current_trend_main == "Uptrend":
                confirmations.append(f"Tren naik selaras di {tf_name.upper()}")
            elif higher_tf_trend == "Downtrend" and current_trend_main == "Downtrend":
                confirmations.append(f"Tren turun selaras di {tf_name.upper()}")
            
            is_bullish_pattern = any(p for p in current_tf_patterns if "Bullish" in p or "Hammer" in p or "Morning Star" in p or "Double Bottom" in p or "Inverse Head & Shoulders" in p)
            is_bearish_pattern = any(p for p in current_tf_patterns if "Bearish" in p or "Shooting Star" in p or "Evening Star" in p or "Double Top" in p or "Head & Shoulders" in p)

            if is_bullish_pattern and higher_tf_trend == "Uptrend":
                confirmations.append(f"Pola bullish di {current_tf_name.upper()} didukung tren Uptrend di {tf_name.upper()}")
            elif is_bearish_pattern and higher_tf_trend == "Downtrend":
                confirmations.append(f"Pola bearish di {current_tf_name.upper()} didukung tren Downtrend di {tf_name.upper()}")
            
    return confirmations

def get_live_candle_potential_and_process(candle: dict, time_progress: float, average_volume_prev_candles: float) -> str:
    """Menganalisis potensi awal dan proses perkembangan live candle dengan detail, termasuk volume."""
    if candle["is_final_bar"]:
        return "" # Hanya berlaku untuk live candle

    open_price = candle["open"]
    current_price = candle["close"]
    high_price = candle["high"]
    low_price = candle["low"]
    current_volume = candle["volume"]

    props = get_candle_properties(candle)

    potential_info = []
    process_info = []
    implication_info = [] # Untuk potensi pergerakan selanjutnya

    # 1. Posisi Harga Saat Ini Relatif Terhadap Harga Pembukaan
    if current_price > open_price:
        potential_info.append("Potensi Awal: **Bullish** 🟢 (harga saat ini di atas pembukaan).")
    elif current_price < open_price:
        potential_info.append("Potensi Awal: **Bearish** 🔴 (harga saat ini di bawah pembukaan).")
    else:
        potential_info.append("Potensi Awal: **Netral** ⚪ (harga saat ini di dekat pembukaan).")

    # 2. Analisis Body dan Shadows
    if props["is_bullish"]:
        process_info.append("Proses: Pembeli dominan mendorong harga naik.")
        if props["lower_shadow_to_range_ratio"] > 0.2:
            process_info.append(f"Ada penolakan kuat dari bawah (lower shadow {props['lower_shadow_to_range_ratio']:.1%} dari range).")
            implication_info.append("Implikasi: Menunjukkan tekanan beli kuat di harga rendah, potensi kelanjutan naik.")
        if props["upper_shadow_to_range_ratio"] > 0.2:
            process_info.append(f"Ada penolakan dari atas (upper shadow {props['upper_shadow_to_range_ratio']:.1%} dari range).")
            implication_info.append("Implikasi: Menunjukkan tekanan jual di harga tinggi, potensi pelemahan momentum naik.")
        if props["body_to_range_ratio"] > 0.6:
            implication_info.append("Implikasi: Momentum bullish kuat, berpotensi menjadi candle penentu tren.")

    elif props["is_bearish"]:
        process_info.append("Proses: Penjual dominan menekan harga turun.")
        if props["upper_shadow_to_range_ratio"] > 0.2:
            process_info.append(f"Ada penolakan kuat dari atas (upper shadow {props['upper_shadow_to_range_ratio']:.1%} dari range).")
            implication_info.append("Implikasi: Menunjukkan tekanan jual kuat di harga tinggi, potensi kelanjutan turun.")
        if props["lower_shadow_to_range_ratio"] > 0.2:
            process_info.append(f"Ada penolakan dari bawah (lower shadow {props['lower_shadow_to_range_ratio']:.1%} dari range).")
            implication_info.append("Implikasi: Menunjukkan tekanan beli di harga rendah, potensi pelemahan momentum turun.")
        if props["body_to_range_ratio"] > 0.6:
            implication_info.append("Implikasi: Momentum bearish kuat, berpotensi menjadi candle penentu tren.")

    else: # Doji-like atau body sangat kecil
        process_info.append("Proses: Harga bergerak bolak-balik, menunjukkan keraguan pasar.")
        if props["upper_shadow_to_range_ratio"] > 0.2 and props["lower_shadow_to_range_ratio"] > 0.2:
            implication_info.append("Implikasi: Ketidakpastian tinggi, pasar menunggu arah. Potensi konsolidasi atau pembalikan.")
        elif props["body_to_range_ratio"] < 0.15: # Disesuaikan dengan is_doji_like
            implication_info.append("Implikasi: Keraguan/konsolidasi. Pergerakan harga minimal, bisa jadi jeda sebelum pergerakan besar.")

    # 3. Analisis Progres Volume
    if average_volume_prev_candles > 0:
        expected_volume_at_this_progress = average_volume_prev_candles * time_progress
        if current_volume > expected_volume_at_this_progress * 1.2: # 20% di atas rata-rata yang diharapkan
            implication_info.append(f"Volume saat ini `{current_volume:.2f}`: **Tinggi** (lebih dari 20% dari volume rata-rata yang diharapkan). Menunjukkan minat besar.")
        elif current_volume < expected_volume_at_this_progress * 0.8: # 20% di bawah rata-rata yang diharapkan
            implication_info.append(f"Volume saat ini `{current_volume:.2f}`: **Rendah** (kurang dari 20% dari volume rata-rata yang diharapkan). Menunjukkan kurangnya minat.")
        else:
            implication_info.append(f"Volume saat ini `{current_volume:.2f}`: **Normal** (sesuai ekspektasi berdasarkan progres waktu).")
    else:
        implication_info.append(f"Volume saat ini: `{current_volume:.2f}` (tidak ada data volume rata-rata historis untuk perbandingan).")


    # 4. Progres Waktu Candle
    if time_progress < 0.3:
        implication_info.append("Catatan: Candle masih di awal periode, bentuknya sangat fluktuatif.")
    elif time_progress > 0.7:
        implication_info.append("Catatan: Candle mendekati penutupan, bentuknya lebih representatif.")

    final_message_parts = [
        "💡 **Analisis Live Candle**:",
        "   " + " ".join(potential_info),
        "   " + " ".join(process_info)
    ]
    if implication_info:
        final_message_parts.append("   " + "\n   ".join(implication_info)) # Pisahkan implikasi dengan newline untuk readability

    return "\n".join(final_message_parts)

def get_average_volume(candles: list, lookback_period: int = 20) -> float:
    """Menghitung rata-rata volume dari beberapa candle terakhir."""
    if len(candles) < lookback_period:
        return 0.0
    recent_volumes = [c["volume"] for c in candles[-lookback_period-1:-1]] # Ambil volume dari candle sebelumnya (tidak termasuk yang terakhir/live)
    if not recent_volumes: return 0.0
    return sum(recent_volumes) / len(recent_volumes)

# --- MAIN LOGIC FOR PERIODIC SCAN ---
def scan_all_intervals_and_notify(triggered_by_websocket_tf: str = None):
    """
    Memindai dan melaporkan pola untuk semua interval yang dikonfigurasi.
    Menggunakan data historis yang di-cache + live candle dari WebSocket.
    Mengirim satu notifikasi Discord yang menggabungkan semua informasi.
    
    Args:
        triggered_by_websocket_tf (str, optional): Timeframe yang memicu scan ini
            karena candle-nya baru saja closed. None jika dipicu oleh timer.
    """
    global _candle_data_cache, _live_websocket_candle_data, last_processed_final_candle_time

    full_alert_message_parts = []
    current_price = None # Akan diisi dari candle terbaru dari salah satu TF

    with data_cache_lock: # Pastikan akses ke cache aman
        for tf in INTERVALS:
            # Prioritaskan live candle dari WebSocket jika ada dan belum final
            latest_candle = _live_websocket_candle_data.get(tf)
            is_live_candle_being_processed = False

            if latest_candle and not latest_candle["is_final_bar"]:
                # Gunakan live candle untuk analisis
                current_candles_for_analysis = list(_candle_data_cache.get(tf, []))
                # Pastikan live candle ada di akhir list untuk analisis
                if not current_candles_for_analysis or latest_candle["time"] > current_candles_for_analysis[-1]["time"]:
                    current_candles_for_analysis.append(latest_candle)
                elif current_candles_for_analysis and latest_candle["time"] == current_candles_for_analysis[-1]["time"]:
                    current_candles_for_analysis[-1] = latest_candle # Update jika sudah ada tapi belum final
                
                is_live_candle_being_processed = True
            else:
                # Jika tidak ada live candle, atau live candle sudah final (misal baru saja close),
                # gunakan candle terakhir dari cache historis
                current_candles_for_analysis = list(_candle_data_cache.get(tf, []))
                if current_candles_for_analysis:
                    latest_candle = current_candles_for_analysis[-1]
                else:
                    latest_candle = None # Tidak ada data sama sekali

            # Cek apakah notifikasi untuk candle final TF ini sudah dikirim dalam sesi ini
            if not is_live_candle_being_processed and latest_candle and \
               latest_candle["is_final_bar"] and \
               last_processed_final_candle_time[tf] == latest_candle["close_time"] and \
               tf != triggered_by_websocket_tf: # Jangan skip jika ini TF yang baru closed dan trigger scan
                # Logger.info(f"Skipping {tf} analysis. Final candle already processed or not the trigger.")
                continue # Skip jika sudah diproses, kecuali jika ini TF yang baru memicu notifikasi

            if not latest_candle:
                full_alert_message_parts.append(f"\n---\n📊 **{tf.upper()} Timeframe**: Tidak ada data candle tersedia.")
                continue
                
            time_progress_for_live_candle = 0.0
            average_volume_past = get_average_volume(current_candles_for_analysis)

            if is_live_candle_being_processed: # Hanya hitung progress jika memang ini live candle
                time_progress_for_live_candle = get_time_progress(latest_candle["time"], tf)
            
            # Menyesuaikan kebutuhan minimum candle berdasarkan jenis analisis
            min_candles_needed = 3 # Default untuk PA dasar (1-3 candle)
            if tf == "1d" and CANDLE_LIMIT[tf] >= 300: # Untuk deteksi chart pattern 1D yang butuh banyak data
                min_candles_needed = 100 
            elif tf in ["1h", "4h"] and CANDLE_LIMIT[tf] >= 150: # Untuk deteksi SMC/SR yang butuh puluhan candle
                min_candles_needed = 50

            if len(current_candles_for_analysis) < min_candles_needed:
                full_alert_message_parts.append(f"\n---\n📊 **{tf.upper()} Timeframe**: Tidak cukup data untuk analisis lengkap. ({len(current_candles_for_analysis)}/{min_candles_needed} candles dibutuhkan)")
                continue

            # latest_candle sudah ditentukan di atas
            if current_price is None:
                current_price = latest_candle['close']

            pa_patterns = detect_price_action(current_candles_for_analysis, enable_volume_confirmation=False)
            cp_patterns = detect_chart_patterns(current_candles_for_analysis)
            bos_choch_patterns = detect_bos_choch(current_candles_for_analysis)
            sr_levels = detect_auto_sr(current_candles_for_analysis)

            # Temp all tf data for multi-tf (pastikan ini juga pakai live candle jika tersedia)
            temp_all_tf_data_for_multi_tf = {}
            for _tf_check in INTERVALS:
                _temp_live_c = _live_websocket_candle_data.get(_tf_check)
                if _temp_live_c and not _temp_live_c["is_final_bar"]:
                    _temp_candles = list(_candle_data_cache.get(_tf_check, []))
                    if not _temp_candles or _temp_live_c["time"] > _temp_candles[-1]["time"]:
                        _temp_candles.append(_temp_live_c)
                    elif _temp_candles and _temp_live_c["time"] == _temp_candles[-1]["time"]:
                        _temp_candles[-1] = _temp_live_c # Update live candle in temp list
                    temp_all_tf_data_for_multi_tf[_tf_check] = _temp_candles
                else:
                    temp_all_tf_data_for_multi_tf[_tf_check] = list(_candle_data_cache.get(_tf_check, [])) # Gunakan cache jika tidak ada live/sudah final


            multi_tf_confirmations = get_multi_tf_confirmations(pa_patterns + cp_patterns + bos_choch_patterns, temp_all_tf_data_for_multi_tf, tf)

            props = get_candle_properties(latest_candle)
            candle_type = "Bullish" if props["is_bullish"] else "Bearish" if props["is_bearish"] else "Doji-like"
            full_range_percent = (props["full_range"] / latest_candle["open"]) * 100 if latest_candle["open"] else 0
            
            status_candle_tag = "[L]" if is_live_candle_being_processed else "[C]" # Ditentukan oleh is_live_candle_being_processed
            status_candle_desc = "LIVE (Open)" if is_live_candle_being_processed else "FINAL (Closed)" # Ditentukan oleh is_live_candle_being_processed
            time_progress_info = ""
            live_candle_potential_info_msg = ""

            # Detail candle (akan selalu ditampilkan untuk final/live)
            candle_details = (
                f"   • Open: `{latest_candle['open']:.2f}`\n"
                f"   • High: `{latest_candle['high']:.2f}`\n"
                f"   • Low: `{latest_candle['low']:.2f}`\n"
                f"   • Close: `{latest_candle['close']:.2f}`\n"
                f"   • Volume: `{latest_candle['volume']:.2f}`\n"
                f"   • Body/Range Ratio: `{props['body_to_range_ratio']:.2f}`\n"
                f"   • Upper Shadow/Range Ratio: `{props['upper_shadow_to_range_ratio']:.2f}`\n"
                f"   • Lower Shadow/Range Ratio: `{props['lower_shadow_to_range_ratio']:.2f}`\n"
            )

            if is_live_candle_being_processed: # Hanya tambahkan info progress jika ini live candle
                time_progress_info = f" ({int(time_progress_for_live_candle*100)}% progress)"
                live_candle_potential_info_msg = get_live_candle_potential_and_process(latest_candle, time_progress_for_live_candle, average_volume_past)
                
            tf_msg_part = f"\n---\n📊 **{tf.upper()} Timeframe** {status_candle_tag} ({status_candle_desc}{time_progress_info})\n" \
                        f"**Waktu Analisis**: {datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"
            
            if status_candle_tag == "[C]": # Jika itu candle yang sudah tertutup
                tf_msg_part += f"**Waktu Penutupan Candle**: {latest_candle['close_time'].astimezone(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"
            else: # Jika itu candle live
                tf_msg_part += f"Waktu Pembukaan Candle: {latest_candle['time'].astimezone(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"


            tf_msg_part += f"Tipe Candle: {candle_type} (Range: {full_range_percent:.2f}%)\n" \
                        f"**Detail Candle (O/H/L/C)**:\n{candle_details}" + \
                        (live_candle_potential_info_msg + "\n" if live_candle_potential_info_msg else "") # Tambah newline jika ada info live

            detected_info_present = False
            
            if pa_patterns: 
                tf_msg_part += "🕯️ **Pola Candlestick**: " + ", ".join(pa_patterns) + "\n"
                detected_info_present = True
            
            if cp_patterns:
                cp_descriptions = []
                for pattern in cp_patterns:
                    if "Double Top" in pattern:
                        cp_descriptions.append(f"**Double Top**: Menunjukkan potensi pembalikan tren dari naik menjadi turun.")
                    elif "Double Bottom" in pattern:
                        cp_descriptions.append(f"**Double Bottom**: Menunjukkan potensi pembalikan tren dari turun menjadi naik.")
                    elif "Head & Shoulders" in pattern:
                        cp_descriptions.append(f"**Head & Shoulders**: Menunjukkan potensi pembalikan tren dari naik menjadi turun.")
                    elif "Inverse Head & Shoulders" in pattern:
                        cp_descriptions.append(f"**Inverse Head & Shoulders**: Menunjukkan potensi pembalikan tren dari turun menjadi naik.")
                    else:
                        cp_descriptions.append(pattern)
                tf_msg_part += "📈 **Pola Chart**: " + "\n" + "\n".join([f"- {desc}" for desc in cp_descriptions]) + "\n"
                detected_info_present = True

            if bos_choch_patterns: 
                tf_msg_part += "🔄 **Struktur Pasar (SMC)**: " + ", ".join(bos_choch_patterns) + "\n"
                detected_info_present = True
            
            sr_info_part = ""
            if sr_levels.get("support"): 
                sr_info_part += "⬇️ **Support Levels**: " + ", ".join([f"{v:.2f}" for v in sr_levels["support"]]) + "\n"
                detected_info_present = True
                for s_level in sr_levels["support"]:
                    if abs(current_price - s_level) / current_price < 0.005: # Dalam 0.5% dari S/R level
                        sr_info_part += f"⚠️ Harga saat ini ({current_price:.2f}) mendekati Support Level {s_level:.2f}\n"

            if sr_levels.get("resistance"): 
                sr_info_part += "⬆️ **Resistance Levels**: " + ", ".join([f"{v:.2f}" for v in sr_levels["resistance"]]) + "\n"
                detected_info_present = True
                for r_level in sr_levels["resistance"]:
                    if abs(current_price - r_level) / current_price < 0.005: # Dalam 0.5% dari S/R level
                        sr_info_part += f"⚠️ Harga saat ini ({current_price:.2f}) mendekati Resistance Level {r_level:.2f}\n"
            tf_msg_part += sr_info_part

            if multi_tf_confirmations: 
                tf_msg_part += "✅ **Konfirmasi Multi-TF**: " + ", ".join(multi_tf_confirmations) + "\n"
                detected_info_present = True
            
            # Diperbarui: Hanya tampilkan ini jika TIDAK ADA informasi yang terdeteksi sama sekali ATAU tidak ada info live
            if not detected_info_present and not live_candle_potential_info_msg: 
                tf_msg_part += "Tidak ada pola atau informasi signifikan terdeteksi saat ini.\n"

            full_alert_message_parts.append(tf_msg_part)

            # Tandai candle ini sudah diproses jika ini adalah candle final yang memicu notifikasi
            if not is_live_candle_being_processed and latest_candle["is_final_bar"] and tf == triggered_by_websocket_tf:
                last_processed_final_candle_time[tf] = latest_candle["close_time"]

    if full_alert_message_parts:
        header_price = f"**Harga Terakhir {SYMBOL}**: {current_price:.2f}\n" if current_price else ""
        final_message = header_price + "\n".join(full_alert_message_parts)
        send_discord_alert(f"📡 {SYMBOL} Real-time Market Update", final_message)
    else:
        logger.info("Tidak ada update yang signifikan untuk dikirim dalam ringkasan real-time.")


# --- DISCORD ALERT ---
def send_discord_alert(title: str, message: str):
    """Mengirim notifikasi ke Discord via webhook."""
    if DISCORD_WEBHOOK == "https://discord.com/api/webhooks/1392182015884787832/OwTMcZHCnm7mB16c7ebATXzgNWe7QmiXtmKPBvVu7YpdRdzAXIHhqSqp8ou9moKg64Tm": # Masih pakai placeholder lama, bisa jadi lupa diganti
        logger.error("DISCORD_WEBHOOK belum diatur. Tidak dapat mengirim notifikasi. Harap ganti placeholder URL.")
        return

    payload = {
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": 3447003, # Green for general alerts
                "timestamp": datetime.datetime.utcnow().isoformat() # Discord timestamp is always UTC
            }
        ]
    }
    try:
        requests.post(DISCORD_WEBHOOK, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=5)
        logger.info(f"Alert Discord terkirim: {title}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending Discord alert: {e}")

# --- WEBSOCKET HANDLERS ---
def on_open(ws):
    logger.info("Connected to Binance WebSocket.")
    streams = [f"{SYMBOL.lower()}@kline_{interval}" for interval in INTERVALS]
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    logger.info(f"Subscribed to streams: {', '.join(streams)}")

def on_message(ws, message):
    global _live_websocket_candle_data, _candle_data_cache, last_processed_final_candle_time

    data = json.loads(message)
    if "k" in data: # Klines event
        kline_data = data["k"]
        interval = kline_data["i"]
        
        current_candle = {
            "time": datetime.datetime.fromtimestamp(kline_data["t"] / 1000, tz=pytz.utc), # Waktu pembukaan (UTC)
            "open": float(kline_data["o"]),
            "high": float(kline_data["h"]),
            "low": float(kline_data["l"]),
            "close": float(kline_data["c"]),
            "volume": float(kline_data["v"]),
            "close_time": datetime.datetime.fromtimestamp(kline_data["T"] / 1000, tz=pytz.utc), # Waktu penutupan yg diharapkan (UTC)
            "is_final_bar": kline_data["x"] # 'x' indicates if this candle is closed
        }
        
        with data_cache_lock: # Amankan akses ke cache saat update
            _live_websocket_candle_data[interval] = current_candle

            if current_candle["is_final_bar"]:
                # Periksa apakah candle ini sudah pernah diproses sebagai final
                if last_processed_final_candle_time[interval] is None or \
                current_candle["close_time"] > last_processed_final_candle_time[interval]:
                    
                    logger.info(f"[{interval.upper()}] Candle DITUTUP (FINAL). Memperbarui data historis dari REST API dan memicu scan.")
                    # Perbarui cache historis dengan data REST API untuk keakuratan
                    _candle_data_cache[interval] = get_klines_rest(SYMBOL, interval, CANDLE_LIMIT[interval])
                    # PENTING: Panggil scan_all_intervals_and_notify dengan parameter agar tahu TF mana yang trigger
                    threading.Thread(target=scan_all_intervals_and_notify, args=(interval,)).start()
                else:
                    logger.debug(f"[{interval.upper()}] Candle FINAL diterima tetapi sudah diproses sebelumnya.")
            else:
                logger.debug(f"[{interval.upper()}] Candle LIVE diperbarui. Harga: {current_candle['close']:.2f}, Volume: {current_candle['volume']:.2f}")

    elif "result" in data:
        logger.info(f"WebSocket result: {data}")
    elif "error" in data:
        logger.error(f"WebSocket error: {data}")

def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}. Reconnecting in 5 seconds...")
    time.sleep(5)
    run_websocket_client()

def run_websocket_client():
    """Menjalankan klien WebSocket Binance."""
    ws_app = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # Ping interval lebih rendah untuk memastikan koneksi tetap hidup
    ws_app.run_forever(ping_interval=20, ping_timeout=10)

# --- RUNNER ---
if __name__ == "__main__":
    # Penting: Periksa placeholder webhook
    if DISCORD_WEBHOOK == "https://discord.com/api/webhooks/1392182015884787832/OwTMcZHCnm7mB16c7ebATXzgNWe7QmiXtmKPBvVu7YpdRdzAXIHhqSqp8ou9moKg64Tm":
        logger.error("ERROR: Variabel DISCORD_WEBHOOK belum diatur. Harap ganti placeholder dengan URL webhook Anda di awal skrip.")
        exit(1)
        
    logger.info("Bot pemantau harga dimulai. Mengambil data historis awal...")
    
    with data_cache_lock: # Kunci saat inisialisasi cache
        for tf in INTERVALS:
            try:
                initial_candles = get_klines_rest(SYMBOL, tf, CANDLE_LIMIT[tf])
                if initial_candles:
                    _candle_data_cache[tf] = initial_candles
                    last_processed_final_candle_time[tf] = initial_candles[-1]["close_time"]
                    logger.info(f"Data historis {tf.upper()} berhasil dimuat. {len(initial_candles)} candle.")
                else:
                    logger.error(f"Gagal memuat data historis untuk {tf}. Bot mungkin tidak berfungsi dengan baik.")
                    exit(1)
            except Exception as e:
                logger.error(f"Kesalahan fatal saat memuat data awal untuk {tf}: {e}. Memastikan ada koneksi internet.")
                exit(1)

    logger.info("Memulai koneksi WebSocket untuk pembaruan real-time...")
    
    ws_thread = threading.Thread(target=run_websocket_client)
    ws_thread.daemon = True # Daemon thread akan berhenti ketika program utama berhenti
    ws_thread.start()

    logger.info("Memberi sedikit waktu untuk WebSocket terhubung dan menerima data live pertama.")
    time.sleep(5)

    logger.info("Memicu analisis awal dan mengirim notifikasi Discord pertama.")
    scan_all_intervals_and_notify() # Panggil tanpa trigger_tf untuk inisialisasi

    logger.info(f"Memulai loop analisis cadangan dan live candle setiap {REAL_TIME_SCAN_INTERVAL_SECONDS / 60} menit.")
    while True:
        time.sleep(REAL_TIME_SCAN_INTERVAL_SECONDS)
        logger.info(f"Memicu analisis live candle dan cadangan pada interval {REAL_TIME_SCAN_INTERVAL_SECONDS / 60} menit.")
        scan_all_intervals_and_notify() # Panggil tanpa trigger_tf untuk scan berkala
