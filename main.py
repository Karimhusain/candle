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
    "1h": 250,
    "4h": 200,
    "1d": 200 # Disesuaikan agar cukup data untuk analisis 1D
}
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1392182015884787832/OwTMcZHCnm7mB16c7ebATXzgNWe7QmiXtmKPBvVu7YpdRdzAXIHhqSqp8ou9moKg64Tm" # <<< PENTING: GANTI DENGAN URL WEBHOOK DISCORD ANDA

# Frekuensi analisis real-time (dalam detik)
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
        "is_doji_like": body_abs / full_range < 0.1, # Jika body sangat kecil
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
def is_bullish_engulfing(c1, c2, confirm_volume=False):
    if not (c1["close"] < c1["open"] and c2["close"] > c2["open"]): return False
    engulfs = c2["close"] > c1["open"] and c2["open"] < c1["close"] and \
              (c2["close"] - c2["open"]) > (c1["open"] - c1["close"])
    if confirm_volume: return engulfs and c2["volume"] > c1["volume"]
    return engulfs

def is_bearish_engulfing(c1, c2, confirm_volume=False):
    if not (c1["close"] > c1["open"] and c2["close"] < c2["open"]): return False
    engulfs = c2["close"] < c1["open"] and c2["open"] > c1["close"] and \
              (c2["open"] - c2["close"]) > (c1["close"] - c1["open"])
    if confirm_volume: return engulfs and c2["volume"] > c1["volume"]
    return engulfs

def is_pin_bar(c):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.35: return False
    return (props["is_bullish"] and props["lower_shadow"] >= 2 * props["body_abs"] and props["upper_shadow"] < 0.2 * props["full_range"]) or \
           (props["is_bearish"] and props["upper_shadow"] >= 2 * props["body_abs"] and props["lower_shadow"] < 0.2 * props["full_range"])

def is_doji(c):
    props = get_candle_properties(c)
    return props["is_doji_like"]

def is_hammer(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.35: return False
    is_hammer_shape = props["lower_shadow"] >= 2 * props["body_abs"] and props["upper_shadow"] < 0.1 * props["full_range"]
    return is_hammer_shape if trend_context == "Downtrend" else is_hammer_shape

def is_hanging_man(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.35: return False
    is_hanging_man_shape = props["lower_shadow"] >= 2 * props["body_abs"] and props["upper_shadow"] < 0.1 * props["full_range"]
    return is_hanging_man_shape if trend_context == "Uptrend" else is_hanging_man_shape

def is_inverted_hammer(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.35: return False
    is_inverted_hammer_shape = props["upper_shadow"] >= 2 * props["body_abs"] and props["lower_shadow"] < 0.1 * props["full_range"]
    return is_inverted_hammer_shape if trend_context == "Downtrend" else is_inverted_hammer_shape

def is_shooting_star(c, trend_context="None"):
    props = get_candle_properties(c)
    if props["full_range"] == 0 or props["body_to_range_ratio"] > 0.35: return False
    is_shooting_star_shape = props["upper_shadow"] >= 2 * props["body_abs"] and props["lower_shadow"] < 0.1 * props["full_range"]
    return is_shooting_star_shape if trend_context == "Uptrend" else is_shooting_star_shape

def is_morning_star(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bearish"] and props3["is_bullish"] and props1["body_to_range_ratio"] > 0.5 and props3["body_to_range_ratio"] > 0.5): return False
    if not (props2["is_doji_like"] or props2["body_to_range_ratio"] < 0.3): return False
    return c3["close"] > (c1["open"] + c1["close"]) / 2

def is_evening_star(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bullish"] and props3["is_bearish"] and props1["body_to_range_ratio"] > 0.5 and props3["body_to_range_ratio"] > 0.5): return False
    if not (props2["is_doji_like"] or props2["body_to_range_ratio"] < 0.3): return False
    return c3["close"] < (c1["open"] + c1["close"]) / 2

def is_inside_bar(c1, c2):
    return c2["high"] < c1["high"] and c2["low"] > c1["low"]

def is_outside_bar(c1, c2):
    return c2["high"] > c1["high"] and c2["low"] < c1["low"]

def is_tweezer_top(c1, c2):
    if not (abs(c1["high"] - c2["high"]) / c1["high"] < 0.0005): return False
    return c1["close"] > c1["open"] and c2["close"] < c2["open"]

def is_tweezer_bottom(c1, c2):
    if not (abs(c1["low"] - c2["low"]) / c1["low"] < 0.0005): return False
    return c1["close"] < c1["open"] and c2["close"] > c2["open"]

def is_three_white_soldiers(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bullish"] and props2["is_bullish"] and props3["is_bullish"] and \
            props1["body_to_range_ratio"] > 0.6 and props2["body_to_range_ratio"] > 0.6 and props3["body_to_range_ratio"] > 0.6): return False
    if not (c2["close"] > c1["close"] and c3["close"] > c2["close"]): return False
    if not (c2["open"] > c1["open"] and c2["open"] < c1["close"]): return False
    if not (c3["open"] > c2["open"] and c3["open"] < c2["close"]): return False
    return True

def is_three_black_crows(c1, c2, c3):
    props1, props2, props3 = get_candle_properties(c1), get_candle_properties(c2), get_candle_properties(c3)
    if not (props1["is_bearish"] and props2["is_bearish"] and props3["is_bearish"] and \
            props1["body_to_range_ratio"] > 0.6 and props2["body_to_range_ratio"] > 0.6 and props3["body_to_range_ratio"] > 0.6): return False
    if not (c2["close"] < c1["close"] and c3["close"] < c2["close"]): return False
    if not (c2["open"] < c1["open"] and c2["open"] > c1["close"]): return False
    if not (c3["open"] < c2["open"] and c3["open"] > c2["close"]): return False
    return True

def is_dark_cloud_cover(c1, c2):
    if not (c1["close"] > c1["open"] and c2["close"] < c2["open"]): return False
    if not (c2["open"] > c1["high"]): return False
    if not (c2["close"] < (c1["open"] + c1["close"]) / 2 and c2["close"] > c1["open"]): return False
    return True

def is_piercing_pattern(c1, c2):
    if not (c1["close"] < c1["open"] and c2["close"] > c2["open"]): return False
    if not (c2["open"] < c1["low"]): return False
    if not (c2["close"] > (c1["open"] + c1["close"]) / 2 and c2["close"] < c1["open"]): return False
    return True

def is_bullish_harami(c1, c2):
    props1, props2 = get_candle_properties(c1), get_candle_properties(c2)
    if not (props1["is_bearish"] and props2["is_bullish"] and props1["body_to_range_ratio"] > 0.6 and props2["body_to_range_ratio"] < 0.4): return False
    return c2["open"] > c1["close"] and c2["close"] < c1["open"]

def is_bearish_harami(c1, c2):
    props1, props2 = get_candle_properties(c1), get_candle_properties(c2)
    if not (props1["is_bullish"] and props2["is_bearish"] and props1["body_to_range_ratio"] > 0.6 and props2["body_to_range_ratio"] < 0.4): return False
    return c2["open"] < c1["close"] and c2["close"] > c1["open"]

def detect_price_action(candles: list, enable_volume_confirmation: bool = True) -> list:
    patterns = []
    if len(candles) < 3: return patterns
    
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    current_trend = get_trend_direction(candles[:-1])

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

    if is_pin_bar(c3): patterns.append("Pin Bar")
    if is_doji(c3): patterns.append("Doji")
    if is_hammer(c3, trend_context=current_trend): patterns.append(f"Hammer (in {current_trend} context)")
    if is_hanging_man(c3, trend_context=current_trend): patterns.append(f"Hanging Man (in {current_trend} context)")
    if is_inverted_hammer(c3, trend_context=current_trend): patterns.append(f"Inverted Hammer (in {current_trend} context)")
    if is_shooting_star(c3, trend_context=current_trend): patterns.append(f"Shooting Star (in {current_trend} context)")

    if len(candles) >= 3: # Pola 3 candle
        if is_morning_star(c1, c2, c3): patterns.append("Morning Star")
        if is_evening_star(c1, c2, c3): patterns.append("Evening Star")
        if is_three_white_soldiers(c1, c2, c3): patterns.append("Three White Soldiers")
        if is_three_black_crows(c1, c2, c3): patterns.append("Three Black Crows")

    return patterns

# --- CHART PATTERN DETECTION ---
def get_significant_swing_points(candles: list, window: int = 20, threshold: float = 0.01) -> tuple:
    swing_highs = []
    swing_lows = []
    if len(candles) < window * 2 + 1: return [], []

    for i in range(window, len(candles) - window):
        current_high = candles[i]["high"]
        current_low = candles[i]["low"]
        is_swing_high = True
        for j in range(i - window, i + window + 1):
            if j != i and candles[j]["high"] > current_high: is_swing_high = False; break
        if is_swing_high and (candles[i+window]["low"] < current_high * (1 - threshold)): swing_highs.append((i, current_high))

        is_swing_low = True
        for j in range(i - window, i + window + 1):
            if j != i and candles[j]["low"] < current_low: is_swing_low = False; break
        if is_swing_low and (candles[i+window]["high"] > current_low * (1 + threshold)): swing_lows.append((i, current_low))
            
    return swing_highs, swing_lows

def detect_double_top_bottom(candles: list) -> list:
    patterns = []
    if len(candles) < 100: return patterns
    swing_highs, swing_lows = get_significant_swing_points(candles, window=25, threshold=0.015)

    if len(swing_highs) >= 2:
        h2_idx, h2_price = swing_highs[-1]
        h1_idx, h1_price = swing_highs[-2]
        if h2_idx > h1_idx + 10 and abs(h1_price - h2_price) / h1_price < 0.01:
            valley_lows = [sl_price for sl_idx, sl_price in swing_lows if h1_idx < sl_idx < h2_idx]
            if valley_lows and candles[-1]["close"] < max(valley_lows) * 0.995:
                patterns.append("Double Top") # Hanya nama pola, deskripsi akan ditambahkan saat output

    if len(swing_lows) >= 2:
        l2_idx, l2_price = swing_lows[-1]
        l1_idx, l1_price = swing_lows[-2]
        if l2_idx > l1_idx + 10 and abs(l1_price - l2_price) / l1_price < 0.01:
            peak_highs = [sh_price for sh_idx, sh_price in swing_highs if l1_idx < sh_idx < l2_idx]
            if peak_highs and candles[-1]["close"] > min(peak_highs) * 1.005:
                patterns.append("Double Bottom") # Hanya nama pola, deskripsi akan ditambahkan saat output
    return patterns

def detect_head_and_shoulders(candles: list) -> list:
    patterns = []
    if len(candles) < 150: return patterns
    swing_highs, swing_lows = get_significant_swing_points(candles, window=25, threshold=0.01)

    # Simplified Head & Shoulders
    if len(swing_highs) >= 3 and len(swing_lows) >= 2:
        s3_idx, s3_price = swing_highs[-1] # Right Shoulder
        h_idx, h_price = swing_highs[-2]   # Head
        s1_idx, s1_price = swing_highs[-3] # Left Shoulder
        if s1_idx < h_idx < s3_idx and h_price > s1_price and h_price > s3_price and \
           abs(s1_price - s3_price) / s1_price < 0.02: # Shoulders within 2%
            valley1_price = max([sl_price for sl_idx, sl_price in swing_lows if s1_idx < sl_idx < h_idx] or [0])
            valley2_price = max([sl_price for sl_idx, sl_price in swing_lows if h_idx < sl_idx < s3_idx] or [0])
            if valley1_price > 0 and valley2_price > 0:
                neckline_level = (valley1_price + valley2_price) / 2
                if candles[-1]["close"] < neckline_level * 0.99 and candles[-2]["close"] > neckline_level: # Broke decisively
                    patterns.append("Head & Shoulders") # Hanya nama pola, deskripsi akan ditambahkan saat output

    # Simplified Inverse Head & Shoulders
    if len(swing_lows) >= 3 and len(swing_highs) >= 2:
        l3_idx, l3_price = swing_lows[-1] # Right Shoulder (low)
        h_idx, h_price = swing_lows[-2]   # Head (lowest)
        l1_idx, l1_price = swing_lows[-3] # Left Shoulder (low)
        if l1_idx < h_idx < l3_idx and h_price < l1_price and h_price < l3_price and \
           abs(l1_price - l3_price) / l1_price < 0.02: # Shoulders within 2%
            peak1_price = min([sh_price for sh_idx, sh_price in swing_highs if l1_idx < sh_idx < h_idx] or [float('inf')])
            peak2_price = min([sh_price for sh_idx, sh_price in swing_highs if h_idx < sh_idx < l3_idx] or [float('inf')])
            if peak1_price != float('inf') and peak2_price != float('inf'):
                neckline_level = (peak1_price + peak2_price) / 2
                if candles[-1]["close"] > neckline_level * 1.01 and candles[-2]["close"] < neckline_level:
                    patterns.append("Inverse Head & Shoulders") # Hanya nama pola, deskripsi akan ditambahkan saat output
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
    if len(candles) < 50: return patterns
    swing_highs, swing_lows = get_significant_swing_points(candles, window=15, threshold=0.005)

    if len(swing_highs) < 2 or len(swing_lows) < 2: return patterns

    last_high_idx, last_high_price = swing_highs[-1]
    prev_high_idx, prev_high_price = swing_highs[-2]
    
    last_low_idx, last_low_price = swing_lows[-1]
    prev_low_idx, prev_low_price = swing_lows[-2]

    current_close = candles[-1]["close"]
    prev_close = candles[-2]["close"]

    is_uptrend_structure = last_high_price > prev_high_price and last_low_price > prev_low_price
    is_downtrend_structure = last_high_price < prev_high_price and last_low_price < prev_low_price

    if is_uptrend_structure and current_close > last_high_price * 1.001 and prev_close <= last_high_price:
        patterns.append("Bullish BOS (New HH)")
    elif is_downtrend_structure and current_close < last_low_price * 0.999 and prev_close >= last_low_price:
        patterns.append("Bearish BOS (New LL)")
    
    if is_uptrend_structure and current_close < last_low_price * 0.999 and prev_close >= last_low_price:
        patterns.append("Bearish CHoCH (Uptrend Reversal)")
    elif is_downtrend_structure and current_close > last_high_price * 1.001 and prev_close <= last_high_price:
        patterns.append("Bullish CHoCH (Downtrend Reversal)")

    return patterns

def detect_auto_sr(candles: list, price_tolerance_percent: float = 0.002) -> dict:
    sr_levels = {"support": [], "resistance": []}
    if len(candles) < 100: return sr_levels

    swing_highs, swing_lows = get_significant_swing_points(candles, window=20, threshold=0.005)

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
    for level_key, prices in clustered_levels.items():
        if len(prices) >= 3: # Only consider levels with at least 3 touches
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
        
        if tf_order.get(tf_name, 0) > tf_order.get(current_tf_name, 0): # Hanya periksa TF yang lebih tinggi
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

def get_live_candle_potential_and_process(candle: dict) -> str:
    """Menganalisis potensi awal dan proses perkembangan live candle dengan detail."""
    if candle["is_final_bar"]:
        return "" # Hanya berlaku untuk live candle

    open_price = candle["open"]
    current_price = candle["close"]
    high_price = candle["high"]
    low_price = candle["low"]

    props = get_candle_properties(candle)

    potential_info = ""
    process_info = ""
    additional_info = [] # Untuk Perhatian dan Potensi Lanjut/Kondisi

    # Potensi Awal (berdasarkan posisi close/current price relatif terhadap open)
    if current_price > open_price:
        potential_info = "Potensi Awal: **Bullish** 🟢 (harga saat ini di atas pembukaan)."
    elif current_price < open_price:
        potential_info = "Potensi Awal: **Bearish** 🔴 (harga saat ini di bawah pembukaan)."
    else:
        potential_info = "Potensi Awal: **Netral** ⚪ (harga saat ini di dekat pembukaan)."

    # Proses Perkembangan dan Potensi Lanjut / Rejection
    if props["is_bullish"]: # Candle sedang bullish (current_price > open_price)
        if props["lower_shadow_to_range_ratio"] < 0.1: # Ekor bawah kecil
            process_info = "Proses: Pembeli dominan mendorong harga naik dengan sedikit perlawanan awal."
        elif props["lower_shadow_to_range_ratio"] > 0.2: # Ekor bawah signifikan
            process_info = "Proses: Harga sempat turun namun didorong kuat naik oleh pembeli."
            additional_info.append("Potensi tekanan beli kuat dari bawah (rejection dari low).")
        else:
            process_info = "Proses: Harga bergerak naik dari level terendah."
        
        # Potensi Lanjut Bullish (strong body, small upper shadow)
        if props["body_to_range_ratio"] > 0.6 and props["upper_shadow_to_range_ratio"] < 0.2:
            additional_info.append("Potensi Lanjut: **Momentum Bullish kuat** berlanjut.")

    elif props["is_bearish"]: # Candle sedang bearish (current_price < open_price)
        if props["upper_shadow_to_range_ratio"] < 0.1: # Ekor atas kecil
            process_info = "Proses: Penjual dominan menekan harga turun dengan sedikit perlawanan awal."
        elif props["upper_shadow_to_range_ratio"] > 0.2: # Ekor atas signifikan
            process_info = "Proses: Harga sempat naik namun ditekan kuat turun oleh penjual."
            additional_info.append("Potensi tekanan jual kuat dari atas (rejection dari high).")
        else:
            process_info = "Proses: Harga bergerak turun dari level tertinggi."

        # Potensi Lanjut Bearish (strong body, small lower shadow)
        if props["body_to_range_ratio"] > 0.6 and props["lower_shadow_to_range_ratio"] < 0.2:
            additional_info.append("Potensi Lanjut: **Momentum Bearish kuat** berlanjut.")

    else: # Doji-like atau body sangat kecil (current_price sangat dekat open_price)
        process_info = "Proses: Harga bergerak bolak-balik, menunjukkan keraguan pasar."
        if props["upper_shadow_to_range_ratio"] > 0.2 and props["lower_shadow_to_range_ratio"] > 0.2:
            additional_info.append("Kondisi: **Sideways/Indecision** (tekanan beli dan jual seimbang).")
        elif props["body_to_range_ratio"] < 0.1:
            additional_info.append("Kondisi: **Keraguan/Konsolidasi** (pergerakan harga minimal).")

    final_message_parts = [potential_info, process_info]
    if additional_info:
        final_message_parts.append("Perhatian: " + " ".join(additional_info))

    return "💡 **Analisis Live Candle**: " + " ".join(final_message_parts)


# --- MAIN LOGIC FOR PERIODIC SCAN ---
def scan_all_intervals_and_notify():
    """
    Memindai dan melaporkan pola untuk semua interval yang dikonfigurasi.
    Menggunakan data historis yang di-cache + live candle dari WebSocket.
    Mengirim satu notifikasi Discord yang menggabungkan semua informasi.
    """
    global _candle_data_cache, _live_websocket_candle_data

    full_alert_message_parts = []
    current_price = None # Akan diisi dari candle terbaru dari salah satu TF

    for tf in INTERVALS:
        current_candles_for_analysis = list(_candle_data_cache.get(tf, []))
        live_candle_for_tf = _live_websocket_candle_data.get(tf)

        if live_candle_for_tf:
            if not current_candles_for_analysis or \
               live_candle_for_tf["time"] > current_candles_for_analysis[-1]["time"]:
                current_candles_for_analysis.append(live_candle_for_tf)
            elif current_candles_for_analysis and \
                 live_candle_for_tf["time"] == current_candles_for_analysis[-1]["time"] and \
                 not current_candles_for_analysis[-1]["is_final_bar"]:
                current_candles_for_analysis[-1] = live_candle_for_tf
        
        if not current_candles_for_analysis or len(current_candles_for_analysis) < CANDLE_LIMIT[tf]:
            # Adjust minimum candle requirement based on what analysis needs for this TF
            min_candles_needed = 3 # Basic PA needs 3, Chart patterns need more
            if tf == "1d" and len(current_candles_for_analysis) < 100: # For daily, chart patterns need around 100+
                 min_candles_needed = 100 
            elif len(current_candles_for_analysis) < 50: # For other TFs, SMC/SR need about 50
                 min_candles_needed = 50

            if len(current_candles_for_analysis) < min_candles_needed:
                full_alert_message_parts.append(f"\n---\n📊 **{tf.upper()} Timeframe**: Tidak cukup data untuk analisis. ({len(current_candles_for_analysis)}/{min_candles_needed} candles dibutuhkan)")
                continue

        latest_candle = current_candles_for_analysis[-1]
        if current_price is None:
            current_price = latest_candle['close']

        pa_patterns = detect_price_action(current_candles_for_analysis, enable_volume_confirmation=True)
        cp_patterns = detect_chart_patterns(current_candles_for_analysis)
        bos_choch_patterns = detect_bos_choch(current_candles_for_analysis)
        sr_levels = detect_auto_sr(current_candles_for_analysis)

        temp_all_tf_data_for_multi_tf = {}
        for _tf_check in INTERVALS:
            temp_candles = list(_candle_data_cache.get(_tf_check, []))
            _live_c = _live_websocket_candle_data.get(_tf_check)
            if _live_c:
                 if not temp_candles or _live_c["time"] > temp_candles[-1]["time"]:
                    temp_candles.append(_live_c)
                 elif temp_candles and _live_c["time"] == temp_candles[-1]["time"] and not temp_candles[-1]["is_final_bar"]:
                    temp_candles[-1] = _live_c
            temp_all_tf_data_for_multi_tf[_tf_check] = temp_candles

        multi_tf_confirmations = get_multi_tf_confirmations(pa_patterns + cp_patterns + bos_choch_patterns, temp_all_tf_data_for_multi_tf, tf)

        props = get_candle_properties(latest_candle)
        candle_type = "Bullish" if props["is_bullish"] else "Bearish" if props["is_bearish"] else "Doji-like"
        full_range_percent = (props["full_range"] / latest_candle["open"]) * 100 if latest_candle["open"] else 0
        
        status_candle_tag = "[C]" if latest_candle["is_final_bar"] else "[L]"
        status_candle_desc = "FINAL (Closed)" if latest_candle["is_final_bar"] else "LIVE (Open)"
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

        if not latest_candle["is_final_bar"]:
            progress = get_time_progress(latest_candle["time"], tf)
            time_progress_info = f" ({int(progress*100)}% progress)"
            live_candle_potential_info_msg = get_live_candle_potential_and_process(latest_candle)
            
        tf_msg_part = f"\n---\n📊 **{tf.upper()} Timeframe** {status_candle_tag} ({status_candle_desc}{time_progress_info})\n" \
                      f"**Waktu Analisis**: {datetime.datetime.now(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"
        
        if latest_candle["is_final_bar"]:
            tf_msg_part += f"**Waktu Penutupan Candle Sebelumnya**: {latest_candle['close_time'].astimezone(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"
        else:
            tf_msg_part += f"Waktu Pembukaan Candle: {latest_candle['time'].astimezone(TARGET_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')} WIB\n"


        tf_msg_part += f"Tipe Candle: {candle_type} (Range: {full_range_percent:.2f}%)\n" \
                       f"**Detail Candle (O/H/L/C)**:\n{candle_details}" + \
                       (live_candle_potential_info_msg + "\n" if live_candle_potential_info_msg else "") # Tambah newline jika ada info live

        detected_info = False
        if pa_patterns: 
            tf_msg_part += "🕯️ **Pola Candlestick**: " + ", ".join(pa_patterns) + "\n"
            detected_info = True
        
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
                    cp_descriptions.append(pattern) # Untuk pola chart lain jika ada di masa depan
            tf_msg_part += "📈 **Pola Chart**: " + "\n" + "\n".join([f"- {desc}" for desc in cp_descriptions]) + "\n"
            detected_info = True

        if bos_choch_patterns: 
            tf_msg_part += "🔄 **Struktur Pasar (SMC)**: " + ", ".join(bos_choch_patterns) + "\n"
            detected_info = True
        
        sr_info_part = ""
        if sr_levels.get("support"): 
            sr_info_part += "⬇️ **Support Levels**: " + ", ".join([f"{v:.2f}" for v in sr_levels["support"]]) + "\n"
            detected_info = True
            for s_level in sr_levels["support"]:
                if abs(current_price - s_level) / current_price < 0.005: # Dalam 0.5% dari S/R level
                    sr_info_part += f"⚠️ Harga saat ini ({current_price:.2f}) mendekati Support Level {s_level:.2f}\n"

        if sr_levels.get("resistance"): 
            sr_info_part += "⬆️ **Resistance Levels**: " + ", ".join([f"{v:.2f}" for v in sr_levels["resistance"]]) + "\n"
            detected_info = True
            for r_level in sr_levels["resistance"]:
                if abs(current_price - r_level) / current_price < 0.005: # Dalam 0.5% dari S/R level
                    sr_info_part += f"⚠️ Harga saat ini ({current_price:.2f}) mendekati Resistance Level {r_level:.2f}\n"
        tf_msg_part += sr_info_part

        if multi_tf_confirmations: 
            tf_msg_part += "✅ **Konfirmasi Multi-TF**: " + ", ".join(multi_tf_confirmations) + "\n"
            detected_info = True
        
        if not detected_info and not live_candle_potential_info_msg: # Only say "no patterns" if literally no info
            tf_msg_part += "Tidak ada pola/informasi signifikan terdeteksi saat ini.\n"

        full_alert_message_parts.append(tf_msg_part)

    if full_alert_message_parts:
        header_price = f"**Harga Terakhir {SYMBOL}**: {current_price:.2f}\n" if current_price else ""
        final_message = header_price + "\n".join(full_alert_message_parts)
        send_discord_alert(f"📡 {SYMBOL} Real-time Market Update", final_message)
    else:
        logger.info("Tidak ada update yang signifikan untuk dikirim dalam ringkasan real-time.")

# --- DISCORD ALERT ---
def send_discord_alert(title: str, message: str):
    """Mengirim notifikasi ke Discord via webhook."""
    if DISCORD_WEBHOOK == "<YOUR_DISCORD_WEBHOOK_HERE>":
        # Perbaikan syntax error di sini
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
        
        _live_websocket_candle_data[interval] = current_candle

        if current_candle["is_final_bar"] and \
           (last_processed_final_candle_time[interval] is None or \
            current_candle["close_time"] > last_processed_final_candle_time[interval]):
            
            logger.info(f"[{interval.upper()}] Candle DITUTUP (FINAL). Memperbarui data historis dari REST API.")
            _candle_data_cache[interval] = get_klines_rest(SYMBOL, interval, CANDLE_LIMIT[interval])
            last_processed_final_candle_time[interval] = current_candle["close_time"]

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
    ws_app.run_forever(ping_interval=30, ping_timeout=10)

# --- RUNNER ---
if __name__ == "__main__":
    if DISCORD_WEBHOOK == "<YOUR_DISCORD_WEBHOOK_HERE>":
        logger.error("ERROR: Variabel DISCORD_WEBHOOK belum diatur. Harap ganti placeholder dengan URL webhook Anda di awal skrip.")
        exit(1)
        
    logger.info("Bot pemantau harga dimulai. Mengambil data historis awal...")
    
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
    ws_thread.daemon = True
    ws_thread.start()

    logger.info("Memberi sedikit waktu untuk WebSocket terhubung dan menerima data live pertama...")
    time.sleep(5)

    logger.info("Memicu analisis awal dan mengirim notifikasi Discord pertama.")
    scan_all_intervals_and_notify()

    logger.info(f"Memulai loop analisis gabungan semua time-frame setiap {REAL_TIME_SCAN_INTERVAL_SECONDS / 60} menit (24/7).")
    while True:
        time.sleep(REAL_TIME_SCAN_INTERVAL_SECONDS)
        logger.info(f"Memicu analisis real-time semua time-frame pada interval {REAL_TIME_SCAN_INTERVAL_SECONDS / 60} menit.")
        scan_all_intervals_and_notify()
