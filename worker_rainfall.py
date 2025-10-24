import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime, timezone, timedelta
import time
import sys 
import psycopg2


# COLOR ANSI
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

# Variabel Lingkungan yang Wajib Dicek
ENV_VARS_TO_CHECK = [
    "BASE_URL",
    "API_KEY",
    "X_API_SECRET",
    "STATION_ID",
    "TARGET_LSID",
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
]

def load_check_env():
    """Cek .env kali aja kosong"""
    load_dotenv()
    is_all_filled = True

    print("-------------------------------------------------")
    print("Status ENV:")
    for var_name in ENV_VARS_TO_CHECK:
        var_value = os.getenv(var_name)
        if not var_value:
            status_text = f"{RED}KOSONG{RESET}"
            is_all_filled = False
        else:
            status_text = f"{GREEN}ADA{RESET}"

        print(f"{var_name:<15}: {status_text}")

    print("-------------------------------------------------")
    if not is_all_filled:
        print(RED + "⚠️ CHECK .env !" + RESET)
    else:
        print(GREEN + "✅ ARMED" + RESET)
        
    return is_all_filled

def get_dotenv():
    """Get all variable .env"""
    load_dotenv()

    baseURL     = os.getenv("BASE_URL")
    apiKey      = os.getenv("API_KEY")
    secretKey   = os.getenv("X_API_SECRET")
    stationID   = os.getenv("STATION_ID")
    
    # Variabel DB
    db_host     = os.getenv("DB_HOST")
    db_port     = os.getenv("DB_PORT")
    db_name     = os.getenv("DB_NAME")
    db_user     = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    sensorID_str = os.getenv("TARGET_LSID")
    
    try:
        sensorID = int(sensorID_str)
    except (TypeError, ValueError):
        sensorID = None 

    return baseURL, apiKey, secretKey, stationID, sensorID, db_host, db_port, db_name, db_user, db_password


def create_db_connection(db_host, db_port, db_name, db_user, db_password):
    """Membuat dan mengembalikan koneksi ke PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print(GREEN + "✅ Koneksi PostgreSQL Berhasil!" + RESET)
        return conn
    except psycopg2.Error as e:
        # oper ke sys.stderr agar terbaca oleh log Docker
        print(RED + f"❌ Gagal koneksi ke PostgreSQL: {e}" + RESET, file=sys.stderr) 
        return None

def insert_data(conn, data):

    ts_tz = data.get("ts_tz")
    rain_rate_last_mm = data.get("rain_rate_last_mm")
    station_id = data.get("station_id")
    created_at_for_check = data.get("created_at") # 👈 Ambil untuk cek kelengkapan data

    # ⚠️ Cek kelengkapan data (created_at_for_check di sini seharusnya tidak pernah None karena sudah diperbaiki di process_data)
    if not ts_tz or rain_rate_last_mm is None or not station_id or not created_at_for_check:
        print(RED + "❌ Data tidak lengkap untuk di-insert." + RESET, file=sys.stderr)
        return False

    cursor = conn.cursor()

    # Query SQL: INSERT ON CONFLICT (time, rain, station_id) DO NOTHING
    # ✅ created_at sekarang menggunakan NOW() dari PostgreSQL
    insert_query = """
    INSERT INTO baramurti_aws_data (station_id, time, rain, created_at)
    VALUES (%s, %s, %s, NOW()) 
    ON CONFLICT (time, rain, station_id) DO NOTHING;
    """

    try:
        # ✅ Hanya 3 parameter yang dikirim: station_id, ts_tz, rain_rate_last_mm
        # created_at_for_check (dari Python) DIABAIKAN di sini
        cursor.execute(insert_query, (station_id, ts_tz, rain_rate_last_mm))

        if cursor.rowcount > 0:
            conn.commit()
            print(GREEN + f"✅ Data baru berhasil di-insert. Time: {ts_tz}" + RESET)
            return True
        else:
            print(YELLOW + f"⚠️ Data dengan timestamp {ts_tz} sudah ada. Skip." + RESET)
            return False

    except psycopg2.Error as e:
        conn.rollback()
        print(RED + f"❌ Gagal INSERT data ke DB: {e}" + RESET, file=sys.stderr)
        return False
    except Exception as e:
        conn.rollback()
        print(RED + f"❌ Error tak terduga saat INSERT: {e}" + RESET, file=sys.stderr)
        return False
    finally:
        cursor.close()

def save_failover_json(data_to_save, error_type="UNKNOWN_ERROR"):
    """[Failover Item saat single run] Menyimpan data hasil fetch ke file JSON saat terjadi error insert(hanya satu data)."""
    
    failover_dir = "failover_logs"
    os.makedirs(failover_dir, exist_ok=True)
    
    now = datetime.now()
    timestamp_str = now.strftime('%Y%m%d%H%M') 
    filename = f"{failover_dir}/{timestamp_str}_{error_type}_worker_fail.json"
    
    # Struktur JSON yang disederhanakan
    failover_data = {
        "error_timestamp_local": now.isoformat(),
        "error_type": error_type,
        "source_data": data_to_save # data_to_save sudah berisi created_at
    }

    try:
        with open(filename, 'w') as f:
            json.dump(failover_data, f, indent=4)
        print(YELLOW + f"⚠️ Data dialihkan ke file failover: {filename}" + RESET)
    except Exception as e:
        print(RED + f"❌ Gagal menyimpan file failover {filename}: {e}" + RESET, file=sys.stderr)

def save_failover_cumulative(data_list, error_type="UNKNOWN_FATAL"):
    """[Failover Kumulatif/Array] Menyimpan list data yang diproses ke file JSON saat terjadi error fatal."""
    
    failover_dir = "failover_logs"
    os.makedirs(failover_dir, exist_ok=True)
    
    now = datetime.now()
    timestamp_str = now.strftime('%Y%m%d%H%M') 
    
    # Gunakan nama file yang unik untuk failover kumulatif
    filename = f"{failover_dir}/{timestamp_str}_CUMULATIVE_{error_type}_fail.json" 
    
    # Struktur JSON akan menjadi list (array) dari objek-objek error
    final_list_to_dump = []

    # Kita akan membuat satu objek error/data per item dalam data_list
    for item in data_list:
        final_list_to_dump.append({
            "error_timestamp_local": now.isoformat(),
            "error_type": error_type,
            "source_data": item
        })

    try:
        with open(filename, 'w') as f:
            json.dump(final_list_to_dump, f, indent=4)
        print(YELLOW + f"⚠️ Data kumulatif ({len(data_list)} item) dialihkan ke file failover: {filename}" + RESET)
    except Exception as e:
        print(RED + f"❌ Gagal menyimpan file failover kumulatif {filename}: {e}" + RESET, file=sys.stderr)


def process_data(raw_filtered_data, station_id, sensor_id):
    """Memproses data mentah menjadi format final yang siap di-insert."""

    rainfall_day = raw_filtered_data.get("rain_rate_last_mm")
    timestamp_unix = raw_filtered_data.get("ts")
    tz_offset_seconds = raw_filtered_data.get("tz_offset") 
    
    # Inisialisasi datetime_utc untuk menghindari NameError
    datetime_utc = None 
    
    # 🆕 Tambahkan waktu eksekusi saat ini untuk keperluan Failover JSON
    created_at_ts = datetime.now(timezone.utc) 

    try:
        # BLOK KONVERSI WAKTU DATA API (ts_tz)
        if timestamp_unix is not None:
            # Konversi UNIX timestamp ke datetime objek
            datetime_utc = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)
            
    except (TypeError, ValueError, AttributeError) as e:
        print(RED + f"❌ Konversi Timestamp Gagal: {e}" + RESET, file=sys.stderr)
        pass 


    piece_final = {
        "rain_rate_last_mm": rainfall_day,
        "ts_tz": datetime_utc, 
        "station_id": station_id,
        "sensor_id": sensor_id,
        "created_at": created_at_ts  # 👈 Variabel ini ADA di piece_final untuk Failover
    }

    return piece_final

def fetch_api(baseURL, apiKey, secretKey, stationID, sensorID):
    """Melakukan request API dan memfilter data sensor yang sesuai."""
    
    endpoint = f"{baseURL}/current/{stationID}"
    
    params = {
        "api-key": apiKey,
    }

    headers = {
        "X-API-SECRET": secretKey
    }

    print(f"-> Melakukan request ke {endpoint}")
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        raw_data = response.json()

        # Cari sensor yang sesuai
        filtered_data = None
        for sensor in raw_data.get("sensors", []):
            if sensor.get("lsid") == sensorID:
                if sensor.get("data"):
                    # Mengambil dictionary pertama dari list "data"
                    filtered_data = sensor["data"][0]
                    break
                    
        if not filtered_data:
            print(f"⚠️ Peringatan: data sensor {sensorID} tidak ditemukan atau datanya kosong.")
            return None
        
        print(f"✅ Data sensor {sensorID} berhasil difilter.")
        return filtered_data

    except requests.exceptions.HTTPError as err:
        print(RED + f"❌ HTTP Error: {err}" + RESET, file=sys.stderr) 
        print(f"URL Request Gagal: {response.url}")
        return None
    except requests.exceptions.RequestException as err:
        print(RED + f"❌ Error Jaringan: {err}" + RESET, file=sys.stderr)
        return None
    except Exception as e:
        print(RED + f"❌ Error tak terduga saat fetch: {e}" + RESET, file=sys.stderr)
        return None
        
#================================================
#               FUNGSI SIKLUS INTI
#================================================

def worker_cycle_logic(base_url, api_key, x_api_secret, station_id, sensor_id, db_conn):
    """
    [Logika Inti 1x Jalan] Melakukan fetch, proses, dan insert data untuk satu siklus.
    Mengembalikan data yang diproses (piece_final) untuk keperluan failover.
    """
    final_processed_data = None
    
    # 1. FETCH DATA
    raw_filtered_data = fetch_api(
        baseURL=base_url, 
        apiKey=api_key,
        secretKey=x_api_secret,
        stationID=station_id,
        sensorID=sensor_id
    )

    if not raw_filtered_data:
        print(YELLOW + "⚠️ Fetch API gagal atau data kosong. Skip insert." + RESET)
        return None
        
    # 2. PROSES DATA
    final_processed_data = process_data(raw_filtered_data, station_id, sensor_id)
    
    # 3. INSERT DATA KE DB
    if db_conn:
        # Panggil fungsi insert
        insert_success = insert_data(db_conn, final_processed_data)
        
        # Jika insert gagal dan BUKAN karena data sudah ada, catat sebagai failover item tunggal
        if not insert_success and final_processed_data.get("ts_tz") != "Konversi Gagal":
            save_failover_json(final_processed_data, error_type="DB_INSERT_FAIL")
    else:
        # Koneksi DB hilang/gagal
        raise Exception("NO_DB_CONNECTION")
        
    return final_processed_data

#================================================
#               FUNGSI RUNNER (MODE EKSEKUSI)
#================================================

def worker_loop(base_url, api_key, x_api_secret, station_id, sensor_id, db_host, db_port, db_name, db_user, db_password, delay_seconds=60):
    """
    [Runner Looping Selamanya] Mengulang siklus secara terus menerus, menyimpan cache data.
    """
    
    processed_data_cache = [] # Cache untuk Failover Kumulatif
    
    db_conn = create_db_connection(db_host, db_port, db_name, db_user, db_password)
    if db_conn is None:
        print(RED + "❌ Worker berhenti karena koneksi DB awal gagal." + RESET, file=sys.stderr)
        sys.exit(1)

    while True:
        final_processed_data = None
        
        try:
            print("\n--- Worker Cycle Mulai ---")
            
            # Cek dan coba reconnect DB
            if db_conn.closed:
                 print(YELLOW + "⚠️ Koneksi DB terputus, mencoba sambungkan kembali..." + RESET)
                 db_conn = create_db_connection(db_host, db_port, db_name, db_user, db_password)
                 if db_conn is None:
                     raise Exception("DB_RECONNECT_FAIL") 
            
            # Panggil logika inti
            final_processed_data = worker_cycle_logic(base_url, api_key, x_api_secret, station_id, sensor_id, db_conn)
            
            # 🆕 TAMBAHKAN DATA YANG BERHASIL DIPROSES KE CACHE
            if final_processed_data:
                processed_data_cache.append(final_processed_data)

        except Exception as e:
            error_msg = str(e)
            error_type = "GENERAL_ERROR"
            
            # Mendeteksi tipe error fatal untuk penamaan failover
            if "DB_RECONNECT_FAIL" in error_msg or "NO_DB_CONNECTION" in error_msg or "psycopg2" in error_msg:
                error_type = "DB_CONN_FAIL"
            elif "HTTP Error" in error_msg or "Error Jaringan" in error_msg:
                error_type = "API_FETCH_FAIL"

            # 🛑 Tindakan Failover Kumulatif sebelum exit
            if processed_data_cache:
                save_failover_cumulative(processed_data_cache, error_type=error_type)
            
            # Data siklus terakhir yang sedang gagal juga dicatat sebagai failover item tunggal
            if final_processed_data and "DB_INSERT_FAIL" not in error_type: 
                 save_failover_json(final_processed_data, error_type=error_type)

            print(RED + f"Worker dihentikan karena error fatal: {error_type}." + RESET, file=sys.stderr)
            
            if db_conn and not db_conn.closed:
                db_conn.close()
            
            sys.exit(1)

        finally:
            # INTERVAL DELAY
            print(f"\n--- Worker menunggu interval {delay_seconds/60:.2f} menit ({delay_seconds} detik)... ---")
            time.sleep(delay_seconds)

def run_worker_app(delay_minutes=1):
    """
    [Runner Looping] Fungsi utama untuk menjalankan worker SELAMANYA (Produksi).
    """
    delay_seconds = delay_minutes * 60 
    
    is_armed = load_check_env()
    
    if is_armed:
        try:
            base_url, api_key, x_api_secret, station_id , sensor_id, db_host, db_port, db_name, db_user, db_password = get_dotenv()
        except ValueError as e:
            print(RED + f"❌ Gagal mengambil variabel lingkungan: {e}" + RESET, file=sys.stderr)
            sys.exit(1)
            
        if sensor_id is not None:
            print(YELLOW + f"\n*** MEMULAI WORKER DENGAN INTERVAL {delay_minutes} MENIT (SELAMANYA) ***" + RESET)
            worker_loop(
                base_url, api_key, x_api_secret, station_id , sensor_id, 
                db_host, db_port, db_name, db_user, db_password,
                delay_seconds=delay_seconds
            )
        else:
             print(RED + "⚠️ Gagal menjalankan worker: TARGET_LSID bukan angka atau kosong." + RESET, file=sys.stderr)
             sys.exit(1)
    else:
        print(RED + "⚠️ Worker dihentikan: Konfigurasi ENV tidak lengkap." + RESET, file=sys.stderr)
        sys.exit(1)

def run_worker_limited_cycles(max_cycles=3, delay_minutes=1):
    """
    [Runner Terbatas] Menjalankan worker untuk sejumlah siklus tertentu, dengan cache failover.
    """
    delay_seconds = delay_minutes * 60 
    
    print(YELLOW + f"\n*** MEMULAI WORKER DALAM MODE TERBATAS ({max_cycles} SIKLUS @ {delay_minutes} MENIT) ***" + RESET)

    is_armed = load_check_env()
    
    if not is_armed:
        print(RED + "⚠️ Worker dihentikan: Konfigurasi ENV tidak lengkap." + RESET, file=sys.stderr)
        sys.exit(1)

    try:
        base_url, api_key, x_api_secret, station_id , sensor_id, db_host, db_port, db_name, db_user, db_password = get_dotenv()
    except ValueError as e:
        print(RED + f"❌ Gagal mengambil variabel lingkungan: {e}" + RESET, file=sys.stderr)
        sys.exit(1)
        
    if sensor_id is None:
        print(RED + "⚠️ Gagal menjalankan worker: TARGET_LSID bukan angka atau kosong." + RESET, file=sys.stderr)
        sys.exit(1)

    db_conn = create_db_connection(db_host, db_port, db_name, db_user, db_password)
    if db_conn is None:
        print(RED + "❌ Worker berhenti karena koneksi DB gagal." + RESET, file=sys.stderr)
        sys.exit(1)
        
    processed_data_cache = [] # Cache untuk Failover Kumulatif
        
    # --- LOOP UTAMA TERBATAS ---
    for cycle_num in range(1, max_cycles + 1):
        final_processed_data = None
        print(f"\n--- Worker Cycle Mulai (Siklus {cycle_num}/{max_cycles}) ---")
        
        try:
            if db_conn.closed:
                 print(YELLOW + "⚠️ Koneksi DB terputus, mencoba sambungkan kembali..." + RESET)
                 db_conn = create_db_connection(db_host, db_port, db_name, db_user, db_password)
                 if db_conn is None:
                     raise Exception("DB_RECONNECT_FAIL") 
            
            final_processed_data = worker_cycle_logic(base_url, api_key, x_api_secret, station_id, sensor_id, db_conn)
            
            # 🆕 TAMBAHKAN DATA YANG BERHASIL DIPROSES KE CACHE
            if final_processed_data:
                processed_data_cache.append(final_processed_data)

        except Exception as e:
            error_msg = str(e)
            error_type = "GENERAL_ERROR"
            
            # Mendeteksi tipe error fatal untuk penamaan failover
            if "DB_RECONNECT_FAIL" in error_msg or "NO_DB_CONNECTION" in error_msg or "psycopg2" in error_msg:
                error_type = "DB_CONN_FAIL"
            elif "HTTP Error" in error_msg or "Error Jaringan" in error_msg:
                error_type = "API_FETCH_FAIL"
            
            # 🛑 Tindakan Failover Kumulatif sebelum exit
            if processed_data_cache:
                save_failover_cumulative(processed_data_cache, error_type=error_type)
            
            # Data siklus terakhir yang sedang gagal juga dicatat
            if final_processed_data and "DB_INSERT_FAIL" not in error_type:
                 save_failover_json(final_processed_data, error_type=error_type)
            
            print(RED + f"\n❌ ERROR FATAL DITEMUKAN: {error_msg} (Tipe: {error_type}). Worker dihentikan." + RESET, file=sys.stderr)
            
            if db_conn and not db_conn.closed:
                db_conn.close()
            
            sys.exit(1)

        # INTERVAL DELAY (Hanya jika belum mencapai siklus terakhir)
        if cycle_num < max_cycles:
            print(f"\n--- Worker menunggu interval {delay_seconds/60:.2f} menit ({delay_seconds} detik)... ---")
            time.sleep(delay_seconds)
    
    if db_conn and not db_conn.closed:
        db_conn.close()
        print(YELLOW + "Koneksi DB ditutup." + RESET)
        
    print(GREEN + f"\n*** Worker selesai menjalankan {max_cycles} siklus dan dihentikan. ***" + RESET)

def run_worker_single_cycle():
    """
    [Runner Single Cycle] Menjalankan satu kali fetch, proses, dan insert (Test Ride).
    Logika ini TIDAK menggunakan cache kumulatif.
    """
    print(YELLOW + "\n*** MEMULAI WORKER DALAM MODE SINGLE-CYCLE (TEST RIDE) ***" + RESET)
    is_armed = load_check_env()
    
    if not is_armed:
        print(RED + "⚠️ Worker dihentikan: Konfigurasi ENV tidak lengkap." + RESET, file=sys.stderr)
        sys.exit(1)

    try:
        base_url, api_key, x_api_secret, station_id , sensor_id, db_host, db_port, db_name, db_user, db_password = get_dotenv()
    except ValueError as e:
        print(RED + f"❌ Gagal mengambil variabel lingkungan: {e}" + RESET, file=sys.stderr)
        sys.exit(1)
        
    if sensor_id is None:
        print(RED + "⚠️ Gagal menjalankan worker: TARGET_LSID bukan angka atau kosong." + RESET, file=sys.stderr)
        sys.exit(1)

    # 1. Buat koneksi DB (sekali)
    db_conn = create_db_connection(db_host, db_port, db_name, db_user, db_password)
    if db_conn is None:
        print(RED + "❌ Single Cycle berhenti karena koneksi DB gagal." + RESET, file=sys.stderr)
        sys.exit(1)
        
    final_processed_data = None
    try:
        # Panggil logika inti satu siklus
        final_processed_data = worker_cycle_logic(base_url, api_key, x_api_secret, station_id, sensor_id, db_conn)
        
        if final_processed_data:
            print(GREEN + "\n✅ Single Cycle berhasil diselesaikan." + RESET)
        
    except Exception as e:
        error_msg = str(e)
        error_type = "SINGLE_RUN_FAIL"
        
        print(RED + f"\n❌ ERROR FATAL DITEMUKAN pada Single Cycle: {error_msg}" + RESET, file=sys.stderr)
        # Jika ada data yang sempat diproses sebelum kegagalan, simpan sebagai failover item tunggal
        if final_processed_data:
            save_failover_json(final_processed_data, error_type=error_type) 
        sys.exit(1)
    finally:
        if db_conn and not db_conn.closed:
            db_conn.close()
            print("Koneksi DB ditutup.")

#================================================
#               FUNGSI UTAMA (MAIN)
#================================================

if __name__ == "__main__":
    
    # --- PUSAT KONTROL EKSEKUSI ---
    
    # 1. PRODUKSI SELAMANYA:
    run_worker_app(delay_minutes=5) 
    
    # 2. SATU KALI RUN:
    #run_worker_single_cycle()
    
    # 3. TEST TERBATAS (Misal: 5x dengan jeda 10 detik):
    # run_worker_limited_cycles(max_cycles=10, delay_minutes=15)
