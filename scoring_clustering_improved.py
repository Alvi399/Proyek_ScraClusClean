"""
Program Scoring dan Clustering Data Wilayah - Versi Diperbaiki
Memperbaiki akurasi validasi dan menambahkan fungsi clustering yang hilang
"""

import pandas as pd
from difflib import SequenceMatcher
import re
import logging
from typing import Tuple, Optional

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# KONFIGURASI
# ============================================================================

# Batas koordinat Surabaya (Update Final 2026)
SURABAYA_BOUNDS = {
    'lat_min': -7.36,  
    'lat_max': -7.15,  
    'lon_min': 112.59, 
    'lon_max': 112.88  
}

# Threshold untuk similarity scoring
SIMILARITY_THRESHOLDS = {
    'high': 0.75,      # Ditemukan dengan confidence tinggi
    'medium': 0.60,    # Perlu review manual
    'low': 0.45        # Kemungkinan tidak ditemukan
}


# ============================================================================
# FUNGSI PEMBERSIHAN DAN NORMALISASI TEKS
# ============================================================================

def clean_text(text):
    """Membersihkan dan normalisasi teks untuk perbandingan"""
    if pd.isna(text):
        return ""
    text = str(text).lower()
    # Hapus karakter khusus tapi pertahankan spasi
    text = re.sub(r'[^\w\s]', ' ', text)
    # Hapus spasi berlebih
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ============================================================================
# FUNGSI SCORING
# ============================================================================

def calculate_similarity(query, place_name, address):
    """
    Menghitung similarity score antara query dengan place_name + address
    
    Returns:
        float: Similarity score antara 0 dan 1
    """
    query_clean = clean_text(query)
    place_clean = clean_text(place_name)
    address_clean = clean_text(address)
    
    # Gabungkan place name dan address
    combined = f"{place_clean} {address_clean}"
    
    # Hitung similarity menggunakan SequenceMatcher
    similarity = SequenceMatcher(None, query_clean, combined).ratio()
    
    # Berikan bonus jika place_name sangat mirip dengan query
    place_similarity = SequenceMatcher(None, query_clean, place_clean).ratio()
    
    # Weighted score: 60% dari combined similarity, 40% dari place similarity
    final_score = (similarity * 0.6) + (place_similarity * 0.4)
    
    return final_score


def classify_similarity(score: float) -> str:
    """
    Klasifikasi similarity score ke dalam kategori
    
    Args:
        score: Similarity score (0-1)
        
    Returns:
        str: Kategori validasi
    """
    if score >= SIMILARITY_THRESHOLDS['high']:
        return 'Ditemukan'
    elif score >= SIMILARITY_THRESHOLDS['medium']:
        return 'Perlu Review'
    else:
        return 'Tidak Ditemukan'


def scoring_data(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Melakukan scoring similarity untuk setiap baris data
    
    Args:
        df_input: DataFrame input
        
    Returns:
        DataFrame dengan kolom similarity_score tambahan
    """
    logger.info("Memulai proses scoring...")
    
    # Bersihkan data
    def clean_cell(x):
        if isinstance(x, str):
            return (
                x.replace('', '')
                .replace('\n', ' ')
                .replace('\r', ' ')
                .strip()
            )
        return x
    
    df_res = df_input.map(clean_cell)
    
    # Validasi kolom yang diperlukan
    required_cols = ['idsbr', 'Query', 'Actual Place Name', 'Address']
    missing_cols = [col for col in required_cols if col not in df_res.columns]
    if missing_cols:
        logger.error(f"Kolom yang hilang: {missing_cols}")
        raise ValueError(f"Kolom yang hilang: {missing_cols}")
    
    logger.info(f"Total baris: {len(df_res)}")
    
    # Tambahkan kolom similarity_score
    df_res['similarity_score'] = 0.0
    
    # Hitung similarity score untuk setiap baris
    for idx, row in df_res.iterrows():
        score = calculate_similarity(
            row['Query'],
            row['Actual Place Name'],
            row['Address']
        )
        df_res.at[idx, 'similarity_score'] = score
    
    logger.info("Proses scoring selesai")
    return df_res


# ============================================================================
# FUNGSI VALIDASI DAN DEDUPLIKASI
# ============================================================================

def validate_and_deduplicate(df_scored: pd.DataFrame) -> pd.DataFrame:
    """
    Melakukan validasi dan deduplikasi berdasarkan similarity score
    
    Args:
        df_scored: DataFrame yang sudah memiliki similarity_score
        
    Returns:
        DataFrame dengan kolom Validasi dan deduplikasi
    """
    logger.info("Memulai proses validasi dan deduplikasi...")
    
    # Tambahkan kolom validasi
    df_scored['Validasi'] = df_scored['similarity_score'].apply(classify_similarity)
    
    # Group by idsbr untuk deduplikasi
    grouped = df_scored.groupby('idsbr')
    
    processed_rows = []
    
    for idsbr, group in grouped:
        if pd.isna(idsbr) or str(idsbr).strip() == '':
            # Jika idsbr kosong, tandai sebagai tidak ditemukan
            for idx, row in group.iterrows():
                row_dict = row.to_dict()
                row_dict['Validasi'] = 'Tidak Ditemukan'
                row_dict['is_winner'] = False
                processed_rows.append(row_dict)
            continue
        
        # Urutkan berdasarkan similarity score (descending)
        sorted_group = group.sort_values('similarity_score', ascending=False)
        
        # Ambil winner (score tertinggi)
        winner_idx = sorted_group.index[0]
        winner_score = sorted_group.loc[winner_idx, 'similarity_score']
        
        for idx, row in sorted_group.iterrows():
            row_dict = row.to_dict()
            
            if idx == winner_idx:
                # Ini adalah winner - pertahankan validasi berdasarkan score
                row_dict['is_winner'] = True
                row_dict['idsbr'] = idsbr
            else:
                # Ini adalah loser - tandai sebagai duplikat
                row_dict['Validasi'] = 'Duplikat'
                row_dict['is_winner'] = False
                # Kosongkan idsbr untuk loser bisa dihapus nanti
            
            processed_rows.append(row_dict)
    
    result_df = pd.DataFrame(processed_rows)
    
    logger.info(f"Deduplikasi selesai. Total rows: {len(result_df)}")
    logger.info(f"Distribusi validasi:\n{result_df['Validasi'].value_counts()}")
    
    return result_df


# ============================================================================
# FUNGSI CLUSTERING BERDASARKAN LOKASI GEOGRAFIS
# ============================================================================

def bersihkan_dan_konversi(nilai, tipe='lat') -> Optional[float]:
    """
    Membersihkan format koordinat dan konversi ke float
    
    Args:
        nilai: Nilai koordinat (bisa string atau numeric)
        tipe: Tipe koordinat ('lat' atau 'lon')
        
    Returns:
        float atau None jika gagal konversi
    """
    try:
        if pd.isna(nilai): 
            return None
        
        # Ubah ke string dan bersihkan karakter non-numerik kecuali minus dan titik
        s = re.sub(r'[^0-9\.\-]', '', str(nilai))
        
        # Jika kosong setelah cleaning
        if not s or s == '-':
            return None
        
        # Jika ada lebih dari satu titik, format ulang
        if s.count('.') > 1:
            digits = s.replace('.', '').replace('-', '')
            sign = "-" if "-" in s else ""
            
            if tipe == 'lat':
                # Latitude Surabaya: -7.xxx
                if len(digits) >= 2:
                    return float(sign + digits[0] + "." + digits[1:])
            else:
                # Longitude Surabaya: 112.xxx
                if len(digits) >= 4:
                    return float(digits[:3] + "." + digits[3:])
        
        result = float(s)
        
        # Validasi range yang masuk akal untuk Indonesia
        if tipe == 'lat':
            if result < -12 or result > 8:  # Indonesia latitude range
                return None
        else:
            if result < 95 or result > 142:  # Indonesia longitude range
                return None
        
        return result
    except (ValueError, TypeError):
        return None


def cek_lokasi_surabaya(lat_raw, lon_raw) -> str:
    """
    Mengecek apakah koordinat berada di dalam batas Surabaya
    
    Args:
        lat_raw: Latitude mentah
        lon_raw: Longitude mentah
        
    Returns:
        str: Status lokasi ('disurabaya', 'tidak disurabaya', atau 'error_koordinat')
    """
    lat = bersihkan_dan_konversi(lat_raw, 'lat')
    lon = bersihkan_dan_konversi(lon_raw, 'lon')
    
    if lat is None or lon is None:
        return "error_koordinat"
    
    # Periksa apakah dalam bounds Surabaya
    is_lat_in = SURABAYA_BOUNDS['lat_min'] <= lat <= SURABAYA_BOUNDS['lat_max']
    is_lon_in = SURABAYA_BOUNDS['lon_min'] <= lon <= SURABAYA_BOUNDS['lon_max']
    
    if is_lat_in and is_lon_in:
        return "disurabaya"
    else:
        return "tidak disurabaya"


def clustering_data(df_scored: pd.DataFrame) -> pd.DataFrame:
    """
    FUNGSI YANG HILANG - Melakukan clustering berdasarkan lokasi geografis
    
    Args:
        df_scored: DataFrame yang sudah memiliki similarity_score
        
    Returns:
        DataFrame dengan kolom status_lokasi dan cluster_wilayah
    """
    logger.info("Memulai proses clustering wilayah...")
    
    # Pastikan kolom koordinat ada
    required_coords = ['Latitude', 'Longitude']
    missing_coords = [col for col in required_coords if col not in df_scored.columns]
    if missing_coords:
        logger.error(f"Kolom koordinat yang hilang: {missing_coords}")
        raise ValueError(f"Kolom koordinat yang hilang: {missing_coords}")
    
    # Tambahkan kolom status lokasi
    df_scored['status_lokasi'] = df_scored.apply(
        lambda row: cek_lokasi_surabaya(row['Latitude'], row['Longitude']), 
        axis=1
    )
    
    # Bersihkan koordinat untuk analisis lebih lanjut
    df_scored['lat_clean'] = df_scored['Latitude'].apply(lambda x: bersihkan_dan_konversi(x, 'lat'))
    df_scored['lon_clean'] = df_scored['Longitude'].apply(lambda x: bersihkan_dan_konversi(x, 'lon'))
    
    # Tambahkan cluster wilayah berdasarkan status lokasi
    def assign_cluster(row):
        status = row['status_lokasi']
        if status == 'disurabaya':
            # Bisa diperluas dengan clustering geografis lebih detail 
            # misalnya berdasarkan kecamatan atau koordinat grid
            return 'Surabaya'
        elif status == 'tidak disurabaya':
            return 'Luar Surabaya'
        else:
            return 'Unknown'
    
    df_scored['cluster_wilayah'] = df_scored.apply(assign_cluster, axis=1)
    
    # Statistik clustering
    logger.info(f"Distribusi status lokasi:\n{df_scored['status_lokasi'].value_counts()}")
    logger.info(f"Distribusi cluster wilayah:\n{df_scored['cluster_wilayah'].value_counts()}")
    
    return df_scored


# ============================================================================
# FUNGSI UTAMA PIPELINE
# ============================================================================

def process_complete_pipeline(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Menjalankan pipeline lengkap: scoring -> validasi -> clustering
    
    Args:
        df_input: DataFrame input
        
    Returns:
        DataFrame hasil akhir dengan semua kolom analisis
    """
    logger.info("="*60)
    logger.info("MEMULAI PIPELINE LENGKAP")
    logger.info("="*60)
    
    # Step 1: Scoring
    df_scored = scoring_data(df_input)
    
    # Step 2: Validasi dan Deduplikasi
    df_validated = validate_and_deduplicate(df_scored)
    
    # Step 3: Clustering Wilayah
    df_final = clustering_data(df_validated)
    
    # Urutkan kolom untuk kemudahan pembacaan
    kolom_utama = [
        'idsbr', 'Query', 'Actual Place Name', 'Category', 'Rating',
        'Address', 'Phone Number', 'Website', 
        'Latitude', 'Longitude', 'lat_clean', 'lon_clean',
        'Status', 'Open Status', 'Operation Hours',
        'similarity_score', 'Validasi', 'is_winner',
        'status_lokasi', 'cluster_wilayah'
    ]
    
    # Ambil kolom yang ada
    kolom_ada = [c for c in kolom_utama if c in df_final.columns]
    kolom_tambahan = [c for c in df_final.columns if c not in kolom_ada]
    
    df_final = df_final[kolom_ada + kolom_tambahan]
    
    logger.info("="*60)
    logger.info("PIPELINE SELESAI")
    logger.info("="*60)
    
    return df_final


# ============================================================================
# FUNGSI REPORTING DAN ANALISIS
# ============================================================================

def generate_report(df_result: pd.DataFrame) -> dict:
    """
    Generate laporan statistik dari hasil clustering
    
    Returns:
        dict: Laporan statistik lengkap
    """
    report = {
        'total_data': len(df_result),
        'validasi_distribusi': df_result['Validasi'].value_counts().to_dict(),
        'lokasi_distribusi': df_result['status_lokasi'].value_counts().to_dict(),
        'cluster_distribusi': df_result['cluster_wilayah'].value_counts().to_dict(),
        'similarity_stats': {
            'mean': df_result['similarity_score'].mean(),
            'median': df_result['similarity_score'].median(),
            'std': df_result['similarity_score'].std(),
            'min': df_result['similarity_score'].min(),
            'max': df_result['similarity_score'].max()
        },
        'data_quality': {
            'winner_count': df_result['is_winner'].sum(),
            'duplicate_count': (df_result['Validasi'] == 'Duplikat').sum(),
            'error_koordinat': (df_result['status_lokasi'] == 'error_koordinat').sum()
        }
    }
    
    return report


def print_report(report: dict):
    """Print laporan dalam format yang mudah dibaca"""
    print("\n" + "="*60)
    print("LAPORAN HASIL CLUSTERING WILAYAH")
    print("="*60)
    
    print(f"\n📊 Total Data: {report['total_data']:,}")
    
    print("\n✅ Distribusi Validasi:")
    for status, count in report['validasi_distribusi'].items():
        pct = (count / report['total_data']) * 100
        print(f"  - {status}: {count:,} ({pct:.2f}%)")
    
    print("\n🗺️  Distribusi Lokasi:")
    for status, count in report['lokasi_distribusi'].items():
        pct = (count / report['total_data']) * 100
        print(f"  - {status}: {count:,} ({pct:.2f}%)")
    
    print("\n📍 Distribusi Cluster Wilayah:")
    for cluster, count in report['cluster_distribusi'].items():
        pct = (count / report['total_data']) * 100
        print(f"  - {cluster}: {count:,} ({pct:.2f}%)")
    
    print("\n📈 Statistik Similarity Score:")
    stats = report['similarity_stats']
    print(f"  - Mean: {stats['mean']:.4f}")
    print(f"  - Median: {stats['median']:.4f}")
    print(f"  - Std Dev: {stats['std']:.4f}")
    print(f"  - Min: {stats['min']:.4f}")
    print(f"  - Max: {stats['max']:.4f}")
    
    print("\n🔍 Kualitas Data:")
    quality = report['data_quality']
    print(f"  - Winner (Data Unik): {quality['winner_count']:,}")
    print(f"  - Duplikat: {quality['duplicate_count']:,}")
    print(f"  - Error Koordinat: {quality['error_koordinat']:,}")
    
    print("\n" + "="*60)


# ============================================================================
# CONTOH PENGGUNAAN
# ============================================================================

if __name__ == "__main__":
    # Contoh: Load data dari Excel
    print("Contoh Penggunaan Script:")
    print("\n# 1. Load data")
    print('df = pd.read_excel("scraping_10feb_cleaned.xlsx")')
    
    print("\n# 2. Jalankan pipeline lengkap")
    print("df_result = process_complete_pipeline(df)")
    
    print("\n# 3. Generate dan print report")
    print("report = generate_report(df_result)")
    print("print_report(report)")
    
    print("\n# 4. Simpan hasil")
    print('df_result.to_excel("hasil_clustering_final.xlsx", index=False)')
    print('df_result.to_csv("hasil_clustering_final.csv", index=False)')
    
    print("\n# 5. Filter data tertentu")
    print("# Hanya data yang ditemukan di Surabaya")
    print('df_surabaya = df_result[')
    print('    (df_result["Validasi"] == "Ditemukan") &')
    print('    (df_result["status_lokasi"] == "disurabaya")')
    print(']')
