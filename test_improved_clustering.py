# Script Testing untuk Pipeline Clustering yang Sudah Diperbaiki
# Menjalankan pipeline lengkap untuk clustering wilayah

from scoring_clustering_improved import (
    process_complete_pipeline,
    generate_report,
    print_report,
    scoring_data,
    clustering_data,
    validate_and_deduplicate
)
import pandas as pd

# ============================================================================
# STEP 1: LOAD DATA
# ============================================================================

print("="*60)
print("STEP 1: Loading data...")
print("="*60)
path_of_data = "./scraping_10feb_cleaned.xlsx"
df = pd.read_excel(path_of_data)
print(f"✅ Data loaded: {len(df)} rows")
print(f"Columns: {list(df.columns)}")
print()

# ============================================================================
# STEP 2: JALANKAN PIPELINE LENGKAP
# ============================================================================

print("="*60)
print("STEP 2: Menjalankan pipeline lengkap...")
print("="*60)
df_result = process_complete_pipeline(df)
print(f"✅ Pipeline selesai: {len(df_result)} rows")
print()

# ============================================================================
# STEP 3: GENERATE DAN PRINT REPORT
# ============================================================================

print("="*60)
print("STEP 3: Generating report...")
print("="*60)
report = generate_report(df_result)
print_report(report)

# ============================================================================
# STEP 4: SIMPAN HASIL
# ============================================================================

print("\n" + "="*60)
print("STEP 4: Menyimpan hasil...")
print("="*60)
df_result.to_excel("hasil_clustering_final.xlsx", index=False)
print("✅ Saved to: hasil_clustering_final.xlsx")

df_result.to_csv("hasil_clustering_final.csv", index=False)
print("✅ Saved to: hasil_clustering_final.csv")

# ============================================================================
# STEP 5: FILTER DATA BERKUALITAS TINGGI
# ============================================================================

print("\n" + "="*60)
print("STEP 5: Filtering data berkualitas tinggi...")
print("="*60)

# Hanya data yang ditemukan di Surabaya
df_surabaya = df_result[
    (df_result["Validasi"] == "Ditemukan") &
    (df_result["status_lokasi"] == "disurabaya") &
    (df_result["is_winner"] == True)
]
print(f"✅ Data berkualitas tinggi (Ditemukan di Surabaya): {len(df_surabaya)} rows")

# Data yang perlu review manual
df_review = df_result[df_result["Validasi"] == "Perlu Review"]
print(f"⚠️  Data perlu review manual: {len(df_review)} rows")

# Data di luar Surabaya
df_outside = df_result[df_result["status_lokasi"] == "tidak disurabaya"]
print(f"📍 Data di luar Surabaya: {len(df_outside)} rows")

# Data dengan error koordinat
df_error = df_result[df_result["status_lokasi"] == "error_koordinat"]
print(f"❌ Data dengan error koordinat: {len(df_error)} rows")

print("\n" + "="*60)
print("✅ SEMUA PROSES SELESAI!")
print("="*60)
print("\nFile output:")
print("  - hasil_clustering_final.xlsx")
print("  - hasil_clustering_final.csv")
print("\nSilakan cek file hasil untuk melihat data yang sudah dicluster!")
