import pandas as pd

file_path = 'data/2019-Nov.csv'

print(f"Reading data from {file_path}...")

df = pd.read_csv(file_path, nrows=5)

print("--- 🔍 5 DÒNG DỮ LIỆU ĐẦU TIÊN ---")
print(df.to_string())
print("\n")

# 2. In ra thông tin các cột (Kiểu dữ liệu)
print("--- 📊 CẤU TRÚC CÁC CỘT (SCHEMA) ---")
print(df.dtypes)

