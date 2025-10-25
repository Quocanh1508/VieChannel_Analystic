import pandas as pd
import os
from sqlalchemy import create_engine
import glob

# --- 1. Cấu hình kết nối ---
# Lấy thông tin từ file docker-compose.yml
DB_USER = "dbt_user"
DB_PASSWORD = "dbt_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "youtube_raw_db"
DB_SCHEMA = "public" # Schema mà dbt sẽ đọc

# Tạo chuỗi kết nối (connection string)
# Cú pháp: postgresql://user:password@host:port/dbname
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Tạo engine kết nối
engine = create_engine(DATABASE_URL)

# --- 2. Định nghĩa thư mục CSV ---
# Thư mục chứa các file data thô
DATA_DIR = "data"

# --- 3. Đọc và Tải từng file CSV ---
def load_csv_to_postgres(csv_file_path, table_name, db_engine, schema_name):
    """
    Hàm đọc file CSV và tải lên Postgres.
    if_exists='replace': Xóa bảng cũ (nếu có) và tạo bảng mới -> tiện cho việc chạy lại.
    """
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Đã đọc file {csv_file_path}, có {len(df)} dòng.")
        
        # Tải DataFrame lên Postgres
        df.to_sql(
            table_name, 
            db_engine, 
            schema=schema_name, 
            if_exists="replace", # 'replace' để chạy lại script dễ dàng
            index=False         # Không lưu index của pandas
        )
        print(f"Tải thành công vào bảng: {schema_name}.{table_name}\n")
    except Exception as e:
        print(f"Lỗi khi xử lý file {csv_file_path}: {e}\n")

# --- 4. Chạy chính ---
if __name__ == "__main__":
    print("--- Bắt đầu quá trình LOAD dữ liệu thô vào Postgres ---")

    # Tự động tìm tất cả các file .csv trong thư mục 'data'
    all_csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    if not all_csv_files:
        print(f"KHÔNG TÌM THẤY file CSV nào trong thư mục '{DATA_DIR}'.")
        print("Hãy chắc chắn bạn đã chạy script get-data và file được lưu vào 'data/'.")
    else:
        for csv_file in all_csv_files:
            # Lấy tên file (ví dụ: 'channels') làm tên bảng
            # 'data\\channels.csv' -> 'channels'
            table_name = os.path.basename(csv_file).replace(".csv", "")
            
            # Chúng ta muốn tên bảng là 'raw_...' để dbt dễ nhận biết
            raw_table_name = f"raw_{table_name}"
            
            load_csv_to_postgres(csv_file, raw_table_name, engine, DB_SCHEMA)

    print("--- Hoàn tất quá trình LOAD dữ liệu ---")