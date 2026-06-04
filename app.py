import os
import boto3
import xgboost as xgb
import pandas as pd
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================================
# 1. KHỞI TẠO API & CẤU HÌNH BẢO MẬT (CORS)
# =========================================================================
app = FastAPI(
    title="Ecommerce Real-time Purchase Prediction API",
    description="API dự đoán khả năng mua hàng của người dùng dựa trên Session Features.",
    version="1.0.0"
)

# Cho phép các ứng dụng Web Frontend gọi API mà không bị chặn CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================================
# 2. TẢI VÀ NẠP MÔ HÌNH TỪ MINIO DATA LAKE (Chỉ chạy 1 lần khi Start App)
# =========================================================================
MODEL_FILE_NAME = "model_cache.json"
model = xgb.Booster()

@app.on_event("startup")
def load_model_from_minio():
    logger.info("⏳ Đang kết nối S3 để tải mô hình từ thư mục 'models'...")
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
        )
        
        minio_bucket = "ecommerce-datalake"
        minio_key = "models/xgboost_session_model.json"
        
        # Tải file từ MinIO về ổ cứng của API Server
        s3_client.download_file(minio_bucket, minio_key, MODEL_FILE_NAME)
        logger.info(f"✅ Đã tải file vật lý {MODEL_FILE_NAME} thành công!")
        
        # Nạp thẳng vào RAM (Memory)
        model.load_model(MODEL_FILE_NAME)
        logger.info("🚀 Lõi XGBoost đã được nạp vào RAM. API sẵn sàng nhận Request!")
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi tải mô hình từ MinIO: {str(e)}")
        raise RuntimeError("Không thể nạp mô hình, dừng khởi động API.")

# =========================================================================
# 3. ĐỊNH NGHĨA CẤU TRÚC DỮ LIỆU ĐẦU VÀO (DATA SCHEMA)
# =========================================================================
class SessionFeatures(BaseModel):
    total_views: int = Field(..., description="Tổng số lượt xem sản phẩm")
    total_carts: int = Field(..., description="Tổng số lần thêm vào giỏ hàng")
    unique_categories: int = Field(..., description="Số lượng danh mục sản phẩm khác nhau đã xem")
    unique_brands: int = Field(..., description="Số lượng thương hiệu khác nhau đã xem")
    session_duration: int = Field(..., description="Thời lượng phiên truy cập tính bằng giây")
    hour_of_day: int = Field(..., description="Giờ bắt đầu phiên (0-23)")
    cart_to_view_ratio: float = Field(..., description="Tỷ lệ Thêm vào giỏ / Xem (Đã clip max 10.0)")

    class Config:
        schema_extra = {
            "example": {
                "total_views": 15,
                "total_carts": 2,
                "unique_categories": 3,
                "unique_brands": 2,
                "session_duration": 450,
                "hour_of_day": 14,
                "cart_to_view_ratio": 0.133
            }
        }

# =========================================================================
# 4. ĐỊNH NGHĨA CÁC ENDPOINTS (ROUTES)
# =========================================================================
@app.get("/health", tags=["System"])
def health_check():
    """Kiểm tra trạng thái sống còn của hệ thống."""
    return {"status": "healthy", "model_loaded": True}

@app.post("/predict", tags=["Machine Learning"])
def predict_purchase(features: SessionFeatures):
    """Nhận tín hiệu hành vi và dự đoán tỷ lệ mua hàng (Real-time)."""
    try:
        # 1. Biến đổi dữ liệu JSON thành DataFrame (chỉ 1 dòng)
        df = pd.DataFrame([features.dict()])
        
        # 2. Đưa vào ma trận DMatrix của XGBoost
        dmatrix = xgb.DMatrix(df)
        
        # 3. Chạy suy luận (Inference)
        prediction = model.predict(dmatrix)
        buy_prob = float(prediction[0])
        
        # 4. Đóng gói kết quả trả về cho Frontend
        return {
            "status": "success",
            "buy_probability": round(buy_prob, 4),
            "will_buy": bool(buy_prob >= 0.5), # Ngưỡng quyết định (Threshold)
            "recommendation": "Gửi Voucher giảm 10%" if buy_prob >= 0.5 else "Hiển thị quảng cáo nhắc nhở"
        }
        
    except Exception as e:
        logger.error(f"Lỗi suy luận: {str(e)}")
        raise HTTPException(status_code=500, detail="Lỗi nội bộ khi chạy dự đoán.")
    
