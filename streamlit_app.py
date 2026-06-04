import streamlit as st
import requests
import pandas as pd

# =========================================================================
# 1. CẤU HÌNH TRANG & KẾT NỐI API
# =========================================================================
st.set_page_config(
    page_title="Hệ Thống Dự Đoán Mua Hàng",
    page_icon="🛒",
    layout="wide"
)

API_URL = "http://localhost:8000/predict"

st.title("🛒 Bảng Điều Khiển Dự Đoán Chuyển Đổi Khách Hàng (Real-time)")
st.markdown("""
    Nhập hành vi của người dùng trong phiên truy cập (Session) hiện tại. 
    Hệ thống sẽ gọi API FastAPI + XGBoost để đánh giá xác suất chốt đơn của khách hàng này.
""")

# =========================================================================
# 2. GIAO DIỆN NHẬP DỮ LIỆU (SIDEBAR & MAIN AREA)
# =========================================================================
st.sidebar.header("⚙️ Nhập Hành Vi Khách Hàng")

with st.sidebar.form(key='behavior_form'):
    total_views = st.number_input("👁️ Tổng số lượt xem (Views)", min_value=0, max_value=500, value=15)
    total_carts = st.number_input("🛍️ Số lần Thêm vào giỏ (Carts)", min_value=0, max_value=100, value=2)
    
    unique_categories = st.slider("📁 Số Danh mục đã xem", min_value=1, max_value=20, value=3)
    unique_brands = st.slider("🏷️ Số Thương hiệu đã xem", min_value=1, max_value=20, value=2)
    
    session_duration = st.number_input("⏱️ Thời lượng phiên (Giây)", min_value=0, max_value=10000, value=450)
    hour_of_day = st.slider("🕒 Giờ truy cập trong ngày", min_value=0, max_value=23, value=14)
    
    submit_button = st.form_submit_button(label='🚀 Phân Tích & Dự Đoán')

# =========================================================================
# 3. XỬ LÝ LOGIC VÀ GỌI API FASTAPI
# =========================================================================
if submit_button:
    calculated_ratio = total_carts / (total_views + 1e-5)
    cart_to_view_ratio = min(calculated_ratio, 10.0) # Clip max 10.0
    
    payload = {
        "total_views": total_views,
        "total_carts": total_carts,
        "unique_categories": unique_categories,
        "unique_brands": unique_brands,
        "session_duration": session_duration,
        "hour_of_day": hour_of_day,
        "cart_to_view_ratio": float(cart_to_view_ratio)
    }
    
    with st.spinner('Đang kết nối tới Cụm máy chủ XGBoost...'):
        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                buy_prob = result.get("buy_probability", 0.0)
                will_buy = result.get("will_buy", False)
                recommendation = result.get("recommendation", "")
                
                st.subheader("📊 Kết Quả Phân Tích")
                
                col1, col2, col3 = st.columns(3)
                
                # Cột 1: Hiển thị Tỷ lệ mua
                with col1:
                    st.metric(
                        label="Xác suất chốt đơn", 
                        value=f"{buy_prob * 100:.2f}%",
                        delta="Khả năng Cao" if will_buy else "Khả năng Thấp",
                        delta_color="normal" if will_buy else "inverse"
                    )
                
                # Cột 2 & 3: Hiển thị Cảnh báo & Hành động
                with col2:
                    if will_buy:
                        st.success(f"**Trạng thái:** Khách hàng RẤT TIỀM NĂNG 🎯")
                    else:
                        st.warning(f"**Trạng thái:** Khách hàng CHƯA SẴN SÀNG 💤")
                        
                with col3:
                    st.info(f"**Hành động Đề xuất:** \n\n👉 {recommendation}")
                
                # Hiển thị lại bảng Data gửi đi để minh bạch
                st.markdown("---")
                st.write("🕵️‍♂️ *Dữ liệu đã gửi tới Mô hình:*")
                st.dataframe(pd.DataFrame([payload]))
                
            else:
                st.error(f"Lỗi từ API: Mã {response.status_code} - {response.text}")
                
        except requests.exceptions.ConnectionError:
            st.error("🚨 Không thể kết nối tới máy chủ FastAPI! Bác đã chạy lệnh `uvicorn app:app` ở một Terminal khác chưa?")
        except Exception as e:
            st.error(f"Lỗi không xác định: {str(e)}")
else:
    st.info("👈 Hãy điều chỉnh các thông số hành vi ở thanh bên trái và bấm 'Phân Tích & Dự Đoán' để bắt đầu.")
