# ใช้ Image หลักของ Airflow เวอร์ชัน 2.8.1 และระบุ Python 3.10
FROM apache/airflow:2.8.1-python3.10

# คัดลอกไฟล์ requirements.txt เข้าไปใน Container
COPY requirements.txt /

# สั่งอัปเดต pip และติดตั้งไลบรารีเพิ่มเติม
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt