FROM python:3.12-slim

# -- 环境变量 --------------------------------------------------
# 禁用 .pyc 字节码生成，减少磁盘占用
# 禁用 Python 输出缓冲，保证 docker logs 实时可见
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app

# -- 系统依赖 --------------------------------------------------
# slim 镜像仅需 tzdata 保证时区准确
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# -- 非 root 用户（安全加固）------------------------------------
RUN useradd -u 1000 -m appuser && chown -R appuser:appuser /app

# -- Python 依赖（利用 Docker 层缓存）---------------------------
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -- 项目文件 --------------------------------------------------
COPY --chown=appuser:appuser . .

USER appuser

# 默认启动调度器
CMD ["python", "scheduler.py"]