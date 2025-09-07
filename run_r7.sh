#!/usr/bin/env bash
set -e

# 환경변수 불러오기
source ~/.bashrc

echo "[R7] 의존성 설치 확인"
pip install -r requirements.txt

echo "[R7] 파이프라인 실행 시작"
python kis_r7_pro_v4_2.py --top_k 60 --limit 1200
