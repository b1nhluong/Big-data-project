#!/usr/bin/env bash
# [TV1] Tải dữ liệu thật REES46 (10–11/2019, ~110 triệu dòng, 14,8 GB CSV) từ Kaggle.
# Cần: pip install kaggle; tạo API token tại kaggle.com -> Settings -> API -> ~/.kaggle/kaggle.json
# 5 tháng còn lại (12/2019–4/2020): https://rees46.com/en/datasets (link Google Drive).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw
kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -p data/raw --unzip
ls -lh data/raw
