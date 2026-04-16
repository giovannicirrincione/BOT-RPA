#!/bin/bash
echo "============================================"
echo " Setup: Bot RPA - Hojas de Ruta"
echo "============================================"

python -m venv .venv
source .venv/Scripts/activate

pip install --upgrade pip
pip install -r requirements.txt

python -m playwright install chromium

echo ""
echo "============================================"
echo " Setup completado."
echo " 1. Copiá .env.example a .env y configurá las variables."
echo " 2. Ejecutá el schema SQL en Supabase (supabase_schema.sql)."
echo " 3. Iniciá la app con: streamlit run frontend/app.py"
echo "============================================"
