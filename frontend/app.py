"""
Interfaz Streamlit para el Bot RPA de Hojas de Ruta.
"""
import os
import sys
import threading
import queue
from datetime import datetime

import pandas as pd
import streamlit as st

# Asegurar que el root del proyecto esté en el path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from bot.rpa_bot import run_bot
from bot.db import fetch_history

# ─── Configuración de página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Bot RPA — Hojas de Ruta",
    page_icon="🚛",
    layout="wide",
)

st.title("BOT HR | Automatización de Rutas")
st.caption("Terminal de control para sincronización de datos · VisionBlo Engine")

# ─── Estilos Personalizados (CSS Premium) ─────────────────────────────────────
custom_css = """
<style>
/* Ocultar barra superior y marca de agua de Streamlit para un efecto de APP Nativa */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Importar tipografía moderna corporativa */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
}

/* Ajustes de margen superior */
.block-container {
    padding-top: 2.5rem !important;
    padding-bottom: 2rem !important;
}

/* Títulos más pulcros */
h1, h2, h3 {
    font-weight: 700 !important;
    letter-spacing: -0.5px !important;
}

/* Etiquetas seleccionadas del multiselect (Azul Marino Ejecutivo) */
span[data-baseweb="tag"] {
    background-color: #1e3a8a !important; 
    color: #eff6ff !important;
    border-radius: 6px !important;
    padding: 0px 10px !important;
    font-weight: 500 !important;
    border: 1px solid #1e40af !important;
}

/* Botón principal (Iniciar Bot) Verde Eléctrico Moderno con Efecto Glass y 3D */
div.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    letter-spacing: 0.5px !important;
    box-shadow: 0 4px 6px -1px rgba(16, 185, 129, 0.25), 0 2px 4px -1px rgba(16, 185, 129, 0.1) !important;
    transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
}
div.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #059669 0%, #047857 100%) !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 12px -2px rgba(16, 185, 129, 0.35), 0 4px 6px -1px rgba(16, 185, 129, 0.2) !important;
}
div.stButton > button[kind="primary"]:active {
    transform: translateY(0) !important;
    box-shadow: 0 2px 3px -1px rgba(16, 185, 129, 0.25) !important;
}

/* Cajas de alerta y dataframe redondeadas */
.stAlert {
    border-radius: 8px !important;
    border: 1px solid rgba(255,255,255,0.05) !important;
}
div[data-testid="stDataFrame"] {
    border-radius: 10px !important;
    overflow: hidden !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# ─── Estado de sesión ─────────────────────────────────────────────────────────
if "bot_running"   not in st.session_state:
    st.session_state.bot_running   = False
if "status_log"    not in st.session_state:
    st.session_state.status_log    = []          # lista de (mensaje, nivel)
if "bot_result"    not in st.session_state:
    st.session_state.bot_result    = None
if "status_queue"  not in st.session_state:
    st.session_state.status_queue  = queue.Queue()

# ─── Sidebar: configuración ───────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuración")
    headless = st.toggle("Modo headless (sin ventana)", value=False)
    st.divider()
    st.markdown(
        "**Variables de entorno requeridas:**\n"
        "- `BOT_URL`\n- `BOT_USER`\n- `BOT_PASSWORD`\n"
        "- `SUPABASE_URL`\n- `SUPABASE_KEY`"
    )

# ─── Layout principal ─────────────────────────────────────────────────────────
col_upload, col_status = st.columns([1, 1], gap="large")

# ── Columna izquierda: carga del archivo ──────────────────────────────────────
with col_upload:
    st.subheader("1. Subir archivo Excel")
    uploaded_file = st.file_uploader(
        "Seleccioná el archivo .xlsx con las Hojas de Ruta",
        type=["xlsx"],
        disabled=st.session_state.bot_running,
    )

    gestor_col = None
    selected_gestores = []
    depositos_gestores = {}
    
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine="openpyxl")
            
            # Autodetección de cabeceras: Si el sistema de exportación dejó títulos arriba (ej. Unnamed: 0)
            if any("unnamed" in str(c).lower() for c in df.columns):
                for idx, row in df.head(10).iterrows():
                    row_strs = [str(x).lower() for x in row.values]
                    if any("gestor" in x or "latitud" in x for x in row_strs):
                        df.columns = row.values
                        df = df.iloc[idx + 1:].reset_index(drop=True)
                        break

            # Normalizar nombres para buscar el gestor
            norm_cols = [str(c).strip().lower().replace(" ", "_").replace("°", "") for c in df.columns]
            df.columns = norm_cols
            
            gestor_col = next((c for c in norm_cols if "gestor" in c), None)
            
            st.success(f"Archivo cargado: **{uploaded_file.name}** — Total filas: {len(df)}")
            
            if gestor_col:
                gestores_disponibles = df[gestor_col].dropna().unique().tolist()
                selected_gestores = st.multiselect(
                    "Seleccioná los Gestores a procesar (1 pestaña de navegador por cada uno):",
                    options=gestores_disponibles,
                    default=gestores_disponibles,
                    disabled=st.session_state.bot_running
                )
                
                if selected_gestores:
                    st.markdown("<br><h5>🏭 Asignación de Depósitos</h5>", unsafe_allow_html=True)
                    cols = st.columns(min(len(selected_gestores), 3) or 1)
                    for i, gestor in enumerate(selected_gestores):
                        with cols[i % len(cols)]:
                            depositos_gestores[gestor] = st.selectbox(
                                f"Depósito para {gestor.split(' ')[0]}",
                                ["General", "Las Heras"],
                                key=f"depo_{gestor}",
                                disabled=st.session_state.bot_running
                            )
            else:
                st.warning("⚠️ No se detectó la columna 'Gestor'. Se procesará todo el archivo como una sola Hoja de Ruta.")
            
            st.caption("Vista previa (primeras 5 filas):")
            st.dataframe(df.head(), use_container_width=True)
            uploaded_file.seek(0)  # resetear para lectura posterior
        except Exception as exc:
            st.error(f"No se pudo leer el archivo: {exc}")

    st.divider()
    st.subheader("2. Iniciar proceso")

    start_btn = st.button(
        "▶ Iniciar Bot",
        type="primary",
        disabled=bool(st.session_state.bot_running or uploaded_file is None or (gestor_col and len(selected_gestores) == 0)),
        use_container_width=True,
    )

# ── Columna derecha: estado del bot ───────────────────────────────────────────
with col_status:
    st.subheader("3. Estado del Bot")
    status_placeholder = st.empty()
    log_placeholder    = st.empty()

# ─── Lógica de ejecución del bot ─────────────────────────────────────────────
LEVEL_ICON = {
    "info":    "ℹ️",
    "success": "✅",
    "error":   "❌",
    "warning": "⚠️",
}
LEVEL_COLOR = {
    "info":    "#1e90ff",
    "success": "#28a745",
    "error":   "#dc3545",
    "warning": "#fd7e14",
}


def render_log(log: list[tuple[str, str]]) -> str:
    lines = []
    for msg, level in reversed(log[-30:]):  # últimos 30 mensajes, más reciente arriba
        icon  = LEVEL_ICON.get(level, "•")
        color = LEVEL_COLOR.get(level, "#333")
        lines.append(
            f'<div style="margin:2px 0; color:{color}; font-size:0.9rem;">'
            f'{icon} {msg}</div>'
        )
    return '<div style="max-height:340px; overflow-y:auto;">' + "\n".join(lines) + "</div>"


def _run_bot_thread(excel_path: str, q: queue.Queue, headless: bool, selected_gestores: list[str], depositos: dict) -> None:
    """Ejecuta el bot en un hilo separado y pone los estados en la queue."""
    def cb(msg: str, level: str = "info") -> None:
        q.put(("status", msg, level))

    completed = False
    def early_done(result: dict) -> None:
        nonlocal completed
        if not completed:
            q.put(("done", result))
            completed = True

    try:
        # Nota: Puedes actualizar la firma de run_bot más tarde para aceptar depositos si es requerido en playwright
        result = run_bot(
            excel_path, 
            selected_gestores=selected_gestores,
            depositos_gestores=depositos,
            on_status=cb, 
            headless=headless, 
            on_success_callback=early_done
        )
        if not completed:
            q.put(("done", result))
    except Exception as exc:
        if not completed:
            q.put(("done", {"success": False, "error": str(exc)}))


if start_btn and uploaded_file and not st.session_state.bot_running:
    # Guardar el archivo con su nombre ORIGINAL en la carpeta data/
    # para que el log y Supabase muestren el nombre real
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)
    original_name = uploaded_file.name                          # ej. "hoja_ruta_abril.xlsx"
    tmp_path = os.path.join(data_dir, original_name)
    with open(tmp_path, "wb") as f:
        f.write(uploaded_file.read())

    # Limpiar estado anterior
    st.session_state.status_log   = []
    st.session_state.bot_result   = None
    st.session_state.bot_running  = True
    st.session_state.status_queue = queue.Queue()

    thread = threading.Thread(
        target=_run_bot_thread,
        args=(tmp_path, st.session_state.status_queue, headless, selected_gestores, depositos_gestores),
        daemon=True,
    )
    thread.start()
    st.rerun()


# ── Polling del estado mientras el bot corre ──────────────────────────────────
if st.session_state.bot_running:
    with col_status:
        status_placeholder.info("🤖 **El bot está trabajando...**")

    # Drenar la queue en este ciclo de render
    q: queue.Queue = st.session_state.status_queue
    try:
        while True:
            item = q.get_nowait()
            if item[0] == "status":
                _, msg, level = item
                st.session_state.status_log.append((msg, level))
            elif item[0] == "done":
                _, result = item
                st.session_state.bot_result  = result
                st.session_state.bot_running = False
    except queue.Empty:
        pass

    # Renderizar log actualizado
    with col_status:
        log_placeholder.markdown(
            render_log(st.session_state.status_log), unsafe_allow_html=True
        )

    if st.session_state.bot_running:
        # Re-renderizar cada 1.5 segundos mientras el bot trabaja
        time_placeholder = st.empty()
        import time
        time.sleep(1.5)
        st.rerun()


# ── Resultado final ───────────────────────────────────────────────────────────
if not st.session_state.bot_running and st.session_state.bot_result is not None:
    result = st.session_state.bot_result

    with col_status:
        if result.get("success"):
            status_placeholder.success(
                f"✅ **Proceso completado con éxito**\n\n"
                f"- Filas procesadas: **{result.get('total_rows', '—')}**\n"
                f"- Exitosas: **{result.get('success_rows', '—')}**\n"
                f"- Con error: **{result.get('error_rows', 0)}**"
            )
        elif result.get("error_rows", 0) > 0:
            status_placeholder.warning(
                f"⚠️ **Proceso finalizado con errores parciales**\n\n"
                f"- Filas procesadas: **{result.get('total_rows', '—')}**\n"
                f"- Exitosas: **{result.get('success_rows', 0)}**\n"
                f"- Con error: **{result.get('error_rows', '—')}**\n\n"
                f"Revisá el historial para más detalles."
            )
        else:
            status_placeholder.error(
                f"❌ **El proceso falló**\n\n{result.get('error', 'Error desconocido')}"
            )

        log_placeholder.markdown(
            render_log(st.session_state.status_log), unsafe_allow_html=True
        )


# ── Log persistente si no está corriendo ─────────────────────────────────────
elif not st.session_state.bot_running and st.session_state.status_log:
    with col_status:
        log_placeholder.markdown(
            render_log(st.session_state.status_log), unsafe_allow_html=True
        )

# ─── Historial de cargas ──────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Historial de Cargas")

refresh_col, _ = st.columns([1, 5])
with refresh_col:
    if st.button("🔄 Actualizar historial"):
        st.rerun()

try:
    history = fetch_history()
    if history:
        hist_df = pd.DataFrame(history)

        # Formatear columnas para visualización
        col_map = {
            "id":           "ID",
            "filename":     "Archivo",
            "uploaded_at":  "Fecha y Hora",
            "total_rows":   "Total Filas",
            "success_rows": "Exitosas",
            "error_rows":   "Con Error",
            "status":       "Estado",
            "error_detail": "Detalle Error",
        }
        hist_df = hist_df.rename(columns={k: v for k, v in col_map.items() if k in hist_df.columns})

        if "Fecha y Hora" in hist_df.columns:
            try:
                dt_series = pd.to_datetime(hist_df["Fecha y Hora"])
                # Si viene sin zona horaria, asumir UTC (estado base de Supabase)
                if dt_series.dt.tz is None:
                    dt_series = dt_series.dt.tz_localize("UTC")
                # Convertir a hora de Argentina (GMT-3)
                dt_series = dt_series.dt.tz_convert("America/Argentina/Buenos_Aires")
                hist_df["Fecha y Hora"] = dt_series.dt.strftime("%d/%m/%Y %H:%M:%S")
            except Exception:
                pass

        def _color_status(val: str) -> str:
            colors = {"Éxito": "color: green", "Error": "color: red", "Procesando": "color: orange"}
            return colors.get(val, "")

        styled = hist_df.style.map(_color_status, subset=["Estado"] if "Estado" in hist_df.columns else [])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("No hay registros de cargas anteriores.")
except Exception as exc:
    st.warning(f"No se pudo cargar el historial: {exc}")
    st.caption("Verificá las variables de entorno SUPABASE_URL y SUPABASE_KEY.")
