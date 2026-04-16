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
import subprocess

# ─── Inicialización de Playwright para Cloud ──────────────────────────────────
def _install_browsers():
    """Instala Chromium si no está presente (específico para Streamlit Cloud)."""
    try:
        # Correr el comando de instalación
        subprocess.run(["playwright", "install", "chromium"], check=True)
    except Exception as e:
        st.error(f"Error al instalar navegadores de Playwright: {e}")

if st.secrets.get("DEPLOY_ENV") == "streamlit":
    # Solo ejecutar en el cloud para no ralentizar el desarrollo local
    if "browsers_installed" not in st.session_state:
        with st.spinner("Preparando entorno del bot..."):
            _install_browsers()
            st.session_state.browsers_installed = True

# Asegurar que el root del proyecto esté en el path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from bot.rpa_bot import run_bot
from bot.db import fetch_history

# ─── Configuración de página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="BOT HR",
    page_icon="❖",
    layout="wide",
)

st.title("BOT HR | Automatización de Rutas")
st.caption("Terminal de control para sincronización de datos · VisionBlo Engine")

# ─── Estilos Personalizados (CSS SaaS) ────────────────────────────────────────
custom_css = """
<style>
/* Ocultar elementos de Streamlit */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Tipografía global */
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"], [data-testid="stMarkdownContainer"] {
    font-family: 'Geist', 'Inter', sans-serif !important;
}

/* Reducir espacio superior */
.block-container {
    padding-top: 2rem !important;
    padding-bottom: 2rem !important;
}

/* Títulos y textos */
h1, h2, h3 {
    font-weight: 600 !important;
    letter-spacing: -0.02em !important;
    color: #111827;
}
p {
    color: #4b5563;
    font-size: 0.95rem;
}

/* Tarjetas (Cards) estilo SaaS aplicadas a columnas centrales */
[data-testid="column"] {
    background-color: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 1.5rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

/* Etiquetas seleccionadas del multiselect */
span[data-baseweb="tag"] {
    background-color: #f3f4f6 !important; 
    color: #111827 !important;
    border-radius: 6px !important;
    padding: 0px 10px !important;
    font-weight: 500 !important;
    border: 1px solid #e5e7eb !important;
}

/* Botón Iniciar Bot */
div.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #10b981 0%, #047857 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.6rem 1.2rem !important;
    font-weight: 500 !important;
    box-shadow: 0 4px 14px 0 rgba(16, 185, 129, 0.39) !important;
    transition: all 0.2s ease !important;
}
div.stButton > button[kind="primary"]:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(16, 185, 129, 0.23) !important;
}

/* Logs Box */
.log-box {
    background-color: #f9fafb;
    border: 1px solid #f3f4f6;
    border-radius: 8px;
    padding: 12px;
    max-height: 400px;
    overflow-y: auto;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    line-height: 1.5;
    margin-top: 1rem;
}
.log-line { margin-bottom: 6px; display: flex; gap: 8px; align-items: start; }
.log-msg { color: #374151; }

/* KPIs */
.kpi-container {
    display: flex;
    gap: 1.5rem;
    margin: 1rem 0;
    padding: 1rem;
    background: #fdfdfd;
    border: 1px solid #f3f4f6;
    border-radius: 8px;
}
.kpi-box { display: flex; flex-direction: column; }
.kpi-value { font-size: 1.8rem; font-weight: 700; color: #111827; line-height: 1.2; margin-bottom: 0.2rem; }
.kpi-success { color: #10b981; }
.kpi-error { color: #ef4444; }
.kpi-label { font-size: 0.8rem; color: #6b7280; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; }

/* Tablas SaaS */
.saas-table-wrapper {
    overflow-x: auto;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    background: #fff;
    margin-top: 1rem;
}
.saas-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    text-align: left;
}
.saas-table th {
    padding: 12px 16px;
    border-bottom: 1px solid #e5e7eb;
    background-color: #f9fafb;
    color: #6b7280;
    font-weight: 500;
}
.saas-table td {
    padding: 12px 16px;
    border-bottom: 1px solid #f3f4f6;
    color: #374151;
}
.saas-table tr:last-child td { border-bottom: none; }

/* Badges */
.badge {
    padding: 4px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 500;
    display: inline-block;
}
.badge-success { background: #dcfce7; color: #166534; }
.badge-error { background: #fee2e2; color: #991b1b; }
.badge-warning { background: #fef9c3; color: #854d0e; }

/* MODO OSCURO */
@media (prefers-color-scheme: dark) {
    h1, h2, h3 { color: #f9fafb; }
    p { color: #9ca3af; }
    [data-testid="column"] { background-color: #111827; border-color: #374151; }
    .log-box { background-color: #030712; border-color: #1f2937; }
    .log-msg { color: #d1d5db; }
    .kpi-container { background: #111827; border-color: #374151; }
    .kpi-value { color: #f9fafb; }
    .kpi-label { color: #9ca3af; }
    .saas-table-wrapper { border-color: #374151; background: #111827; }
    .saas-table th { background-color: #1f2937; color: #9ca3af; border-color: #374151; }
    .saas-table td { color: #e5e7eb; border-color: #1f2937; }
    .badge-success { background: rgba(22, 101, 52, 0.3); color: #4ade80; }
    .badge-error { background: rgba(153, 27, 27, 0.3); color: #f87171; }
    .badge-warning { background: rgba(133, 77, 14, 0.3); color: #facc15; }
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
    st.header("Configuración")
    # En Cloud es obligatorio usar headless (sin ventana) porque no hay pantalla conectada.
    is_cloud = (st.secrets.get("DEPLOY_ENV") == "streamlit")
    headless = st.toggle(
        "Modo headless (sin ventana)", 
        value=True, 
        disabled=is_cloud,
        help="En Streamlit Cloud este modo es obligatorio." if is_cloud else "Localmente permite ver el navegador."
    )
    if is_cloud:
        headless = True
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
    st.subheader("Subida de Archivos")
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
                            st.markdown(f"**{gestor.split(' ')[0]}**")
                            origen = st.selectbox(
                                "Origen",
                                ["General", "Las Heras", "Maipu"],
                                key=f"origen_{gestor}",
                                disabled=st.session_state.bot_running
                            )
                            destino = st.selectbox(
                                "Destino",
                                ["General", "Las Heras", "Maipu"],
                                key=f"destino_{gestor}",
                                disabled=st.session_state.bot_running
                            )
                            depositos_gestores[gestor] = {"origen": origen, "destino": destino}
            else:
                st.warning("⚠️ No se detectó la columna 'Gestor'. Se procesará todo el archivo como una sola Hoja de Ruta.")
            
            st.caption("Vista previa (primeras 5 filas):")
            st.dataframe(df.head(), use_container_width=True)
            uploaded_file.seek(0)  # resetear para lectura posterior
        except Exception as exc:
            st.error(f"No se pudo leer el archivo: {exc}")

    st.divider()
    st.subheader("Paradas Adicionales")
    st.caption("Cargá paradas extra con sus coordenadas (ej. -32.89, -68.84) de manera independiente para cada gestor.")
    
    extra_stops_dfs = {}
    if gestor_col and selected_gestores:
        for gestor in selected_gestores:
            st.markdown(f"**Paradas para {gestor.split(' ')[0]}**")
            extra_stops_dfs[gestor] = st.data_editor(
                pd.DataFrame([{"Tipo Parada": "Estación de servicio", "Coordenadas": ""}]),
                num_rows="dynamic",
                use_container_width=True,
                key=f"stops_{gestor}"
            )
    else:
        extra_stops_dfs["Único"] = st.data_editor(
            pd.DataFrame([{"Tipo Parada": "Estación de servicio", "Coordenadas": ""}]),
            num_rows="dynamic",
            use_container_width=True,
            key="stops_unico"
        )
    
    st.markdown("<br>", unsafe_allow_html=True)

    start_btn = st.button(
        "Iniciar Bot",
        type="primary",
        disabled=bool(st.session_state.bot_running or uploaded_file is None or (gestor_col and len(selected_gestores) == 0)),
        use_container_width=True,
    )

# ── Columna derecha: estado del bot ───────────────────────────────────────────
with col_status:
    st.subheader("Estado")
    status_placeholder = st.empty()
    log_placeholder    = st.empty()

# ─── Lógica de ejecución del bot ─────────────────────────────────────────────
def render_log(log: list[tuple[str, str]]) -> str:
    lines = []
    ICON = {
        "info":    '<span style="color:#3b82f6;">●</span>',
        "success": '<span style="color:#10b981;">✓</span>',
        "error":   '<span style="color:#ef4444;">✕</span>',
        "warning": '<span style="color:#f59e0b;">▲</span>',
    }
    for msg, level in reversed(log[-50:]):
        # Limpiar posibles emojis del mensaje
        import re
        clean_msg = re.sub(r'[^\w\s,\.\-\:\/\(\)\[\]\'\"]', '', msg).strip()
        icon = ICON.get(level, "●")
        lines.append(f'<div class="log-line">{icon} <span class="log-msg">{clean_msg}</span></div>')
    return '<div class="log-box">' + "\n".join(lines) + "</div>"


def _run_bot_thread(excel_path: str, q: queue.Queue, headless: bool, selected_gestores: list[str], depositos: dict, extra_stops: dict) -> None:
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
            extra_stops=extra_stops,
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
    # Parsing paradas adicionales por gestor
    extra_stops_dict = {}
    for g, df_g in extra_stops_dfs.items():
        g_list = []
        for _, r in df_g.iterrows():
            tipo = str(r.get("Tipo Parada", "")).strip()
            coords = str(r.get("Coordenadas", "")).strip()
            if coords:
                parts = coords.split(",")
                lat = parts[0].strip() if len(parts) > 0 else ""
                lon = parts[1].strip() if len(parts) > 1 else ""
                if lat and lon:
                    g_list.append({
                        "latitud": lat, 
                        "longitud": lon, 
                        "cliente": tipo or "Parada Adicional"
                    })
        extra_stops_dict[g] = g_list

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
        args=(tmp_path, st.session_state.status_queue, headless, selected_gestores, depositos_gestores, extra_stops_dict),
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
        # Generar KPIs
        kpi_html = f"""
        <div class="kpi-container">
            <div class="kpi-box">
                <div class="kpi-value">{result.get('total_rows', '-')}</div>
                <div class="kpi-label">Filas Procesadas</div>
            </div>
            <div class="kpi-box">
                <div class="kpi-value kpi-success">{result.get('success_rows', '-')}</div>
                <div class="kpi-label">Exitosas</div>
            </div>
            <div class="kpi-box">
                <div class="kpi-value kpi-error">{result.get('error_rows', '-')}</div>
                <div class="kpi-label">Con Error</div>
            </div>
        </div>
        """
        
        if result.get("success"):
            status_placeholder.markdown("### Completado con éxito", unsafe_allow_html=True)
            status_placeholder.markdown(kpi_html, unsafe_allow_html=True)
        elif result.get("error_rows", 0) > 0:
            status_placeholder.markdown("### Finalizado con observaciones", unsafe_allow_html=True)
            status_placeholder.markdown(kpi_html, unsafe_allow_html=True)
        else:
            status_placeholder.error(
                f"Falla de ejecución: {result.get('error', 'Error desconocido')}"
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
st.subheader("Historial de Procesamiento")

refresh_col, _ = st.columns([1, 5])
with refresh_col:
    if st.button("Actualizar Historial"):
        st.rerun()

try:
    history = fetch_history()
    if history:
        hist_df = pd.DataFrame(history)

        col_map = {
            "id":           "ID",
            "filename":     "Archivo",
            "uploaded_at":  "Fecha y Hora",
            "total_rows":   "Filas",
            "success_rows": "Exitosas",
            "error_rows":   "Con Error",
            "status":       "Estado",
            "error_detail": "Detalle Error",
        }
        hist_df = hist_df.rename(columns={k: v for k, v in col_map.items() if k in hist_df.columns})

        if "Fecha y Hora" in hist_df.columns:
            try:
                dt_series = pd.to_datetime(hist_df["Fecha y Hora"])
                if dt_series.dt.tz is None:
                    dt_series = dt_series.dt.tz_localize("UTC")
                dt_series = dt_series.dt.tz_convert("America/Argentina/Buenos_Aires")
                hist_df["Fecha y Hora"] = dt_series.dt.strftime("%d/%m/%Y %H:%M")
            except Exception:
                pass

        def render_badge(val):
            if val == 'Éxito': return '<span class="badge badge-success">Éxito</span>'
            elif val == 'Error': return '<span class="badge badge-error">Error</span>'
            return f'<span class="badge badge-warning">{val}</span>'

        if "Estado" in hist_df.columns:
            hist_df["Estado"] = hist_df["Estado"].apply(render_badge)

        html_table = hist_df.to_html(escape=False, index=False, classes="saas-table")
        st.markdown(f'<div class="saas-table-wrapper">{html_table}</div>', unsafe_allow_html=True)
    else:
        st.info("No hay registros de cargas anteriores.")
except Exception as exc:
    st.warning(f"No se pudo cargar el historial: {exc}")
    st.caption("Verificá las variables de entorno SUPABASE_URL y SUPABASE_KEY.")
