"""
Bot RPA con Playwright para cargar Hojas de Ruta.

Emite actualizaciones de estado vía un callback `on_status(msg, level)`
para que la UI de Streamlit pueda mostrar el progreso en tiempo real.
También escribe un log detallado en data/bot_log.txt para diagnóstico.
"""
import logging
import os
import sys
import time
import asyncio
import traceback
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PWTimeout

from bot.db import (
    create_history_record,
    update_history_record,
    insert_row_detail,
)

load_dotenv()

# ─── Constantes de configuración ──────────────────────────────────────────────
def _get_secret(key: str, default: str = "") -> str:
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key, default)
    except Exception:
        return os.getenv(key, default)

BOT_URL      = _get_secret("BOT_URL", "https://sistema-tercero.com/login")
BOT_USER     = _get_secret("BOT_USER", "")
BOT_PASSWORD = _get_secret("BOT_PASSWORD", "")

# Tiempo de espera máximo para selectores (ms)
DEFAULT_TIMEOUT = 30_000

StatusCallback = Callable[[str, str], None]  # (message, level: info|success|error|warning)


# ─── Logger de archivo ────────────────────────────────────────────────────────

def _setup_logger() -> logging.Logger:
    """
    Configura un logger que escribe en consola Y en data/bot_log.txt.
    Cada ejecución agrega al mismo archivo (no sobreescribe).
    """
    log_dir = Path("data")
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "bot_log.txt"

    logger = logging.getLogger("rpa_bot")
    logger.setLevel(logging.DEBUG)

    # Evitar duplicar handlers si el módulo se recarga (ej. Streamlit)
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler → archivo
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler → consola
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


log = _setup_logger()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _noop(msg: str, level: str = "info") -> None:
    try:
        print(f"[{level.upper()}] {msg}")
    except UnicodeEncodeError:
        print(f"[{level.upper()}] {msg.encode('ascii', 'ignore').decode('ascii')}")


def _emit(on_status: StatusCallback, msg: str, level: str = "info") -> None:
    """Emite el mensaje al callback de UI Y lo escribe en el log de archivo."""
    log_fn = {
        "info":    log.info,
        "success": log.info,
        "warning": log.warning,
        "error":   log.error,
    }.get(level, log.info)
    log_fn(msg)
    on_status(msg, level)


# ─── Login ────────────────────────────────────────────────────────────────────

def _login(page: Page, on_status: StatusCallback) -> None:
    """Realiza el login en el sistema."""
    _emit(on_status, f"Abriendo sistema web: {BOT_URL}", "info")
    page.goto(BOT_URL, wait_until="networkidle", timeout=30_000)
    _emit(on_status, f"Página cargada. URL actual: {page.url}", "info")

    try:
        # Detectar si el formulario de login realmente existe en la pantalla (5 segundos)
        page.wait_for_selector("input[type='password'], input[name='password']", timeout=5000)
    except PWTimeout:
        _emit(on_status, f"✅ Sesión activa detectada. No hace falta login.", "success")
        return

    _emit(on_status, f"Ingresando usuario: {BOT_USER}", "info")
    page.fill("input[name='username'], input[id='username'], input[type='text']:first-of-type", BOT_USER)
    page.fill("input[name='password'], input[id='password'], input[type='password']", BOT_PASSWORD)

    _emit(on_status, "Enviando formulario de login...", "info")
    page.click("button[type='submit'], input[type='submit']")

    try:
        page.wait_for_url(
            lambda url: "/login" not in url and "/signin" not in url,
            timeout=DEFAULT_TIMEOUT,
        )
        _emit(on_status, f"✅ Login exitoso. URL: {page.url}", "success")
    except PWTimeout:
        error_el   = page.query_selector(".error, .alert-danger, [class*='error']")
        error_text = error_el.inner_text() if error_el else "Tiempo de espera agotado"
        log.error(f"Login fallido. URL: {page.url} | Mensaje del sistema: {error_text}")
        raise RuntimeError(f"Login fallido: {error_text}")


# ─── Navegación ───────────────────────────────────────────────────────────────

def _navigate_to_form(page: Page, on_status: StatusCallback) -> None:
    """
    Navega al formulario de Hoja de Ruta siguiendo la secuencia real del menú.
    Agregamos esperas extra para estabilidad en entornos Cloud.
    """
    _emit(on_status, "Paso 1 – Haciendo clic en menú 'Utilidades' (li#menu-1-abm)...", "info")
    page.wait_for_selector("li#menu-1-abm", timeout=DEFAULT_TIMEOUT)
    page.click("li#menu-1-abm")
    
    # Pausa para permitir que el submenú se despliegue (animación)
    page.wait_for_timeout(1500)
    _emit(on_status, "Paso 1 ✅ Menú 'Utilidades' abierto.", "info")

    _emit(on_status, "Paso 2 – Buscando icono 'Hojas de ruta'...", "info")
    selector_paso2 = "div[title*='Hojas de ruta']" # Coincidencia parcial por si hay espacios o mayúsculas
    try:
        page.wait_for_selector(selector_paso2, state="visible", timeout=DEFAULT_TIMEOUT)
        page.click(selector_paso2)
    except Exception:
        # Intento alternativo por texto si el título falla
        _emit(on_status, "Aviso: Reintentando Paso 2 por texto...", "warning")
        page.locator("div").filter(has_text="Hojas de ruta").first.click(timeout=DEFAULT_TIMEOUT)

    page.wait_for_timeout(1000)
    _emit(on_status, "Paso 2 ✅ Icono presionado.", "info")

    _emit(on_status, "Paso 3 – Haciendo clic en opción final 'Hojas de ruta'...", "info")
    selector_paso3 = "li#opcion-menu-hojaderuta"
    try:
        # Intentamos esperar normal
        page.wait_for_selector(selector_paso3, state="attached", timeout=DEFAULT_TIMEOUT)
        # Forzamos el clic en el primero que encuentre, incluso si Playwright duda de su visibilidad
        page.locator(selector_paso3).first.click(force=True, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        _emit(on_status, f"Aviso: Reintento agresivo en Paso 3... ({e})", "warning")
        page.evaluate(f"document.querySelector('{selector_paso3}').click()")

    # Esperar a que el formulario principal cargue (botón Agregar) con paciencia
    _emit(on_status, "Esperando carga del formulario final...", "info")
    page.wait_for_selector("button#hojaderuta_agregar", state="visible", timeout=DEFAULT_TIMEOUT)
    _emit(on_status, f"Paso 3 ✅ Formulario cargado. URL: {page.url}", "success")


def _click_agregar_hoja(page: Page, on_status: StatusCallback) -> None:
    """Paso 4 – Hace clic en 'Agregar hoja de ruta' y espera que el formulario se abra."""
    _emit(on_status, "Paso 4 – Haciendo clic en 'Agregar hoja de ruta' (button#hojaderuta_agregar)...", "info")
    page.click("button#hojaderuta_agregar")

    _emit(on_status, "Paso 4 – Esperando que aparezca el formulario con selector de tipo...", "info")
    
    # En vez de wait_for_selector directo que agarra el primero que encuentre (puede estar oculto),
    # forzamos a que espere que haya selecciones visibles. Le damos un segundo fijo para
    # asegurarnos que el DOM terminó de agregar la nueva sección.
    page.wait_for_timeout(600)
    
    # Nos aseguramos que al menos uno de los select class="button" esté visible.
    page.locator("select.button").last.wait_for(state="attached", timeout=DEFAULT_TIMEOUT)

    # Contar cuántas filas/selects hay en el formulario
    selects = page.query_selector_all("select.button")
    _emit(on_status, f"Paso 4 ✅ Formulario abierto. Filas detectadas: {len(selects)}", "success")

    # Se extrajo la funcionalidad de "Hoy" a su propia función para forzar parada si falla

def _seleccionar_hoy(page: Page, on_status: StatusCallback) -> None:
    """Selecciona obligatoriamente la fecha 'Hoy' de la Hoja de Ruta. Si falla, el proceso aborta."""
    _emit(on_status, "Paso 4.5 – Seleccionando fecha 'Hoy' de forma estricta...", "info")
    try:
        # Apuntamos estrictamente al contenedor cal2_hojaderuta_fecha para evitar duplicados en la página
        btn_hoy = page.locator("#cal2_hojaderuta_fecha div.radio label").filter(has=page.locator("input[value='hoy']")).locator("div.value")
        btn_hoy.first.click(force=True, timeout=5000)
        _emit(on_status, "Paso 4.5 ✅ Botón 'Hoy' seleccionado.", "success")
    except Exception as e:
        _emit(on_status, f"❌ Paso 4.5 Crítico: No se pudo localizar o clickear el botón 'Hoy'.", "error")
        raise RuntimeError(f"Falla obligatoria: No se pudo seleccionar Hoy. Detalle: {str(e)}")

def _seleccionar_vehiculo(page: Page, vehiculo: str, on_status: StatusCallback) -> None:
    """Busca y selecciona el vehículo abriendo primero el combobox."""
    _emit(on_status, f"Paso 4.6 – Buscando vehículo '{vehiculo}' de forma estricta...", "info")
    try:
        # Paso 1: Abrir el desplegable clickeando el contenedor cabecera
        _emit(on_status, "  -> Abriendo lista de unidades...", "info")
        # Filtramos explícitamente por 'visible' adjuntando la pseudo-clase CSS
        span_unidad = page.locator("#crear-editar-content-hr span:has-text('Seleccione una unidad'):visible").first
        span_unidad.click(timeout=5000)
        page.wait_for_timeout(1000)

        # Paso 2: Tipear en el buscador emergente
        _emit(on_status, f"  -> Tipeando patente '{vehiculo}'...", "info")
        search_input = page.locator("input[type='search'][placeholder='Buscar...']:visible").first
        search_input.fill(vehiculo)
        page.wait_for_timeout(1000)

        # Paso 3: Esperar a que la lista se refresque y pinchar el recuadro de la patente
        _emit(on_status, "  -> Seleccionando la patente filtrada...", "info")
        option_label = page.locator(f"div.value:has-text('{vehiculo}'):visible").first
        option_label.click(force=True, timeout=5000)
        
        _emit(on_status, f"Paso 4.6 ✅ Vehículo '{vehiculo}' seleccionado.", "success")
    except Exception as e:
        _emit(on_status, f"❌ Paso 4.6 Crítico: No se pudo localizar o seleccionar el vehículo '{vehiculo}'.", "error")
        raise RuntimeError(f"Falla obligatoria: No se pudo seleccionar el Vehículo. Detalle: {str(e)}")


def _get_base_coords(depo_name: str) -> dict[str, str]:
    """Retorna las coordenadas del depósito según su nombre."""
    if depo_name == "Las Heras":
        return {"latitud": "-32.8411689", "longitud": "-68.8305377", "cliente": f"BASE ({depo_name})"}
    elif depo_name == "Maipu":
        return {"latitud": "-32.940681", "longitud": "-68.786823", "cliente": f"BASE ({depo_name})"}
    else:
        # Por defecto "General"
        return {"latitud": "-32.887850", "longitud": "-68.828539", "cliente": f"BASE (General)"}


# ─── Formato de coordenadas ───────────────────────────────────────────────────

def _format_coord(value: Any) -> str:
    """
    Convierte un valor de coordenada a string.
    El sistema las espera en formato decimal con punto.
    Ejemplo: -38.9876 -> "-38.9876"
    """
    return str(value).strip()


# ─── Carga de fila ────────────────────────────────────────────────────────────

def _fill_form_row(
    page: Page,
    row: dict[str, Any],
    row_index: int,
    on_status: StatusCallback,
    is_last_row: bool = False,
) -> None:
    """
    Completa UNA fila del formulario de Hoja de Ruta:
      - Paso 5: selecciona "Coordenadas" en el desplegable de esa fila.
      - Paso 6: carga latitud y longitud en formato "lat, lon".

    El Excel debe tener columnas: latitud  y  longitud
    (o lat / lon / latitude / longitude — se normalizan en minúsculas).
    """
    lat_raw = (
        row.get("latitud")
        or row.get("lat")
        or row.get("latitude")
        or ""
    )
    lon_raw = (
        row.get("longitud")
        or row.get("lon")
        or row.get("longitude")
        or ""
    )

    _emit(on_status, f"  Fila {row_index + 1} – Valores leídos del Excel: lat={lat_raw!r}  lon={lon_raw!r}", "info")

    if not lat_raw or not lon_raw:
        raise ValueError(
            f"Fila {row_index + 1}: latitud o longitud vacías "
            f"(lat={lat_raw!r}, lon={lon_raw!r}). "
            "Verificá los nombres de columnas del Excel."
        )

    # ── Paso 5: seleccionar "Coordenadas" en la fila actual ───────────────────
    _emit(on_status, f"  Fila {row_index + 1} – Paso 5: buscando la fila en el contenedor...", "info")
    
    # Obtener todas las filas dentro de la sección "puntos"
    rows = page.query_selector_all("div[name='puntos'] > div")
    if not rows:
        raise RuntimeError("No se encontró el contenedor de filas div[name='puntos'] > div.")
        
    if row_index >= len(rows):
        raise RuntimeError(
            f"El formulario solo tiene {len(rows)} filas, pero se necesita acceder a la fila {row_index + 1}. "
            "El botón '+' no generó la fila esperada."
        )

    current_row = rows[row_index]

    # Tomar el primer select de esa fila (el que dice "Punto" por defecto)
    target_select = current_row.query_selector("select.button")
    if not target_select:
        raise RuntimeError("No se encontró el desplegable en esta fila.")

    target_select.select_option("coords")
    _emit(on_status, f"  Fila {row_index + 1} – Paso 5 ✅ modo 'Coordenadas' seleccionado.", "info")

    # Pequeña pausa para que el sistema reemplace el <input> por los de lat/lon
    page.wait_for_timeout(400)

    # ── Paso 6: cargar las coordenadas en ese único input de la fila ──────────
    _emit(on_status, f"  Fila {row_index + 1} – Paso 6: llenando datos en el input...", "info")
    
    # Buscamos el único input de esa fila
    coord_input = current_row.query_selector("input.form-input")

    lat_fmt = _format_coord(lat_raw)
    lon_fmt = _format_coord(lon_raw)
    coordenada_completa = f"{lat_fmt},{lon_fmt}"

    if coord_input:
        coord_input.click(click_count=3)
        coord_input.type(coordenada_completa, delay=30)
        page.wait_for_timeout(300)
        coord_input.evaluate("node => node.blur()")  # Soltar foco sin saltar al campo de Hora
        page.wait_for_timeout(200)
        _emit(on_status, f"  Fila {row_index + 1} – Coordenada cargada y fijada: {coordenada_completa}", "info")
    else:
        raise RuntimeError(f"No se encontró el 'input.form-input' para la fila {row_index + 1}.")

    _emit(on_status, f"  Fila {row_index + 1} ✅ Coordenadas ({coordenada_completa}) cargadas correctamente.", "success")

    # ── Paso 7: clic en "+" para agregar la siguiente fila ────────────────────
    if not is_last_row:
        _emit(on_status, f"  Fila {row_index + 1} – Paso 7: haciendo clic en botón '+'...", "info")
        
        add_btn = current_row.query_selector("img[src*='mas_small_true']")
        if not add_btn:
            raise RuntimeError(f"Fila {row_index + 1}: no se encontró el botón '+' en esta fila.")

        add_btn.click()
        page.wait_for_timeout(400)  # pausa para que el sistema agregue la fila nueva abajo
        _emit(on_status, f"  Fila {row_index + 1} – Paso 7 ✅ Nueva fila agregada al formulario.", "info")
    else:
        _emit(on_status, f"  Fila {row_index + 1} – Paso 7 omitido: Última parada del gestor, no se agrega fila vacía extra.", "info")


def _reordenar_ruta(page: Page, on_status: StatusCallback) -> None:
    """
    Selecciona la opción "Definir automáticamente" y hace clic en el botón "Reordenar"
    al final de cargar las paradas.
    """
    _emit(on_status, "Paso 8 – Seleccionando 'Definir automáticamente'...", "info")
    try:
        radio_auto = page.locator("input[type='radio'][name='traza'][value='auto']").first
        radio_auto.evaluate("node => node.scrollIntoView()")
        radio_auto.click(force=True)
        page.wait_for_timeout(500)
        _emit(on_status, "Paso 8 ✅ 'Definir automáticamente' seleccionado.", "success")
    except Exception as e:
        _emit(on_status, "❌ Paso 8: No se pudo seleccionar 'Definir automáticamente'.", "error")
        raise RuntimeError(f"No se pudo seleccionar 'Definir auto': {e}")

    _emit(on_status, "Paso 9 – Haciendo clic en 'Reordenar'...", "info")
    try:
        btn_reord = page.locator("button.btn.--celeste:has-text('Reordenar')").first
        btn_reord.evaluate("node => node.scrollIntoView()")
        btn_reord.click(force=True)
        page.wait_for_timeout(2000) # Espera un poco más para que procese el orden
        _emit(on_status, "Paso 9 ✅ Puntos reordenados correctamente.", "success")
    except Exception as e:
        _emit(on_status, "❌ Paso 9: No se pudo hacer clic en 'Reordenar'.", "error")
        raise RuntimeError(f"No se pudo clickear 'Reordenar': {e}")

    _emit(on_status, "Paso 10 – Haciendo clic en 'Guardar'...", "info")
    try:
        btn_save = page.locator("button.btn.--rojo:has-text('Guardar')").first
        btn_save.evaluate("node => node.scrollIntoView()")
        btn_save.click(force=True)
        page.wait_for_timeout(2000)
        _emit(on_status, "Paso 10 ✅ Hoja de ruta guardada con éxito.", "success")
    except Exception as e:
        _emit(on_status, "❌ Paso 10: No se pudo hacer clic en 'Guardar'.", "error")
        raise RuntimeError(f"No se pudo clickear 'Guardar': {e}")

# ─── Función principal exportada ──────────────────────────────────────────────


def run_bot(
    excel_path: str,
    selected_gestores: list[str] = None,
    depositos_gestores: dict[str, str] = None,
    on_status: StatusCallback = _noop,
    headless: bool = False,
    extra_stops: dict = None,
    on_success_callback: Callable[[dict[str, Any]], None] = None,
) -> dict[str, Any]:
    """
    Ejecuta el bot completo para uno o múltiples gestores en pestañas separadas.
    """
    run_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(excel_path)
    
    # IMPORTANTE: Streamlit usa Tornado, que en Windows inyecta WindowSelectorEventLoopPolicy.
    # Playwright necesita ProactorEventLoop para lanzar subprocessos.
    # Parcheamos new_event_loop dinámicamente para que sync_playwright obtenga el correcto.
    original_new_event_loop = asyncio.new_event_loop
    if sys.platform == "win32":
        asyncio.new_event_loop = asyncio.ProactorEventLoop

    log.info("=" * 70)
    log.info(f"INICIO DE EJECUCIÓN  id={run_id}  archivo={filename}")
    log.info("=" * 70)

    # ── Lectura del Excel ─────────────────────────────────────────────────────
    _emit(on_status, f"📂 Leyendo archivo Excel: {filename}", "info")
    try:
        df = pd.read_excel(excel_path, engine="openpyxl")
        
        # Autodetección de cabeceras si el export dejaba filas basura arriba
        if any("unnamed" in str(c).lower() for c in df.columns):
            for idx, row in df.head(10).iterrows():
                row_strs = [str(x).lower() for x in row.values]
                if any("gestor" in x or "latitud" in x for x in row_strs):
                    df.columns = row.values
                    df = df.iloc[idx + 1:].reset_index(drop=True)
                    break
                    
        df.columns = [str(c).strip().lower().replace(" ", "_").replace("°", "") for c in df.columns]
        # REPARACIÓN CRÍTICA: Convertir celdas NaN (vacías) a strings vacíos
        # para que el cliente JSON de Supabase no falle con "Out of range float values: nan"
        df = df.fillna("")
    except Exception as exc:
        _emit(on_status, f"No se pudo leer el Excel: {exc}", "error")
        log.error(traceback.format_exc())
        return {"success": False, "error": str(exc)}

    if df.empty:
        _emit(on_status, "El archivo Excel está vacío.", "error")
        return {"success": False, "error": "Excel vacío"}

    gestor_col = next((c for c in df.columns if "gestor" in c), None)
    if gestor_col and selected_gestores:
        # Filtrar el DataFrame solo usando las filas correspondientes a los gestores seleccionados
        df = df[df[gestor_col].isin(selected_gestores)]
    else:
        # Fallback si no hay soporte, es un string estático para el groupby
        gestor_col = "dummy_gestor"
        df[gestor_col] = "Único"
        selected_gestores = ["Único"]

    # Cada gestor procesado incluye ahora 2 paradas adicionales (base origen y base destino) más extra_stops si existen
    extra_stops_count = sum(len(stops) for stops in (extra_stops or {}).values())
    total_rows   = len(df) + (len(df[gestor_col].unique()) * 2) + extra_stops_count
    success_rows = 0
    error_rows   = 0

    _emit(on_status, f"Columnas detectadas: {list(df.columns)}", "info")
    _emit(on_status, f"Total de puntos a cargar (incluyendo bases fijas): {total_rows} en {len(df[gestor_col].unique())} gestor(es).", "info")

    veh_col = next((c for c in df.columns if c in ["vehiculo", "vehículo", "patente"]), None)
    vehiculo_global = None

    # Crear registro en Supabase (estado inicial: Procesando)
    history_id = create_history_record(filename, total_rows)
    log.info(f"Registro Supabase creado: history_id={history_id}")

    try:
        with sync_playwright() as pw:
            _emit(on_status, f"Iniciando navegador Chromium (headless={headless})...", "info")
            browser = pw.chromium.launch(headless=headless, slow_mo=50)
            context: BrowserContext = browser.new_context()

            # Preparamos las agrupaciones
            gestor_groups = df.groupby(gestor_col, sort=False)

            # ── Iterar Gestores (creando Pestañas) ──────────────────────────────────
            for gestor_name, df_g in gestor_groups:
                _emit(on_status, "-" * 40, "info")
                _emit(on_status, f"🚘 Procesando Gestor/Hoja de Ruta: {gestor_name} ({len(df_g)} paradas)", "info")
                
                # Extraemos la patente específica del gestor
                vehiculo_del_gestor = None
                if veh_col:
                    val = str(df_g.iloc[0][veh_col]).strip()
                    if val and val.lower() != "nan":
                        vehiculo_del_gestor = val
                        _emit(on_status, f"Vehículo detectado para '{gestor_name}': '{vehiculo_del_gestor}'", "success")
                
                # Abrimos nueva pestaña independiente para esta Hoja de Ruta
                page_g = context.new_page()
                page_g.set_default_timeout(DEFAULT_TIMEOUT)
                
                try:
                    # Validar credenciales y sesión para esta pestaña independiente
                    _login(page_g, on_status)

                    # Navegar al formulario de hoja de ruta
                    _navigate_to_form(page_g, on_status)
                    _click_agregar_hoja(page_g, on_status)
                    
                    if vehiculo_del_gestor:
                        _seleccionar_vehiculo(page_g, vehiculo_del_gestor, on_status)
                    else:
                        _emit(on_status, f"⚠️ Gestor '{gestor_name}' no tiene Vehículo/Patente cargado. Se omite el paso.", "warning")
                    
                    _seleccionar_hoy(page_g, on_status)
                    
                except Exception as exc:
                    _emit(on_status, f"❌ Gestor '{gestor_name}' -> Falló la inicialización del formulario: {exc}", "error")
                    log.error(f"Falla inicialización gestor {gestor_name}\n{traceback.format_exc()}")
                    # Contabilizamos sus filas como error y pasamos al siguiente gestor
                    error_rows += len(df_g)
                    continue

                # Generar la lista de paradas inyectando explícitamente el origen y fin
                # Extraemos qué depósitos le corresponden a este Gestor (por defecto General)
                depo_config = (depositos_gestores or {}).get(gestor_name, {"origen": "General", "destino": "General"})
                
                # Manejar compatibilidad si por algún motivo llega un string solo
                if isinstance(depo_config, str):
                    depo_origen = depo_config
                    depo_destino = depo_config
                else:
                    depo_origen = depo_config.get("origen", "General")
                    depo_destino = depo_config.get("destino", "General")

                base_punto_inicio = _get_base_coords(depo_origen)
                base_punto_fin    = _get_base_coords(depo_destino)
                
                _emit(on_status, f"📍 Ruta asignada: Origen={depo_origen} | Destino={depo_destino}", "info")
                
                paradas_list = []
                # 1. Punto de inicio fijo (índice lógico 0)
                paradas_list.append({"row_num": 0, "row_dict": base_punto_inicio})
                
                # 1.5 Paradas adicionales manuales por gestor
                if extra_stops and gestor_name in extra_stops:
                    for ext in extra_stops[gestor_name]:
                        paradas_list.append({"row_num": 0, "row_dict": ext})
                
                # 2. Puntos variables del Excel
                for idx, row in df_g.iterrows():
                    paradas_list.append({
                        "row_num": df.index.get_loc(idx) + 1, 
                        "row_dict": row.to_dict()
                    })
                    
                # 3. Punto de fin fijo (índice alto por defecto para identificarlo fácil)
                paradas_list.append({"row_num": 999999, "row_dict": base_punto_fin})

                # Carga secuencial de filas en esta hoja
                current_dom_row_index = 0
                for local_i, item in enumerate(paradas_list):
                    row_dict = item["row_dict"]
                    row_num = item["row_num"]
                    is_last = (local_i == len(paradas_list) - 1)
                    
                    if local_i == 0:
                        label_desc = "BASE INICIO"
                    elif is_last:
                        label_desc = "BASE FIN"
                    elif row_num == 0:
                        label_desc = f"PARADA MANUAL ({row_dict.get('cliente', 'Extra')})"
                    else:
                        label_desc = f"Fila Excel {row_num}"
                        
                    _emit(on_status, f"▶ [Gestor: {gestor_name}] Procesando parada {local_i + 1} ({label_desc})...", "info")
                    
                    try:
                        _fill_form_row(page_g, row_dict, current_dom_row_index, on_status, is_last_row=is_last)
                        success_rows += 1
                        current_dom_row_index += 1
                        _emit(on_status, f"✅ [Gestor: {gestor_name}] Parada {label_desc} cargada.", "success")
                        insert_row_detail(history_id, row_num, row_dict, "Éxito")
                    except Exception as exc:
                        error_rows += 1
                        tb = traceback.format_exc()
                        _emit(on_status, f"❌ Error en parada {label_desc}: {exc}", "error")
                        log.error(f"Gestor {gestor_name} parada {label_desc} falló:\n{tb}")
                        insert_row_detail(history_id, row_num, row_dict, "Error", str(exc))

                        # Screenshot de error local
                        ss_path = f"data/error_{gestor_name}_{row_num}_{int(time.time())}.png".replace(" ", "_")
                        try:
                            page_g.screenshot(path=ss_path)
                        except Exception:
                            pass
                        
                        # Recuperación
                        _emit(on_status, f"↩ Intentando recuperar formulario para {gestor_name}...", "warning")
                        try:
                            _navigate_to_form(page_g, on_status)
                            _click_agregar_hoja(page_g, on_status)
                            if vehiculo_del_gestor:
                                _seleccionar_vehiculo(page_g, vehiculo_del_gestor, on_status)
                            _seleccionar_hoy(page_g, on_status)
                            current_dom_row_index = 0
                            _emit(on_status, "↩ Formulario recuperado. Continuando de forma limpia.", "info")
                        except Exception as r_exc:
                            _emit(on_status, f"❌ No se pudo recuperar pestaña de {gestor_name}. Abortando este gestor.", "error")
                            break
                            
                # Tareas finales para el gestor: Reordenar y Guardar
                try:
                    _reordenar_ruta(page_g, on_status)
                except Exception as r_exc:
                    _emit(on_status, f"⚠️ Aviso: Fallo en ordenamiento de ruta: {r_exc}", "warning")

                # Terminó este gestor. La pestaña se DEJA ABIERTA `page_g` para la revisión manual.
                _emit(on_status, f"🎉 Gestor {gestor_name} completado. Pestaña lista de fondo.", "info")

            # Guardar resultados antes de iniciar el timeout
            # Se considera éxito total si al menos las filas del Excel original pasaron bien
            es_exitoso = (error_rows == 0) or (success_rows >= len(df))
            final_status = "Éxito" if es_exitoso else "Error"
            update_history_record(history_id, success_rows, error_rows, final_status)

            summary = f"Proceso finalizado: {success_rows} éxitos, {error_rows} errores de {total_rows} filas."
            level = "success" if es_exitoso else "warning"
            _emit(on_status, summary, level)
            log.info(summary)
            log.info(f"FIN DE EJECUCIÓN  id={run_id}  status={final_status}")

            final_result = {
                "success":      es_exitoso,
                "total_rows":   total_rows,
                "success_rows": success_rows,
                "error_rows":   error_rows,
                "history_id":   history_id,
            }

            # Avisar al frontend que la tarea finalizó en DB, el bloque se queda paralizado por debajo
            if on_success_callback:
                on_success_callback(final_result)

            # Al finalizar todos los gestores
            if not headless:
                _emit(on_status, "⏳ Tarea marcada como Finalizada en Historial. Pestañas abiertas por 20 minutos para control manual.", "warning")
                try:
                    # Usamos la última pestaña abierta para mantener vivo el proceso
                    if 'page_g' in locals():
                        page_g.wait_for_timeout(1200_000)
                    else:
                        page_master.wait_for_timeout(1200_000)
                except Exception:
                    pass

            browser.close()
            log.info("Navegador cerrado.")
            
    except Exception as exc:
        trace = traceback.format_exc()
        msg = f"{type(exc).__name__}: {exc}"
        log.error(f"Error fatal de Playwright: {msg}\n{trace}")
        _emit(on_status, f"❌ Error del sistema: {msg}", "error")
        update_history_record(history_id, success_rows, error_rows, "Error", msg)
        final_result = {"success": False, "error": msg, "history_id": history_id}
        if on_success_callback:
            on_success_callback(final_result)
        return final_result
    finally:
        # Restaurar el event loop default por prolijidad
        if sys.platform == "win32":
            asyncio.new_event_loop = original_new_event_loop

    return final_result
