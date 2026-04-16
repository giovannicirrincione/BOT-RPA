# Guía de Adaptación del Bot

## Pasos obligatorios antes de usar

### 1. Configurar variables de entorno

Copiar `.env.example` a `.env` y completar:

```
BOT_URL=https://url-del-sistema-real.com/login
BOT_USER=mi_usuario
BOT_PASSWORD=mi_contraseña
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=eyJhbGci...
```

### 2. Crear las tablas en Supabase

Ejecutar `supabase_schema.sql` en el SQL Editor de tu proyecto Supabase.

### 3. Adaptar `bot/rpa_bot.py`

Hay **tres funciones** que debés editar según el sistema real:

---

#### `_login(page, on_status)` — línea ~40

Reemplazar los selectores genéricos por los reales del formulario de login.

```python
# Ejemplo con selectores reales:
page.fill("#user",     BOT_USER)
page.fill("#password", BOT_PASSWORD)
page.click("#btn-login")
page.wait_for_url("**/dashboard", timeout=15_000)
```

**Tip:** Abrí el sistema en Chrome, F12 → Inspector, y copiá el `id` o `name` de cada campo.

---

#### `_navigate_to_form(page, on_status)` — línea ~63

Navegá hasta el formulario de nueva Hoja de Ruta.

```python
# Opción A: navegar por menú
page.click("text=Logística")
page.click("text=Nueva Hoja de Ruta")
page.wait_for_load_state("networkidle")

# Opción B: ir directo a la URL
page.goto("https://sistema.com/hojas-de-ruta/nueva")
```

---

#### `_fill_form_row(page, row, on_status)` — línea ~79

Mapear cada columna del Excel con los campos del formulario.

```python
# La variable `row` es un dict con claves = columnas del Excel (en minúsculas con _)
# Ejemplo si tu Excel tiene columnas: Fecha, Origen, Destino, GPS, Tipo

page.fill("#fecha",          str(row.get("fecha", "")))
page.fill("#origen",         str(row.get("origen", "")))
page.fill("#destino",        str(row.get("destino", "")))
page.fill("#gps",            str(row.get("gps", "")))
page.select_option("#tipo",  str(row.get("tipo", "")))
page.click("button:has-text('Guardar')")
page.wait_for_selector(".alert-success", timeout=10_000)
```

**Importante:** Siempre esperá una confirmación del sistema (`.alert-success`, cambio de URL, etc.)
antes de pasar a la siguiente fila.

---

### 4. Ejecutar la app

```bash
# Activar entorno virtual (Windows)
.venv\Scripts\activate

# Iniciar Streamlit
streamlit run frontend/app.py
```

La app abre en `http://localhost:8501`.

---

## Estructura del proyecto

```
Bot-TE/
├── bot/
│   ├── __init__.py
│   ├── rpa_bot.py        # ← Bot Playwright (ADAPTAR aquí)
│   └── db.py             # Persistencia Supabase
├── frontend/
│   └── app.py            # UI Streamlit
├── data/                 # Screenshots de errores (auto-generado)
├── .env.example
├── requirements.txt
├── setup.bat
├── supabase_schema.sql
└── ADAPTACION.md         # Este archivo
```
