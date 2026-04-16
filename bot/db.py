"""
Módulo de persistencia en Supabase.
"""
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        try:
            import streamlit as st
            url = st.secrets.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL")
            key = st.secrets.get("SUPABASE_KEY") or os.environ.get("SUPABASE_KEY")
        except Exception:
            url = os.environ.get("SUPABASE_URL")
            key = os.environ.get("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("Faltan las variables SUPABASE_URL o SUPABASE_KEY.")
            
        _client = create_client(url, key)
    return _client


def create_history_record(filename: str, total_rows: int) -> int:
    """Inserta un registro en estado 'Procesando' y devuelve su id."""
    client = get_client()
    response = (
        client.table("upload_history")
        .insert({
            "filename": filename,
            "total_rows": total_rows,
            "success_rows": 0,
            "error_rows": 0,
            "status": "Procesando",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        })
        .execute()
    )
    return response.data[0]["id"]


def update_history_record(
    history_id: int,
    success_rows: int,
    error_rows: int,
    status: str,
    error_detail: str | None = None,
) -> None:
    """Actualiza el registro una vez finalizado el proceso."""
    client = get_client()
    payload: dict = {
        "success_rows": success_rows,
        "error_rows": error_rows,
        "status": status,
    }
    if error_detail:
        payload["error_detail"] = error_detail
    client.table("upload_history").update(payload).eq("id", history_id).execute()


def insert_row_detail(
    history_id: int,
    row_index: int,
    row_data: dict,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Guarda el resultado de cada fila individual."""
    client = get_client()
    client.table("upload_rows").insert({
        "history_id": history_id,
        "row_index": row_index,
        "row_data": row_data,
        "status": status,
        "error_msg": error_msg,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }).execute()


def fetch_history() -> list[dict]:
    """Devuelve todo el historial ordenado por fecha descendente."""
    client = get_client()
    response = (
        client.table("upload_history")
        .select("*")
        .order("uploaded_at", desc=True)
        .execute()
    )
    return response.data
