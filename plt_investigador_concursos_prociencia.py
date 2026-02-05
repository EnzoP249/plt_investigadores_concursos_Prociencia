# -*- coding: utf-8 -*-
"""
Created on Thu Feb  5 09:30:38 2026

@author: Enzo
"""

##################################################################################
# SCRIPT PARA LA CREACIÓN DE UNA BASE DE DATOS SOBRE UN CONJUNTO DE INVESTIGADORES
##################################################################################

# Se importan las librerias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re              # estándar
import unicodedata
import time
import requests


# Se convierten los archivos en formato xlsx en objetos dataframes de pandas

# La información sobre el CTIvitae a diciembre de 2025
ctvitae = pd.read_excel("Data_cti_vitae_dic25.xlsx", sheet_name="Hoja1", header=0)
ctvitae.shape
ctvitae.columns

# Se renombran algunas columnas del dataframe
ctvitae.rename(columns=({"Nro de Documento de Identidad":"DNI",
                         "id_perfil_scopus":"codigo_scopus",
                         "wos_researcher_id":"codigo_wos",
                         "id_orcid":"codigo_orcid"
                         }), inplace=True)

ctvitae.columns


# La información sobre los investigadores Renacyt a diciembre de 2025

# Se construye una función que aborde la conversión de int en str para un procesamiento óptimizado
def int_to_str(value):
    return str(value)


# Especifica el diccionario de conversión en el parámetro converters
converters = {"DNI": int_to_str}

renacyt = pd.read_excel("Data_renacyt_dic25.xlsx", sheet_name="Hoja1", header=0, converters=converters)


# La información sobre los investigadores considerados
muestra = pd.read_excel("Investigadores_Incorporados.xlsx", sheet_name="Investigadores", header=0)
muestra.shape
muestra.columns

# Se renombra una variable del dataframe muestra
muestra.rename(columns=({"Nombre":"nombre_completo"}), inplace=True)


# Se construye una nueva columna para el dataframe ctvitae
ctvitae["nombre_completo"] = (
    ctvitae["Nombres"].astype(str) + " " +
    ctvitae["Apellido Paterno"].astype(str) + " " +
    ctvitae["Apellido Materno"].astype(str)
)


# Considerando el dataframe muestra, se consideran los nombres en mayúscula
muestra["Nombre"] = muestra["Nombre"].str.upper()

# Se crea un subdataframe considerando ctvitae 
ctvitae1 = ctvitae[["nombre_completo","Tipo_Documento","DNI", "Genero", "codigo_scopus", "codigo_wos",
                    "codigo_orcid", "codigo_renacyt", "pais_nacimiento", "Grado Académico Máximo Importado SUNEDU", "Areas|Sub Areas|Disciplinas"]]


# -----------------------------
# 1) Normalización de nombres
# -----------------------------
def normalize_name(s):
    """Mayúsculas + sin tildes/diacríticos + espacios limpios."""
    if pd.isna(s):
        return ""
    s = str(s).strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s).strip()
    return s

# -----------------------------
# 2) Clave robusta de match
#    (apellidos + primer nombre)
# -----------------------------
def build_match_key(fullname_norm: str):
    parts = fullname_norm.split()
    if len(parts) < 3:
        return ""
    primer_nombre = parts[0]
    apellidos = " ".join(parts[-2:])  # paterno + materno
    return f"{apellidos}|{primer_nombre}"

# -----------------------------
# 3) Preparar DF: normalizar + match_key
# -----------------------------
def prepare_for_match(df: pd.DataFrame, col_fullname: str, prefix: str):
    df = df.copy()
    df[f"{prefix}_norm"] = df[col_fullname].map(normalize_name)
    df["match_key"] = df[f"{prefix}_norm"].map(build_match_key)
    return df

# -----------------------------
# 4) Match + "aprovechar" columnas
# -----------------------------
def match_and_enrich(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    col_fullname_left: str,
    col_fullname_right: str,
    cols_to_bring=None,     # lista de columnas de df_right que quieres traer (None = trae todas)
    how="left"
):
    left = prepare_for_match(df_left, col_fullname_left, prefix="left")
    right = prepare_for_match(df_right, col_fullname_right, prefix="right")

    # Si quieres traer solo algunas columnas del right:
    if cols_to_bring is not None:
        cols_to_bring = [c for c in cols_to_bring if c in right.columns]
        right = right[["match_key"] + cols_to_bring]

    merged = left.merge(right, on="match_key", how=how, suffixes=("_df1", "_df2"))
    return merged

# -----------------------------
# 5) USO
# -----------------------------
# Ejemplo:
# muestra tiene columna "nombre_completo"
# ctvitae1 tiene columna "nombre_completo"


merged = match_and_enrich(
    df_left=muestra,
    df_right=ctvitae1,
    col_fullname_left="nombre_completo",
    col_fullname_right="nombre_completo",
    cols_to_bring=None,   # si quieres traer todo, pon None
    how="left"
)


merged.columns
# Considerando el dataframe merge, se utiliza valores únicos
merged = merged.drop_duplicates(subset=["nombre_completo_df1"])

# Se organiza el datafrmae merged
merged.columns
merged = merged[['nombre_completo_df1', 'Entidad Actual',
       'nombre_completo_df2',"Tipo_Documento",'DNI', 'Genero', 'codigo_scopus', 'codigo_wos',
       'codigo_orcid', 'codigo_renacyt', 'pais_nacimiento',
       'Grado Académico Máximo Importado SUNEDU',
       'Areas|Sub Areas|Disciplinas']]


# El dataframe merged se convierte en un archivo xlsx
merged.to_excel("datos_trabajados.xlsx")


# Se procede a realizar una descarga de la información almacenada en la base de datos de SCOPUS

# Especifica el diccionario de conversión en el parámetro converters
cambio = {"codigo_scopus": int_to_str}
investigador = pd.read_excel("BD_información_investigadores.xlsx", sheet_name="Investigadores", header=0, converters=cambio)
investigador.columns

# Se convierte una columna en una lista
investigador1 = investigador.dropna(subset=["codigo_scopus"])
caso = investigador1["codigo_scopus"].tolist()
type(caso)
print(caso)


API_KEY = "7fe684a86517ef163c6e8acd82c787f7"

BASE_URL = "https://api.elsevier.com/content/search/scopus"
HEADERS = {"X-ELS-APIKey": API_KEY, "Accept": "application/json"}

COUNT = 25            # 25 suele ir bien; sube/baja según tu cuota
SLEEP = 0.25          # ajusta si te cae 429
VIEW = "COMPLETE"     # trae más campos que STANDARD

def fetch_author_pubs(author_id: str) -> list[dict]:
    """Descarga todas las publicaciones de un autor (Scopus Author ID) usando cursor."""
    rows = []
    cursor = "*"
    while True:
        params = {
            "query": f"AU-ID({author_id})",
            "view": VIEW,
            "count": COUNT,
            "cursor": cursor,
        }
        r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)

        # manejo simple de rate-limit
        if r.status_code == 429:
            time.sleep(2.5)
            continue

        r.raise_for_status()
        data = r.json()
        sr = data.get("search-results", {})
        entries = sr.get("entry", []) or []
        if not entries:
            break

        for e in entries:
            # Campos típicos disponibles en Search (COMPLETE)
            rows.append({
                "author_id": author_id,
                "eid": e.get("eid"),
                "scopus_id": e.get("dc:identifier"),        # suele venir como "SCOPUS_ID:xxxx"
                "doi": e.get("prism:doi"),
                "title": e.get("dc:title"),
                "creator": e.get("dc:creator"),
                "publication_name": e.get("prism:publicationName"),
                "cover_date": e.get("prism:coverDate"),
                "aggregation_type": e.get("prism:aggregationType"),
                "subtype": e.get("subtypeDescription"),
                "citedby_count": e.get("citedby-count"),
                "openaccess": e.get("openaccess"),
                "authkeywords": e.get("authkeywords"),
                # financiamiento (si tu entitlement lo devuelve en Search)
                "fund_sponsor": e.get("fund-sponsor"),
                "fund_no": e.get("fund-no"),
                "fund_acr": e.get("fund-acr"),
            })

        # siguiente cursor (viene en link rel="next")
        next_link = None
        for lk in sr.get("link", []) or []:
            if lk.get("@ref") == "next":
                next_link = lk.get("@href")
                break

        if not next_link or "cursor=" not in next_link:
            break

        cursor = next_link.split("cursor=", 1)[1]
        time.sleep(SLEEP)

    return rows


# ==========
# MAIN
# ==========
all_rows = []
for i, aid in enumerate(caso, start=1):
    print(f"[{i}/{len(caso)}] Descargando author_id={aid} ...")
    all_rows.extend(fetch_author_pubs(str(aid).strip()))

df = pd.DataFrame(all_rows)

# Un paper puede aparecer por múltiples autores: dedup por EID (si existe)
if "eid" in df.columns:
    df = df.drop_duplicates(subset=["eid"], keep="first")

df.to_csv("scopus_produccion_40_autores.csv", index=False, encoding="utf-8-sig")
print(f"Listo ✅ Filas: {len(df)} | Archivo: scopus_produccion_40_autores.csv")





































