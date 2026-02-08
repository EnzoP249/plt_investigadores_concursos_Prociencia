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


# El dataframe merged se convierte en un archivo xlsx, el cual recibe intervención manual con el objetivo de ampliar su información
merged.to_excel("datos_trabajados.xlsx")


# Se procede a realizar una descarga de la información almacenada en la base de datos de SCOPUS

# Especifica el diccionario de conversión en el parámetro converters
cambio = {"codigo_scopus": int_to_str}
investigador = pd.read_excel("BD_información_investigadores.xlsx", sheet_name="Investigadores", header=0, converters=cambio)
investigador.columns

# Modifico la columna Area/subareas/disciplina del dataframe investigador
investigador["area_principal"] = investigador["Areas|Sub Areas|Disciplinas"].str.split("|").str[0].str.split("|").str[0].str.strip()

# Se convierte una columna en una lista
investigador1 = investigador.dropna(subset=["codigo_scopus"])
caso = investigador1["codigo_scopus"].tolist()
type(caso)
print(caso)


# Utilizando la descarga manual realizada de la información almacenada en SCOPUS, se procede a procesar dicha información
# Es importante señalar que se agrega información de la producción científica de un investigador seleccionado, por eso _2 

publicacion = pd.read_csv("bd_investigadores_seleccionados_2.csv", encoding = "utf-8", delimiter=",")

 
ids_set = set(map(str, caso))  # lookup rápido

# 2) Nombre exacto de la columna con IDs en tu DF
col_ids = "Author(s) ID"

# 3) Pasar de "una fila por publicación" a "una fila por (publicación, autor)"
df_long = (
    publicacion
      .assign(author_id=publicacion[col_ids].fillna("").astype(str).str.split(r"\s*;\s*"))
      .explode("author_id")
)

# 4) Limpiar y filtrar solo los IDs de tu lista
df_long["author_id"] = df_long["author_id"].str.strip()
df_match = df_long[df_long["author_id"].isin(ids_set)].copy()
df_match.columns

# Se identifica la cantidad única de investigadores seleccionados asociados con estas publicaciones
df_match["author_id"].nunique()

#Se cuenta las publicaciones científicas asociadas con un autor típico
df_match[df_match["author_id"]=="57193778002"].count()

# Se renombran columnas del dataframe df_match
df_match.rename(columns=({"Document Type":
                          "Document_type", "author_id":
                              "codigo_scopus"}), inplace=True)


# Se realiza una distribución total de las publicaciones científicas de los autores seleccionados
df_match.Document_type.value_counts()
df_match.Document_type.value_counts(normalize=True).round(3)*100

# Se elabora una tabla de contingencia que muestre la cantidad total de publicaciones científicas por año para cada investigador seleccionado
table1 = pd.pivot_table(df_match, values="EID", index="codigo_scopus", columns="Year", aggfunc="count")
type(table1)
table1.reset_index(inplace=True)


# Se elabora una tabla de contingencia que muestra la cantidad de producción científica delimitada por año y por cada investigador
categoria_sele = ["Article", "Review", "Conference paper", "Book chapter"]
df_match1 = df_match[df_match["Document_type"].isin(categoria_sele)]

table2 = pd.pivot_table(df_match1, values="EID", index="codigo_scopus", columns="Year", aggfunc="count")
type(table2)
table2.reset_index(inplace=True)

# Realizo dos fusiones considernado como ancla el código scopus
fusion1 = pd.merge(investigador, table1, on="codigo_scopus", how="left")
fusion2 = pd.merge(investigador, table2, on="codigo_scopus", how="left")

# Se construye investigador2

investigador2 = investigador[["codigo_scopus", "nombre_completo_df1"]]

# Se crea un dataframe que muestra la relación entre el investigador y su producción científica total
produccion1 = df_match[["codigo_scopus", "EID", "DOI", "Title", "Year", "Source title", "Affiliations",
                             "Language of Original Document", "Funding Details", "Open Access"]]

produccion2 = df_match1[["codigo_scopus", "EID", "DOI", "Title", "Year", "Source title", "Affiliations",
                             "Language of Original Document", "Funding Details", "Open Access"]]


produccion_total = pd.merge(investigador2, produccion1, on="codigo_scopus", how="left")
produccion_parcial = pd.merge(investigador2, produccion2, on="codigo_scopus", how="left")

# Se delimita tanto la producción total y la producción parcial
produccion_total = produccion_total[produccion_total["Year"]>=2016]
produccion_parcial = produccion_parcial[produccion_parcial["Year"]>=2016]


a = "BD_investigadores_producción_cientifica_arch"

with pd.ExcelWriter(f'{a}.xlsx') as writer:
    # Guardar cada DataFrame en una hoja diferente
    fusion2.to_excel(writer, sheet_name="Investigador", index=False)
    produccion_parcial.to_excel(writer, sheet_name="Publicaciones", index=False)
    #df_sin_duplicados.to_excel(writer, sheet_name='2. PubCalificadasRenacytUniv', index=False)
    #fusion4.to_excel(writer, sheet_name="3.PubClasificadasAfill", index=False)


###############################################################################
# AGREGAR INFORMACIÓN A UN CONJUNTO DE ARTÍCULOS CIENTÍFICOS
###############################################################################

# Se utilizan la base de datos sobre publicaciones científicas indizadas en scopus
pub_scopus = pd.read_csv("tbl_scopus_pub.csv", encoding = "utf-8", delimiter=",")
pub_scopus.shape
dir(pub_scopus)
pub_scopus.columns


# Se crea un subset del dataframe pub_scopus
pub_scopus1 = pub_scopus[["eid", "doi", "source_title", "title", "cover_date"]]


# Se utiliza una base datos sobre publicaciones científicas y sus autores
autor_scopus = pd.read_csv("tbl_ws_api_scopus_detalle_afiliacion_publicaciones_renacyt.csv",
                           encoding="utf-8", delimiter=",")

autor_scopus.columns

autor_scopus1 = autor_scopus[["eid", "doi", "auth_id", "auth_name", "af_id", "affil_name"]]


# Se utiliza un archivo que contiene publicaciones científicas enviado por el Prociencia

















































































