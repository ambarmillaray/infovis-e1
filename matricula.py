import pandas as pd
import glob
import unicodedata

mapeo_regiones = {
    "TARAPACA": "Tarapacá", "ANTOFAGASTA": "Antofagasta", "ATACAMA": "Atacama",
    "COQUIMBO": "Coquimbo", "VALPARAISO": "Valparaíso", "OHIGGINS": "O'Higgins",
    "LIBERTADOR GENERAL BERNARDO OHIGGINS": "O'Higgins", "MAULE": "Maule",
    "BIOBIO": "Biobío", "BIO BIO": "Biobío", "ARAUCANIA": "La Araucanía",
    "LA ARAUCANIA": "La Araucanía", "LOS LAGOS": "Los Lagos", "AYSEN": "Aysén",
    "AISEN DEL GENERAL CARLOS IBAÑEZ DEL CAMPO": "Aysén",
    "MAGALLANES": "Magallanes", "MAGALLANES Y DE LA ANTARTICA CHILENA": "Magallanes",
    "METROPOLITANA": "Metropolitana", "REGION METROPOLITANA DE SANTIAGO": "Metropolitana",
    "LOS RIOS": "Los Ríos", "ARICA Y PARINACOTA": "Arica", "ARICA": "Arica", "NUBLE": "Ñuble"
}

def limpiar_texto(txt):
    if pd.isna(txt): return ""
    s = str(txt).upper().strip()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

archivos = glob.glob('*.csv')
lista_resumenes = []

def buscador_superior(col):
    c = col.upper()
    return 'PERIODO' in c or 'REGION' in c

for archivo in archivos:
    if 'resumen' in archivo: continue
    print(f"Procesando Matriculados: {archivo}")
    
    df = None
    try:
        df = pd.read_csv(archivo, sep=';', encoding='utf-8-sig', low_memory=False)
    except:
        df = pd.read_csv(archivo, sep=';', encoding='latin-1', low_memory=False)

    if df is not None:
        try:
            df.columns = [c.strip().upper() for c in df.columns]
            
            col_año = [c for c in df.columns if 'PERIODO' in c][0]
            col_reg = [c for c in df.columns if 'REGION' in c][0]
            
            df['REGION_LIMPIA'] = df[col_reg].apply(limpiar_texto)
            df['NOM_REGION'] = df['REGION_LIMPIA'].map(mapeo_regiones)
            
            df.loc[df['NOM_REGION'].isna(), 'NOM_REGION'] = df[col_reg].astype(str).str.title()

            resumen = df.groupby([col_año, 'NOM_REGION']).size().reset_index(name='TOTAL_MATRICULA')
            resumen.columns = ['AGNO', 'NOM_REGION', 'TOTAL_MATRICULA']
            
            lista_resumenes.append(resumen)
            print("Procesado")
            
        except Exception as e:
            print(f"Error en contenido: {e}")

if lista_resumenes:
    df_final = pd.concat(lista_resumenes)
    df_final['AGNO'] = pd.to_numeric(df_final['AGNO'], errors='coerce').fillna(0).astype(int)
    df_final = df_final[df_final['AGNO'] > 0]
    
    df_final = df_final.sort_values(by=['AGNO', 'NOM_REGION'])
    df_final.to_csv('resumen_superior.csv', index=False)
    print("\n'resumen_superior.csv' generado")