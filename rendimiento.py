import pandas as pd
import glob

mapeo_regiones = {
    1: "Tarapacá", 2: "Antofagasta", 3: "Atacama", 4: "Coquimbo", 
    5: "Valparaíso", 6: "O'Higgins", 7: "Maule", 8: "Biobío", 
    9: "La Araucanía", 10: "Los Lagos", 11: "Aysén", 12: "Magallanes", 
    13: "Metropolitana", 14: "Los Ríos", 15: "Arica", 16: "Ñuble"
}

archivos = glob.glob('*.csv')
lista_resumenes = []

def buscador_columnas(nombre):
    return nombre.strip().upper() in ['AGNO', 'COD_REG_RBD', 'PROM_GRAL']

for archivo in archivos:
    if 'resumen' in archivo: continue
    print(f" {archivo}")
    
    df = None
    try:
        df = pd.read_csv(archivo, sep=';', encoding='utf-8-sig', usecols=buscador_columnas, low_memory=False, on_bad_lines='skip')
    except:
        try:
            df = pd.read_csv(archivo, sep=';', encoding='latin-1', usecols=buscador_columnas, low_memory=False, on_bad_lines='skip')
        except Exception as e:
            print(f"{e}")

    if df is not None:
        try:
            df.columns = [c.strip().upper() for c in df.columns]

            df['PROM_GRAL'] = df['PROM_GRAL'].astype(str).str.replace(',', '.').astype(float)
            df = df[df['PROM_GRAL'] > 0]
            
            df['COD_REG_RBD'] = pd.to_numeric(df['COD_REG_RBD'], errors='coerce')
            df = df.dropna(subset=['COD_REG_RBD'])
            
            df['NOM_REGION'] = df['COD_REG_RBD'].map(mapeo_regiones)
            
            resumen = df.groupby(['AGNO', 'NOM_REGION'])['PROM_GRAL'].mean().reset_index()
            lista_resumenes.append(resumen)
            print("Procesado")
            
        except Exception as e:
            print(f"{e}")

if lista_resumenes:
    df_final = pd.concat(lista_resumenes)
    df_final['AGNO'] = pd.to_numeric(df_final['AGNO'], errors='coerce').astype(int)
    df_final = df_final.sort_values(by=['AGNO', 'NOM_REGION'])
    df_final.to_csv('resumen_nacional.csv', index=False)
    print("OK")