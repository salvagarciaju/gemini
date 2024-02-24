import csv
import openai
import re
import concurrent.futures
import threading
import os
import unicodedata
import random

apis_openai = [line.strip() for line in open("0. OpenAI.txt", "r", encoding="utf-8")]
autores = [line.strip() for line in open("1. Autores.txt", "r", encoding="utf-8")]

titulo_sistema = open("0. Sistema/0. Titulo.txt", "r", encoding="utf-8").read().strip()
resumen_sistema = open("0. Sistema/1. Resumen.txt", "r", encoding="utf-8").read().strip()
estructura_sistema = open("0. Sistema/2. Estructura.txt", "r", encoding="utf-8").read().strip()
articulo_sistema = open("0. Sistema/3. Articulo.txt", "r", encoding="utf-8").read().strip()
categoria_sistema = open("0. Sistema/4. Categoria.txt", "r", encoding="utf-8").read().strip()

titulo_usuario = open("1. Usuario/0. Titulo.txt", "r", encoding="utf-8").read().strip()
resumen_usuario = open("1. Usuario/1. Resumen.txt", "r", encoding="utf-8").read().strip()
estructura_usuario = open("1. Usuario/2. Estructura.txt", "r", encoding="utf-8").read().strip()
articulo_usuario = open("1. Usuario/3. Articulo.txt", "r", encoding="utf-8").read().strip()
categoria_usuario = open("1. Usuario/4. Categoria.txt", "r", encoding="utf-8").read().strip()

titulo_asistente = open("2. Asistente/0. Titulo.txt", "r", encoding="utf-8").read().strip()
resumen_asistente = open("2. Asistente/1. Resumen.txt", "r", encoding="utf-8").read().strip()
estructura_asistente = open("2. Asistente/2. Estructura.txt", "r", encoding="utf-8").read().strip()
articulo_asistente = open("2. Asistente/3. Articulo.txt", "r", encoding="utf-8").read().strip()
categoria_asistente = open("2. Asistente/4. Categoria.txt", "r", encoding="utf-8").read().strip()

api_openai_actual = 0
contador_keywords = 0
bloqueo = threading.Lock()

def chatGPT(sistema, usuario, asistente):
    global api_openai_actual
    while True:
        api_openai_actual = (api_openai_actual + 1) % len(apis_openai)
        openai.api_key = apis_openai[api_openai_actual]
        try:
            respuesta = openai.ChatCompletion.create(
                model="gpt-3.5-turbo-16k",
                messages=[
                    {"role": "system", "content": sistema},
                    {"role": "user", "content": usuario},
                    {"role": "assistant", "content": asistente}
                ]
            )
            return respuesta.choices[0].message["content"].strip()
        except Exception:
            pass

def chatGPT16k(sistema, usuario, asistente):
    global api_openai_actual
    while True:
        api_openai_actual = (api_openai_actual + 1) % len(apis_openai)
        openai.api_key = apis_openai[api_openai_actual]
        try:
            respuesta = openai.ChatCompletion.create(
                model="gpt-3.5-turbo-16k",
                messages=[
                    {"role": "system", "content": sistema},
                    {"role": "user", "content": usuario},
                    {"role": "assistant", "content": asistente}
                ]
            )
            return respuesta.choices[0].message["content"].strip()
        except Exception:
            pass

def crear_titulo(keywords, titulo):
    titulo = chatGPT(titulo_sistema.format(titulo=titulo), titulo_usuario.format(keywords=keywords), titulo_asistente).replace('"', '')
    intentos = 0
    while len(titulo) > 70 and intentos < 3:
        titulo = chatGPT("Haz más pequeño el título recibido.", titulo, titulo_asistente).replace('"', '')
        intentos +=1
    return titulo.rstrip(".")

def crear_resumen(contenido):
    return chatGPT(resumen_sistema, resumen_usuario.format(contenido=contenido), resumen_asistente)

def crear_estructura(titulo, resumen):
    return chatGPT(estructura_sistema.format(titulo=titulo), estructura_usuario.format(resumen=resumen), estructura_asistente)

def crear_articulo(titulo, resumen, estructura, keywords):
    articulo = chatGPT16k(articulo_sistema.format(titulo=titulo, estructura=estructura,keywords=keywords), articulo_usuario.format(resumen=resumen), articulo_asistente)
    articulo = re.sub(r'En (conclusión|resumen), (\w)', lambda match: match.group(2).upper(), articulo)
    articulo = articulo.replace("En conclusión, ", "").replace("En resumen, ", "")
    if "<h3>" in articulo and "<h2>" in articulo:
        primer_h2 = articulo.find("<h2>")
        seccion_antes_h2 = articulo[:primer_h2].replace("<h3>", "<h2>")
        articulo = seccion_antes_h2 + articulo[primer_h2:]
    return articulo

def crear_categoria(titulo):
    return chatGPT(categoria_sistema, categoria_usuario.format(titulo=titulo), categoria_asistente)

def crear_slug(keywords):
  slug = keywords.split('\n')[0]
  slug = slug.lower()
  slug = unicodedata.normalize('NFKD', slug).encode('ASCII', 'ignore').decode('ASCII')
  slug = slug.replace('ñ', 'n')
  slug = slug.replace(' ', '-')
  return slug

def crear_autor():
  return random.choice(autores)

def leer_scrapeado(nombre_archivo):
    with open(nombre_archivo, newline="", encoding="utf-8") as archivo_csv:
        lector = csv.DictReader(archivo_csv)
        return [fila for fila in lector]

def leer_keywords_procesadas(nombre_archivo):
    if os.path.exists(nombre_archivo):
        with open(nombre_archivo, newline="", encoding="utf-8") as archivo_csv:
            lector = csv.DictReader(archivo_csv)
            return {fila['Keywords'] for fila in lector}
    else:
        return set()

def procesar_fila(fila, keywords_procesadas):
    if fila['Keywords'] not in keywords_procesadas:
        global contador_keywords
        titulo = crear_titulo(fila['Keywords'], fila['Titulo'])
        resumen = crear_resumen(fila['Contenido'])
        estructura = crear_estructura(titulo, resumen)
        contenido = crear_articulo(titulo, resumen, estructura, fila['Keywords'])
        categoria = crear_categoria(titulo)
        slug = crear_slug(fila['Keywords'])
        autor = crear_autor()
        fila_resultado = [fila['URL'], fila['Keywords'], titulo, contenido, fila['Portada'], categoria, slug, autor]
        with bloqueo:
            escritor.writerow(fila_resultado)
        contador_keywords += 1
        print(f"Progreso: {contador_keywords}/{keywords_totales} | Título: {titulo}")

datos_scrapeados = leer_scrapeado("4. Scrapeado.csv")
keywords_procesadas = leer_keywords_procesadas("5. Redactado.csv")
keywords_totales_scrapeadas = {fila['Keywords'] for fila in datos_scrapeados}
keywords_restantes = keywords_totales_scrapeadas - keywords_procesadas

contador_keywords = 0
keywords_totales = len(keywords_restantes)

modo_apertura = "a" if keywords_procesadas else "w"

with open("5. Redactado.csv", modo_apertura, newline="", encoding="utf-8") as archivo_csv:
    escritor = csv.writer(archivo_csv)
    if not keywords_procesadas:
        escritor.writerow(["URL", "Keywords", "Titulo", "Contenido", "Portada", "Categoria", "SLUG", "Autor"])
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ejecutor:
        futures = [ejecutor.submit(procesar_fila, fila, keywords_procesadas) for fila in datos_scrapeados if fila['Keywords'] in keywords_restantes]
        concurrent.futures.wait(futures)
