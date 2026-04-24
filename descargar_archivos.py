import boto3, os
from botocore import UNSIGNED
from botocore.config import Config

DATASET = "ds004362"
BUCKET  = "openneuro.org"
RUNS    = ["3", "4", "7", "8", "11", "12"]  # Task1 (real) + Task2 (imaginado)
DESTINO = "./data"

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def listar_archivos(prefijo):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefijo):
        for obj in page.get("Contents", []):
            yield obj["Key"]

os.makedirs(DESTINO, exist_ok=True)
descargados, omitidos, errores = 0, 0, 0

for key in listar_archivos(f"{DATASET}/"):
    filename = key.split("/")[-1]

    if not any(f"run-{r}_eeg" in filename for r in RUNS):
        continue
    if not (filename.endswith(".set") or filename.endswith(".fdt")):
        continue

    ruta_local = os.path.join(DESTINO, *key.split("/")[1:])
    os.makedirs(os.path.dirname(ruta_local), exist_ok=True)

    if os.path.exists(ruta_local):
        print(f"[skip] {filename}")
        omitidos += 1
        continue

    try:
        print(f"[↓] {key}")
        s3.download_file(BUCKET, key, ruta_local)
        descargados += 1
    except Exception as e:
        print(f"[!] Error en {key}: {e}")
        errores += 1

print(f"\n✓ {descargados} descargados | {omitidos} omitidos (ya existían) | {errores} errores")