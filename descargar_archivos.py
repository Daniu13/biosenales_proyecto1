import boto3, os
from botocore import UNSIGNED
from botocore.config import Config

DATASET = "ds004362"
BUCKET  = "openneuro.org"
RUNS    = ["4", "8", "12"]
DESTINO = "./data"

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def listar_archivos(prefijo):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefijo):
        for obj in page.get("Contents", []):
            yield obj["Key"]

os.makedirs(DESTINO, exist_ok=True)
descargados, errores = 0, 0

for key in listar_archivos(f"{DATASET}/"):
    filename = key.split("/")[-1]

    # Filtrar solo .set y .fdt de los runs 4, 8 y 12
    if not any(f"run-{r}_eeg" in filename for r in RUNS):
        continue
    if not (filename.endswith(".set") or filename.endswith(".fdt")):
        continue

    # Replicar estructura sub-XXX/eeg/ localmente
    ruta_local = os.path.join(DESTINO, *key.split("/")[1:])
    os.makedirs(os.path.dirname(ruta_local), exist_ok=True)

    if os.path.exists(ruta_local):
        print(f"[skip] {filename}")
        continue

    try:
        print(f"[↓] {key}")
        s3.download_file(BUCKET, key, ruta_local)
        descargados += 1
    except Exception as e:
        print(f"[!] Error en {key}: {e}")
        errores += 1

print(f"\n✓ {descargados} archivos descargados | {errores} errores")