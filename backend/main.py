"""ОТГ Cleaner — single-purpose FastAPI service.

Accepts two registry files (land + real estate) as Excel or CSV, returns one
clean .xlsx workbook with consistent names, addresses and tax IDs.
"""

from __future__ import annotations

import datetime as _dt
import shutil
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from cleaner.pipeline import clean_to_xlsx


ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = ROOT / "frontend"

ALLOWED_SUFFIXES = {".xlsx", ".xlsm", ".csv"}


app = FastAPI(
    title="ОТГ Cleaner",
    description="Завантаж два реєстри (земля + нерухомість) — отримай один чистий Excel.",
    version="1.0.0",
)


def _save_upload(upload: UploadFile, dest_dir: Path) -> Path:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        raise HTTPException(
            status_code=415,
            detail=f"Непідтримуваний формат: {suffix!r}. Дозволено: .xlsx / .csv",
        )
    dest = dest_dir / (upload.filename or f"upload{suffix}")
    with dest.open("wb") as f:
        shutil.copyfileobj(upload.file, f)
    return dest


@app.post("/api/clean")
def clean(
    land: UploadFile = File(..., description="Земельний реєстр (ДРРП / ДЗК)"),
    realestate: UploadFile = File(..., description="Реєстр нерухомості"),
) -> Response:
    tmp = Path(tempfile.mkdtemp(prefix="otg_clean_"))
    try:
        land_path = _save_upload(land, tmp)
        re_path = _save_upload(realestate, tmp)
        try:
            result = clean_to_xlsx(
                land_path,
                re_path,
                land_display_name=land.filename,
                re_display_name=realestate.filename,
            )
        except Exception as e:  # noqa: BLE001 — surfacing a friendly message to the UI
            raise HTTPException(status_code=422, detail=f"Не вдалось обробити файли: {e}") from e
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"otg_audit_{stamp}.xlsx"
    stats = result["stats"]
    sev = stats.get("findings_by_severity") or {}
    return Response(
        content=result["bytes"],
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Stats-Land": str(stats["land_rows"]),
            "X-Stats-RealEstate": str(stats["realestate_rows"]),
            "X-Stats-Owners": str(stats["owners"]),
            "X-Stats-LandChanged": str(stats["land_rows_changed"]),
            "X-Stats-RealEstateChanged": str(stats["realestate_rows_changed"]),
            "X-Findings-Total": str(stats.get("findings_total", 0)),
            "X-Findings-Critical": str(sev.get("critical", 0)),
            "X-Findings-High": str(sev.get("high", 0)),
            "X-Findings-Medium": str(sev.get("medium", 0)),
            "X-Findings-Low": str(sev.get("low", 0)),
            "X-Findings-Exposure": str(stats.get("findings_exposure_uah", 0)),
        },
    )


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    def root() -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
