"""BRD ingestion: turn an uploaded file (PDF or text) into plain BRD text.

The /plan endpoint accepts either pasted text, a .txt/.md file, or a PDF. PDFs
are parsed with pypdf (pure-Python, no native deps). Everything else is decoded
as UTF-8. Nothing here touches the LLM — it only produces the raw BRD string the
parser then reasons over.
"""

from __future__ import annotations

import io


def _looks_like_pdf(filename: str, data: bytes) -> bool:
    if filename and filename.lower().endswith(".pdf"):
        return True
    # PDFs start with the "%PDF-" magic header regardless of extension.
    return data[:5] == b"%PDF-"


def extract_pdf_text(data: bytes) -> str:
    """Extract text from a PDF byte stream. Raises a clear error if pypdf is
    missing or the file is unreadable."""
    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "PDF upload needs the 'pypdf' package — run: pip install pypdf"
        ) from exc

    try:
        reader = PdfReader(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001 — surface a friendly message
        raise RuntimeError(f"Could not read the PDF: {exc}") from exc

    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    text = "\n".join(pages).strip()
    if not text:
        raise RuntimeError(
            "The PDF has no extractable text (it may be a scanned image). "
            "Paste the BRD as text instead."
        )
    return text


def extract_brd_text(filename: str, data: bytes) -> str:
    """Return BRD text from an uploaded file — PDF or UTF-8 text/markdown."""
    if _looks_like_pdf(filename or "", data):
        return extract_pdf_text(data)
    return data.decode("utf-8", errors="replace").strip()
