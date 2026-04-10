from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

_TEMPLATE_DIR = Path(__file__).parent / "templates"

_env = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "html.j2"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


class PdfRenderError(RuntimeError):
    pass


def render_html(template_name: str, context: dict[str, Any]) -> str:
    template = _env.get_template(template_name)
    return template.render(**context)


def render_pdf(template_name: str, context: dict[str, Any]) -> bytes:
    try:
        from weasyprint import HTML  # type: ignore[import-not-found]
    except (ImportError, OSError) as e:
        raise PdfRenderError(
            "WeasyPrint runtime is unavailable. "
            "Install GTK3 runtime to enable PDF rendering, "
            "or use HTML output and print to PDF via browser. "
            f"Underlying error: {e}"
        ) from e

    html_str = render_html(template_name, context)
    try:
        return HTML(string=html_str).write_pdf()
    except OSError as e:
        raise PdfRenderError(
            f"WeasyPrint failed during rendering: {e}"
        ) from e
