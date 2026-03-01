"""
nfe_scraper.py  (workers)
=========================
Módulo responsável por realizar o scraping da NF-e a partir de uma URL
extraída do QR code, utilizando Firefox headless via Playwright.

Função exportada: fetch_nfe_html(url: str) -> str | None
"""

import logging
import time

log = logging.getLogger("workers.nfe_scraper")

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0"
)

_CONTEXT_OPTS = dict(
    user_agent=_USER_AGENT,
    locale="pt-BR",
    viewport={"width": 1280, "height": 720},
    java_script_enabled=True,
    extra_http_headers={
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/webp,*/*;q=0.8"
        ),
    },
)

_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
"""

_NFE_INDICATORS = ["tabResult", "txtTit", "CNPJ", "Qtde.", "Vl. Unit."]


def _contem_dados_nfe(html: str) -> bool:
    """Heurística: verifica se o HTML contém dados reais de NF-e."""
    return any(ind in html for ind in _NFE_INDICATORS)


def fetch_nfe_html(url: str, timeout_resultado: int = 45) -> str | None:
    """
    Acessa a URL da NF-e via Firefox headless e retorna o HTML com os dados.

    Parâmetros
    ----------
    url : str
        URL extraída do QR code da nota fiscal.
    timeout_resultado : int
        Segundos máximos aguardando a resposta com dados (default: 45).

    Retorna
    -------
    str | None
        HTML completo da página de resultado, ou None se falhar.
    """
    from playwright.sync_api import sync_playwright

    log.info("Iniciando Firefox headless para: %s", url)

    resultado_html: str | None = None
    snapshot_fallback: str | None = None

    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            context = browser.new_context(**_CONTEXT_OPTS)
            context.add_init_script(_STEALTH_JS)
            page = context.new_page()

            # Listener de respostas: captura o POST JSF com resultado
            def on_response(response):
                nonlocal resultado_html
                try:
                    if "resultadoQRCode" in response.url and response.status == 200:
                        log.info(
                            "  Resposta resultadoQRCode capturada: %s", response.url
                        )
                        resultado_html = response.text()
                    else:
                        log.debug("  [resp] %s %s", response.status, response.url)
                except Exception as ex:
                    log.debug("  Erro ao ler corpo da resposta: %s", ex)

            page.on("response", on_response)

            log.debug("  Navegando (domcontentloaded)...")
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # Polling até capturar resultado ou timeout
            deadline = time.time() + timeout_resultado
            while resultado_html is None and time.time() < deadline:
                page.wait_for_timeout(500)
                try:
                    dom = page.content()
                    if _contem_dados_nfe(dom):
                        log.info("  Dados NFe encontrados diretamente no DOM.")
                        snapshot_fallback = dom
                        break
                except Exception:
                    pass

            # Último snapshot caso o listener não tenha capturado nada
            if resultado_html is None and snapshot_fallback is None:
                try:
                    snapshot_fallback = page.content()
                    log.debug("  Snapshot final: %d bytes", len(snapshot_fallback))
                except Exception:
                    pass

            browser.close()

    except Exception as e:
        log.error("Erro no Firefox headless: %s", e, exc_info=True)
        return None

    html = resultado_html or snapshot_fallback

    if html is None:
        log.warning("Nenhum HTML capturado para: %s", url)
        return None

    if _contem_dados_nfe(html):
        log.info("✅ HTML com dados NFe retornado (%d bytes)", len(html))
    else:
        log.warning("⚠️  HTML capturado mas sem dados NFe (%d bytes)", len(html))

    return html
