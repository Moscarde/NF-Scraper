"""
nfe_parser.py  (workers)
========================
Faz o parse do HTML da NF-e e retorna dicts serializáveis com
cabeçalho e itens da nota.

Baseado em parse_response_to_df.py, mas retorna listas de dicts
em vez de DataFrames para persistência direta no banco.

Função exportada:
    parse_nfe_html(html: str) -> dict
"""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

log = logging.getLogger("workers.nfe_parser")


# ---------------------------------------------------------------------------
# Helpers de limpeza de texto
# ---------------------------------------------------------------------------

def _clean_number(text: str) -> float | None:
    """Remove espaços e troca vírgula por ponto. Retorna float ou None."""
    try:
        cleaned = text.strip().replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def _extract_text(element, strip: bool = True) -> str:
    """Extrai texto de um elemento BeautifulSoup ou '' se None."""
    if element is None:
        return ""
    text = element.get_text()
    return text.strip() if strip else text


# ---------------------------------------------------------------------------
# Parse do cabeçalho
# ---------------------------------------------------------------------------

def _parse_header(soup: BeautifulSoup) -> dict[str, Any]:
    """
    Extrai dados do cabeçalho da NF-e.
    Campos: estabelecimento, cnpj, endereco, chave_acesso.
    """
    header: dict[str, Any] = {
        "estabelecimento": "",
        "cnpj":            "",
        "endereco":        "",
        "chave_acesso":    "",
    }

    # Nome do estabelecimento — div/span com classes comuns no layout RJ
    for selector in [
        ("div",  {"class": "txtTopo"}),
        ("span", {"class": "NomeEmit"}),
        ("div",  {"id": "collapse1"}),
    ]:
        tag, attrs = selector
        el = soup.find(tag, attrs)
        if el:
            header["estabelecimento"] = _extract_text(el).split("\n")[0].strip()
            break

    # CNPJ — span ou td que contenha "CNPJ"
    cnpj_el = soup.find(string=re.compile(r"CNPJ", re.I))
    if cnpj_el:
        parent = cnpj_el.parent
        raw = _extract_text(parent)
        cnpj_match = re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", raw)
        if cnpj_match:
            header["cnpj"] = cnpj_match.group()

    # Endereço — elemento logo após o CNPJ ou span com classe de endereço
    endereco_el = soup.find("span", {"class": re.compile(r"[Ee]nd|[Aa]ddr")})
    if not endereco_el:
        # Fallback: procura "Endereço" como string
        end_label = soup.find(string=re.compile(r"Endereço|Logradouro", re.I))
        if end_label and end_label.parent:
            endereco_el = end_label.parent.find_next_sibling()
    if endereco_el:
        header["endereco"] = _extract_text(endereco_el)

    # Chave de acesso
    chave_el = soup.find("span", {"class": "chave"})
    if chave_el:
        header["chave_acesso"] = _extract_text(chave_el).replace(" ", "")

    return header


# ---------------------------------------------------------------------------
# Parse dos itens
# ---------------------------------------------------------------------------

def _parse_items(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """
    Extrai itens da tabela #tabResult.
    Cada linha <tr id="ItemN"> vira um dict.
    """
    items: list[dict[str, Any]] = []

    table = soup.find("table", id="tabResult")
    if not table:
        log.warning("Tabela #tabResult não encontrada no HTML da NF-e.")
        return items

    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        col_dados = cols[0]

        nome_el    = col_dados.find("span", class_="txtTit")
        codigo_el  = col_dados.find("span", class_="RCod")
        qtd_el     = col_dados.find("span", class_="Rqtd")
        un_el      = col_dados.find("span", class_="RUN")
        vlunit_el  = col_dados.find("span", class_="RvlUnit")
        vltotal_el = cols[1].find("span", class_="valor")

        if not nome_el:
            continue

        descricao = _extract_text(nome_el)

        # Código — extrai apenas dígitos
        codigo_raw = _extract_text(codigo_el)
        codigo_match = re.search(r"\d+", codigo_raw)
        codigo = codigo_match.group() if codigo_match else ""

        # Quantidade — remove prefixo "Qtde.:"
        qtd_raw = _extract_text(qtd_el).replace("Qtde.:", "").strip()
        quantidade = _clean_number(qtd_raw)

        # Unidade
        un_raw = _extract_text(un_el).replace("UN:", "").strip()

        # Valor unitário — remove prefixo "Vl. Unit.:"
        vlunit_raw = _extract_text(vlunit_el).replace("Vl. Unit.:", "").strip()
        valor_unitario = _clean_number(vlunit_raw)

        # Valor total
        vltotal_raw = _extract_text(vltotal_el)
        valor_total = _clean_number(vltotal_raw)

        items.append({
            "descricao":      descricao,
            "codigo":         codigo,
            "quantidade":     quantidade,
            "unidade":        un_raw,
            "valor_unitario": valor_unitario,
            "valor_total":    valor_total,
        })

    return items


# ---------------------------------------------------------------------------
# Função principal exportada
# ---------------------------------------------------------------------------

def parse_nfe_html(html: str) -> dict[str, Any]:
    """
    Faz o parse do HTML da NF-e.

    Parâmetros
    ----------
    html : str
        HTML completo retornado por fetch_nfe_html().

    Retorna
    -------
    dict com as chaves:
        - header: dict  (estabelecimento, cnpj, endereco, chave_acesso)
        - items:  list[dict]  (um dict por produto)
        - total_itens: int
        - valor_total: float | None  (soma de valor_total dos itens)
        - ok: bool  (False se não encontrou tabela de itens)
    """
    soup = BeautifulSoup(html, "html.parser")

    header = _parse_header(soup)
    items  = _parse_items(soup)

    valor_total: float | None = None
    try:
        valores = [i["valor_total"] for i in items if i["valor_total"] is not None]
        valor_total = round(sum(valores), 2) if valores else None
    except Exception:
        pass

    result = {
        "header":       header,
        "items":        items,
        "total_itens":  len(items),
        "valor_total":  valor_total,
        "ok":           len(items) > 0,
    }

    log.info(
        "Parse NF-e: %d itens | valor_total=%.2f | estabelecimento=%r",
        result["total_itens"],
        result["valor_total"] or 0,
        header.get("estabelecimento"),
    )

    return result
