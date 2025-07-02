"""
utils/comic_parser.py – 2025-06-13 EXTENDED
------------------------------------------
* [DL版] タグ・前後空白を除去
* 半角記号 → 全角記号
* アルファベット → カタカナ（jaconv が入っていれば自動）
* 作者名中の空白も削除
"""

import re
from typing import Dict, Optional

try:
    import jaconv             # optional
    to_kana = lambda s: jaconv.alphabet2kata(s)
except ImportError:
    to_kana = lambda s: s     # フォールバック

_ASCII_ONLY = re.compile(r'^[A-Za-z0-9 _\-]+$')

_FULLWIDTH = str.maketrans({
    '!': '！', '#': '＃', '$': '＄', '%': '％', '&': '＆',
    '+': '＋', ',': '，', '-': '－', ':': '：', ';': '；', '?': '？',
    '/': '／',
    '_': ' ','　': ' ','.': ''
})

_COMIC_RE = re.compile(
    r"^.*?\[(?P<author>[^\]]+)\]\s*"     # [作者]
    r"(?P<title>.*?)"                    # タイトル（非貪欲）
    r"(?:\s*第(?P<volume>\d+)巻)?"       # optional 第N巻
    r"\s*$",
    re.UNICODE,
)

_TAG_RE = re.compile(r"\[(DL版|Digital|dlsite_ver)\]|\(オリジナル\)|\(PRESTIGE COMIC\)", re.IGNORECASE)

def _ascii_only(s: str) -> bool:
    return bool(_ASCII_ONLY.fullmatch(s))

def _maybe_kana(s: str) -> str:
    return to_kana(s) if _ascii_only(s) else s

def _clean_author(author: str) -> str:
    author = author.replace(" ", "")
    if _ascii_only(author):
        author = author.lower()
        author = to_kana(author)
    return author.translate(_FULLWIDTH).strip()

def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    text = _TAG_RE.sub("", text).strip()
    if _ascii_only(text):
        text = text.lower()
        text = to_kana(text)
    return text.translate(_FULLWIDTH).strip()

_AUTHOR_BLOCK_RE = re.compile(r"\[([^\]]+)\]")        # すべての […]

def _extract_author_block(text: str) -> str:
    """
    各行の最後に出現する […] を作者ブロックとして返す。
    無ければ空文字。
    """
    text = _TAG_RE.sub("", text).strip()
    blocks = _AUTHOR_BLOCK_RE.findall(text)
    return blocks[-1] if blocks else ""

def parse_comic_name(name: str) -> Dict[str, Optional[str]]:
   # --- 作者ブロックを自前で抽出 ---------------------
    author_raw = _extract_author_block(name)

    # --- タイトル & 巻数は従来の正規表現で ---------- 
    m = _COMIC_RE.search(name)
    title_raw  = m.group("title")  if m else name
    volume_raw = m.group("volume") if m else None

    return {
        "author": _clean_author(author_raw),
        "title":  _clean(title_raw),
        "volume": volume_raw,
    }


def format_comic(info: Dict[str, Optional[str]]) -> str:
    parts = []
    if info.get("author"):
        parts.append(f"[{info['author']}]")
    if info.get("title"):
        parts.append(info["title"])
    if info.get("volume"):
        parts.append(f" 第{info['volume']}巻")
    return "".join(parts)
