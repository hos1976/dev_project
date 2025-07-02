"""utils/title_formatter.py

パイプライン側から `format_title(kind, name)` を呼び出すだけで
- コミック / CG            : `[作者]タイトル第N巻`
- 成年                      : `[作者][出版社][20yymmdd]タイトル`
- その他                    : PATTERN_MAPPING / CLIP_REGEX / fallback
のいずれかを返す共通ユーティリティ。

依存:
- utils.comic_parser        : parse_comic_name, format_comic
- utils.mapping_config      : PATTERN_MAPPING, CLIP_REGEX

これにより **pipeline_transformer.py** は極小の I/O & 日付付与ロジックのみとなり、
種類別の文字列変換はすべて本モジュール内に集約されます。
"""

from datetime import datetime
import re, importlib
from utils.comic_parser import parse_comic_name, format_comic
import utils.mapping_config as map_cfg
importlib.reload(map_cfg)

PATTERN_MAPPING = getattr(map_cfg, 'PATTERN_MAPPING', {})
CLIP_REGEX = getattr(map_cfg, 'CLIP_REGEX', {})
DEFAULT_FILL = datetime.today().strftime('%Y%m%d')

__all__ = ['format_title']

# -- 内部ヘルパー --

def _format_comic_or_cg(name):
    return format_comic(parse_comic_name(name))

def _format_seinen(name):
    info = parse_comic_name(name)
    author = info['author'] or ''
    title = info['title'] or name
    today = datetime.today().strftime('%y%m%d')
    return f"[{author}][出版社](20{today}){title}"

def _mapping_lookup(kind, name):
    for pattern, fmt in PATTERN_MAPPING.get(kind, []):
        m = re.search(pattern, name)
        if m:
            try:
                return fmt.format(*m.groups())
            except IndexError:
                continue
    if kind in CLIP_REGEX:
        pat, grp = CLIP_REGEX[kind]
        m = re.search(pat, name)
        if m:
            return m.group(grp)
    return DEFAULT_FILL

# --- 公開関数 ---

def format_title(kind: str, name: str) -> str:
    kind = kind.strip()
    if kind in {'コミック', 'CG'}:
        return _format_comic_or_cg(name)
    if kind == '成年':
        return _format_seinen(name)
    return _mapping_lookup(kind, name)
