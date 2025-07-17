"""pipeline_transformer.py (パイプライン用トランスフォーマー)

目的:
- rename.tsv を読み込みタイトルを正規化
  * コミック / CG      : comic_parser で `[作者]タイトル第N巻` 形式へ
  * 成年               : `[作者][出版社][20yymmdd]タイトル` 形式へ
  * それ以外           : PATTERN_MAPPING & CLIP_REGEX でマッピング
- `出力日時` を追加し Shift_JIS+Tab で保存

依存:
- utils.mapping_config  : PATTERN_MAPPING, CLIP_REGEX
- utils.comic_parser    : parse_comic_name, format_comic
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import os
import re
from datetime import datetime
import importlib
import chardet
import unicodedata

# --- ユーティリティ読み込み & 強制リロード ---
import utils.mapping_config as map_cfg
importlib.reload(map_cfg)
import importlib, utils.comic_parser as cp; importlib.reload(cp)

from utils.comic_parser import parse_comic_name, format_comic

PATTERN_MAPPING = getattr(map_cfg, 'PATTERN_MAPPING', {})
CLIP_REGEX = getattr(map_cfg, 'CLIP_REGEX', {})
PREFIX_MAPPING   = getattr(map_cfg, 'PREFIX_MAPPING', {})
DEFAULT_FILL = datetime.today().strftime('%Y%m%d')

@transformer
def normalize_titles(data, *args, **kwargs):
    if isinstance(data, list):
        data = data[0] if data else {}

    input_path = data.get('input_path')
    output_path = data.get('output_path')

    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)

    # df = pd.read_csv(input_path, sep='\t', encoding='utf-8')
    try:
        df = pd.read_csv(input_path, sep='\t', encoding='utf-8')
    except UnicodeDecodeError:
        # 1) まず CP932 / Shift_JIS で再試行
        try:
            df = pd.read_csv(input_path, sep='\t', encoding='cp932')
        except UnicodeDecodeError:
            # 2) さらに自動判定（chardet）
            with open(input_path, 'rb') as f:
                raw = f.read(4096)
            enc = chardet.detect(raw)['encoding'] or 'utf-8'
            df = pd.read_csv(input_path, sep='\t', encoding=enc, errors='ignore')

    if {'種別', '名称'} - set(df.columns):
        raise ValueError('必須列「種別」「名称」が不足しています')
    
    unknown_series = set()          # ★ 変換できなかったシリーズの一時保管

    def _normalize_series(s: str) -> str:
        # ① 全角ハイフン・長音・ダッシュ類 → 半角ハイフン
        s = re.sub(r'[―ー−–－]', '-', s)
        # ② ハイフン前後を空白に置換して“単語区切り”へ
        s = s.replace('-', ' ')
        # ③ 全角空白 → 半角空白、連続空白 → 1 個
        s = unicodedata.normalize('NFKC', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def convert(row):
        kind = str(row['種別']).strip()
        name = str(row['名称']).strip()
        tag   = PREFIX_MAPPING.get(kind, 'def')  # 先頭に付与するタグ（無ければdef）

        name_norm = _normalize_series(name)

        # ---（変換せずそのまま出力） ----
        if tag in {"def"}:
            return name
        # --- 設定資料 ---------------------------------------
        if kind in {"その他", "小説"}:
            return f"{tag}{name}" if tag else name
        # --- 設定資料 ---------------------------------------
        if kind == "美術":
            info   = parse_comic_name(name)      # [作者] があれば拾う
            author = info["author"] or ""        # 無ければ空文字
            title  = info["title"]  or name      # タイトルはそのまま
            today  = datetime.today().strftime("%Y%m%d")
            # [作者] が取れたら `[作者][出版社](不明)`、無ければ `[出版社](不明)`
            prefix = f"[{author}][出版社]({today})" if author else f"[作者][出版社]({today})"
            return f"{tag}{prefix}{title}" if tag else f"{prefix}{title}"
        # --- コミック / CG ---------------------------------
        if kind == "コミック":
            info = parse_comic_name(name)
            title  = format_comic(info)
            return title
        # --- 成年 ------------------------------------------
        if kind in {"成年コミック", "電子成年コミック"}:
            info = parse_comic_name(name)
            author = info["author"] or ""
            title  = info["title"]  or name
            today  = datetime.today().strftime("%y%m%d")
            fixed  = f"[出版社](20{today})"
            return f"{tag}[{author}]{fixed}{title}"
        # --- 同人（イベント名抽出）---------------------------
        if kind in {"同人", "電子同人"}:
            clean_name = cp._TAG_RE.sub('', name).strip()
            # 例: ... (サンクリ2024)  /  (COMIC1☆25) ...
            m_evt = re.search(r"\(([^)]*(サンクリ|例大祭|COMIC|C\d+)[^)]*)\)", name, re.IGNORECASE)

            if m_evt:
                event = m_evt.group(1)  # 例: C105, COMIC1☆25
            else:
                # 2) yyyy-mm-dd フォーマットを探す
                d = re.search(r"\d{4}-\d{2}-\d{2}", clean_name)
                event = d.group(0).replace('-', '') if d else "イベント不明"

            # ③ シリーズ名＝最後の (…) を抽出
            m_ser = re.search(r"\(([^()]+)\)(?!.*\([^()]*\))", clean_name)
            series_raw = m_ser.group(1).strip() if m_ser else ""

            series_short = map_cfg.SERIES_MAPPING.get(series_raw, series_raw)
            if series_raw == series_short:
                unknown_series.add(series_raw)

            # ⑤ 作者・タイトル
            info   = parse_comic_name(clean_name)
            author = info["author"] or ""
            title  = info["title"] or clean_name
            if series_raw:                          # タイトル末尾の (シリーズ名) を除去
                title = re.sub(r'\s*\(' + re.escape(series_raw) + r'\)\s*$', '', title).strip()
            
            out = f"{tag}[{author}]({event}){title}"
            if series_short:
                out += f" ({series_short})"
            return out

        # --- その他（マッピング ＋ クリップ）---------------
        for pattern, fmt in PATTERN_MAPPING.get(kind, []):
            m = re.search(pattern, name)
            if m:
                try:
                    mapped = fmt.format(*m.groups())
                    if kind == "成年雑誌":
                        # 月を 2 桁 0 埋め
                        mapped = re.sub(r'年(\d{1,2})月', lambda m: f'年{int(m.group(1)):02d}月', mapped)
                        mapped = re.sub(r'-(\d{1,2})', lambda m: f'-{int(m.group(1)):02d}', mapped)
                    if kind == "雑誌":
                        info  = parse_comic_name(name)
                        title  = info["title"]  or name
                        mapped = f"{mapped} {title}"
                    return f"{tag}{mapped}" if tag else mapped
                except IndexError:
                    continue

        if kind in CLIP_REGEX:
            pattern, grp = CLIP_REGEX[kind]
            m = re.search(pattern, name)
            if m:
                base = m.group(grp)
                return f"{tag}{base}" if tag else base
        return DEFAULT_FILL

    df['変換後'] = df.apply(convert, axis=1)
    df['出力日時'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ★ 未登録シリーズを DataFrame 化（空なら行ゼロ）
    log_df = pd.DataFrame({'未登録シリーズ候補': sorted(unknown_series)})

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, sep='\t', index=False, encoding='shift_jis', errors='ignore')

    # --- Unknown シリーズをログファイルへ ----------------------
    if unknown_series:
        log_dir  = os.path.dirname(output_path)
        ts       = datetime.now().strftime('%Y%m%d-%H%M%S')
        log_path = os.path.join(log_dir, f"unknown_series_{ts}.log")
        with open(log_path, 'w', encoding='utf-8') as f:
            for s in sorted(unknown_series):
                f.write(s + '\n')
        print(f"[LOG] 未登録シリーズ {len(unknown_series)} 件 → {log_path}")
    else:
        print('[LOG] 未登録シリーズはありません')

    return [df[['種別', '名称', '変換後', '出力日時']], log_df]
