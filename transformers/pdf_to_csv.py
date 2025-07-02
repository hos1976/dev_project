from __future__ import annotations

"""
PDF → CSV 変換ブロック（複数 PDF & 複数ページ対応版）
=====================================================
* **input/** 配下にあるすべての `*.pdf` を探索し、**全ページ**から見つかった
  すべてのテーブルを抽出して 1 つの `DataFrame` にまとめます。
* 各 PDF ごとにまとめた結果を **output/** に `<元ファイル名>.csv` として
  UTF‑8 (BOM付き) で保存します。
* 行内がすべて空（空白 / 空文字 / NaN）の行は取り除いてから書き出します。
* 失敗したファイルはスキップし、原因をコンソールへ出力して処理を継続します。
* 返り値は、処理できたすべての PDF から得た DataFrame を行方向で連結した
  単一の `DataFrame` です（列が合わない場合は外部結合）。

依存パッケージ:
    pip install "camelot-py[cv]" pandas
    # Linux 系 OS では追加で ghostscript が必要な場合があります
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pathlib import Path
import traceback
import camelot  # type: ignore
import pandas as pd


@transformer
def pdf_to_csv_fixed(*_, **__) -> pd.DataFrame:
    """Walk through every PDF under input/, extract all tables, save and merge."""

    root = Path.cwd()
    in_dir = root / "input"
    out_dir = root / "output"
    out_dir.mkdir(exist_ok=True)

    merged: list[pd.DataFrame] = []

    for pdf_path in in_dir.glob("*.pdf"):
        try:
            # ---- 全ページの表を抽出 ----
            tables = camelot.read_pdf(str(pdf_path), pages="all")
            if not tables:
                print(f"⚠️  No tables found in {pdf_path.relative_to(root)} — skipped")
                continue

            # ---- 同 PDF から得たテーブルを結合 ----
            dfs = [t.df for t in tables]
            df_pdf = pd.concat(dfs, ignore_index=True)
            # 空白セル → NA 置換後、全列 NA 行を除外
            df_pdf = (
                df_pdf.replace(r"^\s*$", pd.NA, regex=True)
                      .dropna(how="all")
            )
            if df_pdf.empty:
                print(f"⚠️  Only blank rows in {pdf_path.name} — skipped")
                continue

            # ---- CSV 書き出し ----
            csv_path = out_dir / f"{pdf_path.stem}.csv"
            df_pdf.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"✅  CSV written → {csv_path.relative_to(root)}")

            merged.append(df_pdf)

        except Exception as err:
            # 個別 PDF の失敗はログ化して続行
            print(f"❌ ERROR processing {pdf_path.name}: {err.__class__.__name__}: {err}")
            err_file = out_dir / f"{pdf_path.stem}_error.txt"
            err_file.write_text(traceback.format_exc(), encoding="utf-8")
            print(f"   ↳ 詳細は {err_file.relative_to(root)} に保存しました")

    if not merged:
        return pd.DataFrame()

    return pd.concat(merged, ignore_index=True, sort=False)


@test
def test_output(output, **_):
    """Sanity check: output must be a DataFrame (can be empty)."""
    assert isinstance(output, pd.DataFrame), "Output is not DataFrame"
