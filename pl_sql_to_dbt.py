import os
import re
import sys



# --- REPLACE ROWNUM / SYSDATE ---
RE_TRIPLE_ROWNUM_SYSDATE = re.compile(r"""
    \bROWNUM\b \s+ (?:AS\s+)?ROW_NUMBER_ID \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_CREATION_DATE \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_LAST_UPDATE_DATE
""", re.IGNORECASE | re.VERBOSE | re.DOTALL)

def normalize_oracle_rownum_sysdate(sql: str) -> str:
    """
    Replaces the Oracle rownum and sysdate system calls with their Snowflake equivalents.
    """
    def _triple_repl(_m):
        return (
            "seq8() + 1 AS ROW_NUMBER_ID, "
            "current_timestamp() AS ROW_CREATION_DATE, "
            "current_timestamp() AS ROW_LAST_UPDATE_DATE"
        )

    #1) For the block of the 3rd var
    s = RE_TRIPLE_ROWNUM_SYSDATE.sub(_triple_repl, sql)

    # 2) Fallbacks if the 3 var is not together
    s = re.sub(
        r"\bROWNUM\b\s+(?:AS\s+)?ROW_NUMBER_ID\b",
        "seq8() + 1 AS ROW_NUMBER_ID",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"\bSYSDATE\b\s+(?:AS\s+)?ROW_CREATION_DATE\b",
        "current_timestamp() AS ROW_CREATION_DATE",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"\bSYSDATE\b\s+(?:AS\s+)?ROW_LAST_UPDATE_DATE\b",
        "current_timestamp() AS ROW_LAST_UPDATE_DATE",
        s,
        flags=re.IGNORECASE,
    )
    return s

#---------- Remove _bz ----------
def transform_table_references(sql):
        """Transforms table references with the appropriate schemas"""
        def replacer(match):
            keyword = match.group(1)
            table = match.group(2)

            # Specific Case for steph_apps_FND_FLEX_VALUES#_bz
            if table.lower() == 'steph_apps_fnd_flex_values#_bz':
                return f"{keyword} DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES"

            # General case for _bz table
            if table.lower().endswith('_bz'):
                table_name = table[:-3]
                return f"{keyword} DEV.LH2_BRONZE_DEV.{table_name}"

            # Otherwise, we keep the SILVER scheme.
            return f"{keyword} DEV.LH2_SILVER_DEV.{table}"
        
        return re.sub(
            r"\b(FROM|JOIN)\s+([A-Za-z0-9_#]+)", 
            replacer, 
            sql, 
            flags=re.IGNORECASE
        )

# ---------- Header dbt ----------
def generate_dbt_config(table_name: str) -> str:
    """
    generate header for dbt
    """
    transient = "true" if "TEMP" in table_name.upper() else "false"
    return f"""-- transient={transient}
{{{{ config(
    materialized='table',
    transient={transient},
    alias='{table_name}'
) }}}}
"""




# ---------- Split statements ----------
def split_sql_statements(sql: str):
    """
        Cut at ; outside comments (-- and /* */),
    respecting parentheses.
    """
    stmts, buf = [], []
    in_single = in_double = False
    in_line_comment = in_block_comment = False
    depth = 0
    i, n = 0, len(sql)

    while i < n:
        ch = sql[i]
        nxt = sql[i+1] if i+1 < n else ''

        if in_line_comment:
            buf.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(ch)
            if ch == '*' and nxt == '/':
                buf.append(nxt); i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '-' and nxt == '-':
                buf.append(ch); buf.append(nxt); i += 2
                in_line_comment = True
                continue
            if ch == '/' and nxt == '*':
                buf.append(ch); buf.append(nxt); i += 2
                in_block_comment = True
                continue

        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch); i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch); i += 1; continue

        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif ch == ';' and depth == 0:
                buf.append(ch)
                stmts.append(''.join(buf).strip())
                buf.clear()
                i += 1
                continue

        buf.append(ch); i += 1

    rest = ''.join(buf).strip()
    if rest:
        stmts.append(rest)
    return [s for s in stmts if s]

def strip_leading_comments(s: str) -> str:
    """Delete comment (-- ... / /* ... */)."""
    i = 0
    n = len(s)
    while i < n:
        while i < n and s[i].isspace():
            i += 1
        if i + 1 < n and s[i:i+2] == "--":
            j = s.find("\n", i + 2)
            if j == -1:
                return ""
            i = j + 1
            continue
        if i + 1 < n and s[i:i+2] == "/*":
            j = s.find("*/", i + 2)
            if j == -1:
                return ""
            i = j + 2
            continue
        break
    return s[i:]


# ----------Bloc Detection  ----------

RE_INS             = re.compile(r"^\s*INSERT\s+INTO\s+", re.IGNORECASE | re.DOTALL)
RE_INS_VALUES      = re.compile(r"^\s*INSERT\s+INTO\s+.+?\bVALUES\b", re.IGNORECASE | re.DOTALL)
RE_INS_SEL_WITH    = re.compile(r"^\s*INSERT\s+INTO\s+.+?\b(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
RE_SEL_OR_WITH     = re.compile(r"^\s*(?:\(\s*)*(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
RE_TABLE_FROM_INS = re.compile(
    r"""^\s*INSERT\s+INTO\s+
        (                                   
          (?:
            "[^"]+"|[A-Za-z0-9_#]+          
          )
          (?:
            \.
            (?:"[^"]+"|[A-Za-z0-9_#]+) 
          )?
        )
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

RE_CTAS_OR_VIEW    = re.compile(
    r"""^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:TRANSIENT|TEMPORARY|TEMP)\s+)?(?P<kind>TABLE|VIEW)\s+
        (?P<name>[^\s(]+)\s+AS\s+(?P<body>.+)$""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

def is_insert(stmt: str) -> bool:
    return RE_INS.match(stmt) is not None

def is_select_or_with(stmt: str) -> bool:
    head = strip_leading_comments(stmt)
    return RE_SEL_OR_WITH.match(head) is not None

def is_ctas_or_view(stmt: str) -> bool:
    head = strip_leading_comments(stmt)
    return RE_CTAS_OR_VIEW.match(head) is not None


def extract_model_blocks(content: str):
    """
    1 block = 1 statement (INSERT ... SELECT/WITH, INSERT ... VALUES, CTAS/VIEW AS, SELECT/WITH nu).
    """
    blocks = []
    for stmt in split_sql_statements(content):
        head = strip_leading_comments(stmt)
        if is_insert(head) or is_select_or_with(head) or is_ctas_or_view(head):
            if not stmt.endswith(";"):
                stmt += ";"
            blocks.append(stmt)
    return blocks


def table_name_from_block_or_filename(block: str, base_filename: str) -> str:
    """Define table name for dbt and Snowflake"""
    head = strip_leading_comments(block)

    m = RE_TABLE_FROM_INS.match(head)
    if m:
        return m.group(1).split(".")[-1].strip('"')

    m2 = RE_CTAS_OR_VIEW.match(head)
    if m2:
        return m2.group("name").split(".")[-1].strip('"')

    stem = os.path.splitext(os.path.basename(base_filename))[0]
    u = stem.upper()
    for suf in ["_PROC", "_PROCEDURE", "_PRC", "_proc"]:
        if u.endswith(suf):
            stem = stem[:-len(suf)]
            u = stem.upper()
            break
    for pre in ["RECREATE_", "CREATE_"]:
        if u.startswith(pre):
            stem = stem[len(pre):]
            break
    return stem or "UNKNOWN_TABLE"

def _strip_tail_paren_and_semicolon(sql: str) -> str:
    """Delete the ; and ) at the ending"""
    s = re.sub(r";\s*$", "", sql.rstrip())
    s = re.sub(r"\)\s*$", "", s)
    return s.rstrip()


# ---------- Conversion INSERT VALUES -> SELECT UNION ALL ----------

RE_PARSE_INSERT_VALUES = re.compile(
    r"""^\s*INSERT\s+INTO\s+
        (?P<table>[^\s(]+)
        \s*
        (?P<cols>\((?P<cols_inner>.*?)\))?
        \s*VALUES\s*(?P<values>.+);?\s*$""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

def _split_top_level_tuples(values_str: str):
    """
    Split different procedure
    """
    tuples = []
    in_single = False
    esc = False
    depth = 0
    cur = []
    i = 0
    while i < len(values_str):
        ch = values_str[i]
        cur.append(ch)
        if ch == "\\" and not esc:
            esc = True
            i += 1
            continue
        if not esc:
            if ch == "'" and depth >= 0:
                in_single = not in_single
            elif not in_single:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        tuples.append("".join(cur).strip())
                        cur = []
                        j = i + 1
                        while j < len(values_str) and values_str[j].isspace():
                            j += 1
                        if j < len(values_str) and values_str[j] == ",":
                            j += 1
                            while j < len(values_str) and values_str[j].isspace():
                                j += 1
                            i = j - 1
        esc = False
        i += 1

    tail = "".join(cur).strip()
    if tail:
        tuples.append(tail)

    clean = []
    for t in tuples:
        t = t.strip().rstrip(",").strip()
        if t.startswith("(") and t.endswith(")"):
            clean.append(t[1:-1].strip())
    return clean


def _split_top_level_commas(exprs: str):
    parts = []
    buf = []
    in_single = False
    esc = False
    depth = 0
    for ch in exprs:
        if ch == "\\" and not esc:
            esc = True
            buf.append(ch)
            continue
        if not esc:
            if ch == "'" and depth >= 0:
                in_single = not in_single
            elif not in_single:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif ch == "," and depth == 0:
                    parts.append("".join(buf).strip())
                    buf = []
                    continue
        buf.append(ch)
        esc = False
    rest = "".join(buf).strip()
    if rest:
        parts.append(rest)
    return parts


def convert_insert_values_to_select_union(block: str) -> str:
    m = RE_PARSE_INSERT_VALUES.match(block)
    if not m:
        return block.strip().rstrip(";")

    cols_inner = m.group("cols_inner")
    values_str = m.group("values")

    tuples = _split_top_level_tuples(values_str)
    if not tuples:
        return block.strip().rstrip(";")

    rows = [_split_top_level_commas(t) for t in tuples]
    ncols = len(rows[0])
    if any(len(r) != ncols for r in rows):
        return block.strip().rstrip(";")

    if cols_inner:
        col_names = [c.strip() for c in _split_top_level_commas(cols_inner)]
    else:
        col_names = [f"COL{i+1}" for i in range(ncols)]

    selects = []
    for idx, r in enumerate(rows):
        if idx == 0:
            pairs = [f"{r[j].strip()} AS {col_names[j]}" for j in range(ncols)]
        else:
            pairs = [r[j].strip() for j in range(ncols)]
        selects.append("SELECT " + ", ".join(pairs))
    return "\nUNION ALL\n".join(selects)


# ---------- DBT Standardization ----------

def normalize_block_for_dbt(block: str) -> str:
    head = strip_leading_comments(block)

    # INSERT ... (SELECT|WITH)
    if RE_INS_SEL_WITH.match(head):
        m_select = re.search(r"\bSELECT\b", head, flags=re.IGNORECASE)
        m_with = re.search(r"\bWITH\b", head, flags=re.IGNORECASE)
        idx = None
        if m_select and m_with:
            idx = min(m_select.start(), m_with.start())
        elif m_select:
            idx = m_select.start()
        elif m_with:
            idx = m_with.start()
        if idx is not None:
            return _strip_tail_paren_and_semicolon(head[idx:])

    # INSERT ... VALUES
    if RE_INS_VALUES.match(head):
        return convert_insert_values_to_select_union(head)

    # CTAS / CREATE VIEW AS
    m = RE_CTAS_OR_VIEW.match(head)
    if m:
        body = m.group("body")
        ms = re.search(r"\bSELECT\b", body, flags=re.IGNORECASE)
        mw = re.search(r"\bWITH\b", body, flags=re.IGNORECASE)
        idx = None
        if ms and mw:
            idx = min(ms.start(), mw.start())
        elif ms:
            idx = ms.start()
        elif mw:
            idx = mw.start()
        if idx is not None:
            return _strip_tail_paren_and_semicolon(body[idx:])
        return _strip_tail_paren_and_semicolon(body)

    # SELECT / WITH nu
    if RE_SEL_OR_WITH.match(head):
        return head.strip().rstrip(";")

    # fallback
    return head.strip().rstrip(";")


# ---------- Traitement d'un fichier ----------

def process_sql_file(file_path: str, output_dir: str) -> bool:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            print(f"[ERROR] {file_path}: Failed to read file due to encoding issue - {e}")
            return False

    blocks = extract_model_blocks(content)
    if not blocks:
        print(f"[SKIPPED] {os.path.basename(file_path)}: No INSERT/SELECT/CTAS/VIEW block found.")
        return False

    base_filename = os.path.basename(file_path)
    stem = os.path.splitext(base_filename)[0]
    os.makedirs(output_dir, exist_ok=True)

    for i, raw_block in enumerate(blocks, start=1):
        table_name = table_name_from_block_or_filename(raw_block, base_filename)
        header = generate_dbt_config(table_name)
        body = normalize_block_for_dbt(raw_block)

        dbt_model = header + "\n" + body + "\n"
        dbt_model = transform_table_references(dbt_model)
        dbt_model = normalize_oracle_rownum_sysdate(dbt_model)

        out_name = f"{stem}.sql" if len(blocks) == 1 else f"{stem}_pt{i}.sql"
        out_path = os.path.join(output_dir, out_name)

        try:
            with open(out_path, "w", encoding="utf-8") as out_f:
                out_f.write(dbt_model)
        except Exception as e:
            print(f"[ERROR] {out_name}: Failed to write output file - {e}")

    return True


def main():
    if len(sys.argv) != 2:
        print("Usage: python convert_to_dbt.py <path_to_sql_folder>")
        return

    input_folder = sys.argv[1]
    if not os.path.isdir(input_folder):
        print(f"Error: {input_folder} is not a valid directory.")
        return

    output_dir = "dbt_models"
    os.makedirs(output_dir, exist_ok=True)

    total_files = 0
    converted_files = 0
    skipped_files = []

    for filename in os.listdir(input_folder):
        if filename.lower().endswith(".sql"):
            total_files += 1
            file_path = os.path.join(input_folder, filename)
            success = process_sql_file(file_path, output_dir)
            if success:
                converted_files += 1
            else:
                skipped_files.append(filename)

    print("\n=== Conversion Summary ===")
    print(f"Total SQL files found: {total_files}")
    print(f"Successfully converted: {converted_files}")
    print(f"Skipped files: {len(skipped_files)}")
    if skipped_files:
        print("Skipped file list:")
        for fname in skipped_files:
            print(f" - {fname}")
        skipped_path = os.path.join(output_dir, "skipped_files.txt")
        with open(skipped_path, "w", encoding="utf-8") as sf:
            for fname in skipped_files:
                sf.write(fname + "\n")
        print(f"Skipped files saved to: {skipped_path}")


if __name__ == "__main__":
    main()