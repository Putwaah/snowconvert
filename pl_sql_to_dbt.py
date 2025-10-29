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
            "seq8() + 1 AS ROW_NUMBER_ID,\n "
            "current_timestamp() AS ROW_CREATION_DATE,\n "
            "current_timestamp() AS ROW_LAST_UPDATE_DATE\n"
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
def transform_table_references(sql, mode):
        """Transforms table references with the appropriate schemas"""
        def replacer(match):
            keyword = match.group(1)
            table = match.group(2)

            if table.lower() == 'steph_apps_fnd_flex_values#_bz':
                return f"{keyword} DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES"


            if mode == "bronze to silver":
                if table.lower().endswith('_bz'):
                    table_name = table[:-3]
                    return f"{keyword} DEV.LH2_BRONZE_DEV.{table_name}"

                return f"{keyword} DEV.LH2_SILVER_DEV.{table}"

            if mode == "silver to gold":
                if table.lower().endswith('_sv'):
                    table_name = table[:-3]
                    return f"{keyword} DEV.LH2_SILVER_DEV.{table_name}"

                return f"{keyword} DEV.LH2_GOLD_DEV.{table}"

        
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

def strip_optimizer_hints(s: str) -> str:
    """
    Supprime les hints Oracle /*+ ... */ où qu'ils se trouvent dans le bloc.
    Non-greedy et multi-lignes.
    Ex: INSERT /*+ APPEND PARALLEL(t 8) */ INTO t ...
    """
    return re.sub(r"/\*\+.*?\*/", "", s, flags=re.DOTALL)

# ----------------Replace Function---------------
MACRO_NAMESPACE = {
    'SILVER': 'silver_funcs',
    'GOLD':   'gold_funcs',
}

PKG_FUNC_START = re.compile(
    r"\bLH2_DTH_(SILVER|GOLD)_FUNCTIONS_PKG\.([A-Za-z0-9_]+)\s*\(",
    re.IGNORECASE
)

def _find_matching_paren(s: str, open_idx: int) -> int:
    """
    Retourne l'index de la parenthèse fermante qui matche s[open_idx] == '(',
    en respectant quotes simples/doubles et parenthèses imbriquées.
    -1 si non trouvé.
    """
    assert s[open_idx] == '(', "open_idx must point to '('"
    depth = 0
    in_single = in_double = False
    i = open_idx
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1

def transform_pkg_functions_to_macros(sql: str) -> str:
    """
    Remplace tout appel à LH2_DTH_{SILVER|GOLD}_FUNCTIONS_PKG.<FUNC>(args)
    par {{ {silver|gold}_funcs.<func>(args) }} et ajoute un rappel en commentaire.
    """
    out = []
    i = 0
    n = len(sql)
    while i < n:
        m = PKG_FUNC_START.search(sql, i)
        if not m:
            out.append(sql[i:])
            break

        # ajouter la partie avant la fonction
        out.append(sql[i:m.start()])

        env = m.group(1).upper()   # SILVER | GOLD
        func = m.group(2)          # nom de la fonction
        ns = MACRO_NAMESPACE.get(env, env.lower() + "_funcs")

        # m.end() est après '(' -> position de '(' = m.end() - 1
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(sql, open_paren)
        if close_paren == -1:
            out.append(sql[m.start():])
            break

        args_str = sql[open_paren + 1: close_paren]
        original_call = sql[m.start(): close_paren + 1]
        macro_call = f"{{{{ {ns}.{func.lower()}({args_str}) }}}} /* ORA_FUNC: {original_call} */"

        out.append(macro_call)
        i = close_paren + 1

    return "".join(out)


# --- Bloc Detection (robuste) ---
RE_INS = re.compile(r"^\s*INSERT\s+INTO\s+", re.IGNORECASE | re.DOTALL)
RE_INS_VALUES = re.compile(r"^\s*INSERT\s+INTO\s+.+?\bVALUES\b", re.IGNORECASE | re.DOTALL)
RE_INS_SEL_WITH = re.compile(r"^\s*INSERT\s+INTO\s+.+?\b(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
RE_SEL_OR_WITH = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)

# INSERT INTO <table> (schéma/quotes autorisés)
RE_TABLE_FROM_INS = re.compile(
    r"""^\s*INSERT\s+INTO\s+
        (
          (?:"[^"]+"|[A-Za-z0-9_#]+)
          (?:\.(?:"[^"]+"|[A-Za-z0-9_#]+))?
        )
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

RE_CTAS_OR_VIEW = re.compile(
    r"""^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:TRANSIENT|TEMPORARY|TEMP)\s+)?(?P<kind>TABLE|VIEW)\s+
        (?P<name>[^\s(]+)\s+AS\s+(?P<body>.+)$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

def is_insert(stmt: str) -> bool:
    cleaned = strip_leading_comments(stmt)
    cleaned = strip_optimizer_hints(cleaned)
    return RE_INS.match(cleaned) is not None

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
    head = strip_optimizer_hints(head)

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

def _strip_tail_paren_and_semicolon(sql: str) -> str:
    s = re.sub(r";\s*$", "", sql.rstrip())
    s = re.sub(r"\)\s*$", "", s)
    return s.rstrip()

def normalize_block_for_dbt(block: str) -> str:
    """
    Extrait le SELECT/WITH utile pour dbt selon 4 cas :
      1) INSERT ... (SELECT|WITH) ...           -> on garde la partie SELECT/WITH
      2) INSERT ... VALUES (...)                 -> on convertit en SELECT ... UNION ALL ...
      3) CTAS/CREATE VIEW AS SELECT ...         -> on garde la partie SELECT/WITH
      4) SELECT/WITH nu                         -> on garde tel quel
    """
    # Nettoyage tête + hints pour fiabiliser les regex
    head = strip_leading_comments(block)
    head = strip_optimizer_hints(head)

    # 1) INSERT ... (SELECT|WITH)
    if RE_INS_SEL_WITH.match(head):
        ms = re.search(r"\bSELECT\b", head, flags=re.IGNORECASE)
        mw = re.search(r"\bWITH\b", head, flags=re.IGNORECASE)
        idx = None
        if ms and mw:
            idx = min(ms.start(), mw.start())
        elif ms:
            idx = ms.start()
        elif mw:
            idx = mw.start()
        if idx is not None:
            return _strip_tail_paren_and_semicolon(head[idx:])

    # 2) INSERT ... VALUES
    if RE_INS_VALUES.match(head):
        return convert_insert_values_to_select_union(head)

    # 3) CTAS / VIEW AS
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

    # 4) SELECT / WITH nu
    if RE_SEL_OR_WITH.match(head):
        return head.strip().rstrip(";")

    # Fallback: on retourne quelque chose (évite None)
    return head.strip().rstrip(";")

# Macros à exclure systématiquement de l'injection en CTE (tu peux adapter)
EXCLUDE_MACROS = {
    "config", "ref", "source", "var", "env_var",  # primitives dbt usuelles
    "this", "target", "adapter"                   # pas des macros de CTE
}


RE_JINJA_MACRO_CALL = re.compile(
    r"\{\{\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*\(([^}]*)\)\s*\}\}",
    re.DOTALL
)

def inject_macro_ctes(sql: str,
                      only_suffix: str = "_cte",
                      forced_macro_calls: list[str] | None = None) -> str:
    """
    Injection de CTE à partir :
      1) des macros présentes dans le SQL (filtrées par only_suffix si non vide),
      2) des macros *forcées* listées dans `forced_macro_calls` (injectées même si non présentes).

    Règles :
      - CTE = <base_name> AS {{ <call> }}, où base_name = dernier identifiant du nom de macro (sans module).
      - Si WITH existe -> on préprend nos CTE juste après 'WITH'.
      - Sinon -> on crée un 'WITH ...' en tête.
      - Anti-doublon : si '<base_name> AS {{' existe déjà, on n’injecte pas.
      - Ne fait rien si le SQL ne contient ni SELECT ni WITH.
    """
    # Normalisation de type
    if sql is None:
        return ""
    if not isinstance(sql, str):
        try:
            sql = sql.decode("utf-8") if isinstance(sql, (bytes, bytearray)) else str(sql)
        except Exception:
            return ""

    # Sans SELECT/WITH -> pas d’injection
    if not re.search(r"\b(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return sql

    candidates: dict[str, str] = {}

    # 1) Macros présentes dans le SQL
    for m in RE_JINJA_MACRO_CALL.finditer(sql):
        full_name = m.group(1).strip()   # "macro" ou "module.macro"
        call      = m.group(0).strip()   # "{{ module.macro(args) }}"
        base_name = full_name.split(".")[-1]
        if only_suffix and not base_name.endswith(only_suffix):
            continue
        candidates.setdefault(base_name, call)

    # 2) Macros FORCÉES (liste d'appels)
    forced_macro_calls = forced_macro_calls or []
    for call in forced_macro_calls:
        mc = RE_JINJA_MACRO_CALL.search(call)
        if not mc:
            # Appel mal formé -> on ignore
            continue
        full_name = mc.group(1).strip()
        base_name = full_name.split(".")[-1]
        candidates.setdefault(base_name, call.strip())

    if not candidates:
        return sql

    # 3) Filtrer celles déjà définies comme CTE "<base_name> AS {{"
    to_inject: list[str] = []
    for base_name, call in candidates.items():
        if re.search(rf"\b{re.escape(base_name)}\b\s+as\s+\{{\{{", sql, flags=re.IGNORECASE):
            continue
        to_inject.append(f"{base_name} AS {call}")

    if not to_inject:
        return sql

    # 4) Injection en tête de WITH, ou création d'un WITH
    if re.match(r"^\s*with\b", sql, flags=re.IGNORECASE):
        m_head = re.match(r"^\s*with\s*", sql, flags=re.IGNORECASE)
        pos = m_head.end()
        injected = ",\n".join(to_inject) + ",\n"
        return sql[:pos] + injected + sql[pos:]
    else:
        return "WITH " + ",\n".join(to_inject) + "\n" + sql
    

def ensure_cte(sql: str, name: str, call: str) -> str:
    """
    Garantit la présence d'une CTE: <name> AS <call>.
    - Si déjà définie, ne fait rien.
    - Si WITH existe, préprend "name AS call," juste après WITH.
    - Sinon, crée un WITH ... avant le SELECT/WITH existant.
    """
    if sql is None:
        return ""
    if not isinstance(sql, str):
        sql = str(sql)

    # Déjà présente ?
    if re.search(rf"\b{name}\b\s+as\s+\{{\{{", sql, flags=re.IGNORECASE):
        return sql

    # Rien à faire si le body ne contient ni SELECT ni WITH
    if not re.search(r"\b(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return sql

    if re.match(r"^\s*with\b", sql, flags=re.IGNORECASE):
        pos = re.match(r"^\s*with\s*", sql, flags=re.IGNORECASE).end()
        return sql[:pos] + f"{name} AS {call},\n" + sql[pos:]
    else:
        return f"WITH {name} AS {call}\n{sql}"


# Remplace la macro scalaire par la colonne issue de la CTE
RE_PO_BUYER_SCALAR = re.compile(
    r"""\{\{[^}]*get_steph_apps_per_all_people_f_name_func\s*\([^)]*\)\s*\}\}
        \s*(?:/\*.*?\*/\s*)?
        (?:AS\s+)?PO_BUYER\b
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

def replace_po_buyer_scalar_with_cte(sql: str) -> str:
    """
    Remplace l'appel scalaire de BUYER par l'expression basée sur la CTE:
      coalesce(global_name_cte.global_name, 'NOT FOUND') AS PO_BUYER
    Supporte les variantes avec ou sans 'AS' et éventuel commentaire ORA_FUNC.
    """
    return RE_PO_BUYER_SCALAR.sub(
        "coalesce(global_name_cte.global_name, 'NOT FOUND') AS PO_BUYER",
        sql or ""
    )


def ensure_left_join_global_name(sql: str) -> str:
    """
    Ajoute 'LEFT JOIN global_name_cte ON global_name_cte.person_id = pha.AGENT_ID'
    juste AVANT le premier WHERE si le JOIN n'existe pas déjà.
    """
    if sql is None:
        return ""
    if not isinstance(sql, str):
        sql = str(sql)

    # JOIN déjà présent ?
    if re.search(r"\bjoin\s+global_name_cte\b", sql, flags=re.IGNORECASE):
        return sql

    join_line = "\nleft join global_name_cte on global_name_cte.person_id = pha.AGENT_ID\n"
    m_where = re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE)
    if m_where:
        return sql[:m_where.start()] + join_line + sql[m_where.start():]
    else:
        return sql + join_line


def apply_model_specific_rules(table_name: str, sql: str) -> str:
    """
    Règles spécifiques par modèle.
    - Pour 'PO_CONTRACT':
        1) force CTE 'global_name_cte as {{ get_global_name_cte() }}'
        2) remplace PO_BUYER scalaire par coalesce(global_name_cte.global_name, 'NOT FOUND')
        3) garantit le LEFT JOIN global_name_cte ... pha.AGENT_ID
    """
    if not sql:
        return sql

    if (table_name or "").upper() == "PO_CONTRACT":
        sql = ensure_cte(sql, "global_name_cte", "{{ get_global_name_cte() }}")
        sql = replace_po_buyer_scalar_with_cte(sql)
        sql = ensure_left_join_global_name(sql)

    return sql

# ---------- Traitement d'un fichier ----------

def process_sql_file(file_path: str, output_dir: str, mode: str) -> bool:
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
        # 1) Normalisation principale
        body = normalize_block_for_dbt(raw_block)

        # 4) Remplacement package -> macros scalaires
        body = transform_pkg_functions_to_macros(body)

        # 5) Suite (réécriture FROM/JOIN + normalisation)
        dbt_model = header + "\n" + body + "\n"
        dbt_model = transform_table_references(dbt_model, mode)
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
    
    print("Choose the conversion layer :")
    print("1) Bronze → Silver")
    print("2) Silver → Gold")
    choice = input("1 or 2 : ").strip()

    if choice == "1":
        mode = "bronze to silver"
    elif choice == "2":
        mode = "silver to gold"
    else:
        print("Invalid number. Default : Bronze to Silver.")
        mode = "bronze to silver"

    output_dir = "dbt_models"
    os.makedirs(output_dir, exist_ok=True)

    total_files = 0
    converted_files = 0
    skipped_files = []

    for filename in os.listdir(input_folder):
        if filename.lower().endswith(".sql"):
            total_files += 1
            file_path = os.path.join(input_folder, filename)
            success = process_sql_file(file_path, output_dir, mode)
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


if __name__ == "__main__":
    main()