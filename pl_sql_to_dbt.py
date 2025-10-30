import os
import re
import sys



# --- REPLACE ROWNUM / SYSDATE ---
RE_TRIPLE_ROWNUM_SYSDATE = re.compile(r"""
    \bROWNUM\b \s+ (?:AS\s+)?ROW_NUMBER_ID \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_CREATION_DATE \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_LAST_UPDATE_DATE
""", re.IGNORECASE | re.VERBOSE | re.DOTALL)

def derive_cte_name_and_alias(macro_full_name: str) -> tuple[str, str, str]:
    """
    A partir du nom complet de macro 'module.func' ou 'func', dérive:
      - cte_name : 'cte_<func>'
      - alias    : 'm_<func>'
      - out_col  : '<func>_out'
    Toutes en snake_case. Ex: 'silver_funcs.get_country_code_func' ->
      ('cte_get_country_code_func', 'm_get_country_code_func', 'get_country_code_func_out')
    """
    func = macro_full_name.split('.')[-1]
    base = re.sub(r'[^A-Za-z0-9_]+', '_', func).lower()
    return f"cte_{base}", f"m_{base}", f"{base}_out"

# --- ANSI JOIN rewrite for old Oracle (+) syntax ---

def _is_kw_at(s: str, i: int, kw: str) -> bool:
    n = len(s)
    j = i + len(kw)
    before = s[i-1] if i > 0 else ' '
    after  = s[j]   if j < n else ' '
    return (s[i:j].lower() == kw.lower()
            and not (before.isalnum() or before == '_')
            and not (after.isalnum()  or after  == '_'))

def _find_top_level_keyword_positions(sql: str, start_kw: str, next_kws: list[str]):
    s = sql
    n = len(s)
    start = start_kw.lower()
    nexts = [kw.lower() for kw in next_kws]
    in_single = in_double = False
    depth = 0
    i = 0
    found_from = -1

    while i < n:
        ch = s[i]
        ch2 = s[i:i+2]
        if ch == "'" and not in_double:
            in_single = not in_single; i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double; i += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth == 0:
                # *** ici : vérif stricte des frontières ***
                if _is_kw_at(s, i, start):
                    found_from = i
                    i += len(start)
                    break
        i += 1
    if found_from < 0:
        return -1, None

    # Suite inchangée, mais applique la même logique pour `next_kws`
    j = i
    best = None
    in_single = in_double = False
    depth = 0
    while j < n:
        ch = s[j]
        if ch == "'" and not in_double:
            in_single = not in_single; j += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double; j += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth == 0:
                for kw in nexts:
                    if _is_kw_at(s, j, kw):
                        best = j
                        return found_from, best
        j += 1
    return found_from, None

    # Cherche prochain mot-clé
    j = i
    best = None
    while j < n:
        ch = s[j]
        if ch == "'" and not in_double:
            in_single = not in_single
            j += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            j += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth == 0:
                for kw in nexts:
                    if s[j:].lower().startswith(kw) and (j == 0 or not s[j-1].isalnum()):
                        best = j
                        return found_from, best
        j += 1
    return found_from, None


def _split_top_level_commas(s: str):
    """Split par virgule au niveau top-level (hors quotes/parenthèses)."""
    parts, buf = [], []
    in_single = in_double = False
    depth = 0
    i, n = 0, len(s)
    while i < n:
        ch = s[i]
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
            elif ch == ',' and depth == 0:
                parts.append(''.join(buf).strip())
                buf = []
                i += 1
                continue
        buf.append(ch); i += 1
    rest = ''.join(buf).strip()
    if rest:
        parts.append(rest)
    return parts


def _split_top_level_and(s: str):
    """Split par AND au niveau top-level (hors quotes/parenthèses)."""
    parts, buf = [], []
    in_single = in_double = False
    depth = 0
    i, n = 0, len(s)
    while i < n:
        # try keyword AND (case-insensitive), ensure token boundary
        if not in_single and not in_double and depth == 0:
            if s[i:].lower().startswith('and') and (i == 0 or not s[i-1].isalnum()):
                # check next char boundary
                j = i + 3
                if j >= n or not s[j].isalnum():
                    parts.append(''.join(buf).strip())
                    buf = []
                    i = j
                    continue
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
        buf.append(ch); i += 1
    rest = ''.join(buf).strip()
    if rest:
        parts.append(rest)
    # drop empties
    return [p for p in parts if p]


def _parse_from_items(from_clause: str):
    """
    Parse chaque item du FROM 'table [AS] alias' ou 'schema.table alias'.
    Retourne:
      - items: liste de strings d'origine (pour réécriture)
      - alias_to_text: {alias_lower: item_text}
      - aliases: set des alias_lower
    """
    items = _split_top_level_commas(from_clause)
    alias_to_text = {}
    aliases = []
    for it in items:
        m = re.match(r'^\s*([A-Za-z0-9_."#]+(?:\s*\.\s*[A-Za-z0-9_."#]+)*)\s*(?:AS\s+)?([A-Za-z0-9_."#]+)?\s*$', it, flags=re.IGNORECASE)
        if m:
            tbl = m.group(1).strip()
            alias = m.group(2).strip() if m.group(2) else None
            if not alias:
                # alias implicite = dernier identifiant du nom de table
                alias = re.split(r'\s*\.\s*', tbl)[-1].strip('"')
            alias_lower = alias.lower()
            alias_to_text[alias_lower] = it.strip()
            aliases.append(alias_lower)
        else:
            # cas exotique -> on garde tel quel, pas d'alias détecté
            pass
    return items, alias_to_text, set(aliases)


def _detect_alias(expr: str, aliases: set[str]) -> str | None:
    """
    Renvoie l'alias (lower) trouvé dans expr en cherchant 'alias.'.
    On trie par longueur décroissante pour éviter les collisions de préfixe.
    """
    for al in sorted(aliases, key=len, reverse=True):
        if re.search(rf'(?<![A-Za-z0-9_]){re.escape(al)}\s*\.', expr, flags=re.IGNORECASE):
            return al
    return None

def _mask_sql_comments_keep_layout(s: str) -> str:
    """
    Remplace le contenu des commentaires par des espaces en conservant la mise en page
    (même nombre de \n), pour garder les index alignés avec la chaîne originale.
    """
    if not s:
        return s
    out = []
    i = 0
    n = len(s)
    in_single = in_double = False
    while i < n:
        ch = s[i]
        nxt = s[i+1] if i+1 < n else ''
        # quotes
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch); i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch); i += 1; continue
        # commentaires (hors quotes)
        if not in_single and not in_double:
            # -- line comment
            if ch == '-' and nxt == '-':
                # remplacer jusqu'au \n par des espaces, conserver le \n
                j = i + 2
                while j < n and s[j] != '\n':
                    out.append(' ')
                    j += 1
                out.append('\n' if j < n else '')
                i = j + 1
                continue
            # /* block comment */
            if ch == '/' and nxt == '*':
                j = i + 2
                while j + 1 < n and not (s[j] == '*' and s[j+1] == '/'):
                    out.append(' ' if s[j] != '\n' else '\n')
                    j += 1
                if j + 1 < n:
                    # ajouter '*/' masqué
                    out.append(' '); out.append(' ')
                    j += 2
                i = j
                continue
        out.append(ch); i += 1
    return ''.join(out)


def _split_top_level_and_spans(s: str):
    """
    Split par AND au niveau top-level, en renvoyant (texte, start, end).
    's' doit être déjà comment-maské pour éviter d'attraper des AND commentés.
    """
    parts = []
    buf = []
    in_single = in_double = False
    depth = 0
    n = len(s)
    i = 0
    seg_start = 0
    while i < n:
        # nouveau mot-clé AND top-level ?
        if not in_single and not in_double and depth == 0:
            if s[i:].lower().startswith('and') and (i == 0 or not s[i-1].isalnum()):
                j = i + 3
                if j >= n or not s[j].isalnum():
                    # flush segment courant
                    seg = ''.join(buf).strip()
                    if seg:
                        parts.append((seg, seg_start, i))
                    buf = []
                    # skip AND
                    i = j
                    # nouveau segment
                    while i < n and s[i].isspace():
                        i += 1
                    seg_start = i
                    continue
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
        buf.append(ch); i += 1
    seg = ''.join(buf).strip()
    if seg:
        parts.append((seg, seg_start, n))
    return parts


def rewrite_oracle_plus_joins(
    sql: str,
    debug: bool = False,
    rewrite_inner: bool = True,
    drop_plus_constant_filters: bool = False,
) -> str:
    """
    Réécrit :
      - 'A = B(+)' -> LEFT/RIGHT JOIN ... ON ...
      - 'A = B' (implicite) -> INNER JOIN ... ON ... (si rewrite_inner=True)
      - Appels de macro Jinja dans une égalité -> injection d'un LEFT JOIN sur une CTE GENERIQUE
        dont le nom/alias/colonne sont dérivés du nom de la fonction :
           {{ module.func(arg) }} = T.col  ==>  LEFT JOIN cte_func m_func ON {{...}} = m_func.func_out
        Puis on remplace la macro par m_func.func_out pour permettre la jointure suivante.
      - Nettoie '(+)', '1=1', 'ON AND ...', 'WHERE ... AND' résiduels.
    """

    if not sql or not isinstance(sql, str):
        return sql

    # --- localiser FROM ... WHERE (niveau top) ---
    idx_from, idx_where = _find_top_level_keyword_positions(sql, 'FROM', ['WHERE'])
    if idx_from < 0 or idx_where is None:
        return sql

    tail_kws = ['GROUP BY', 'ORDER BY', 'QUALIFY', 'LIMIT', 'UNION', 'MINUS', 'INTERSECT']
    _, idx_tail = _find_top_level_keyword_positions(sql[idx_where:], 'WHERE', tail_kws)
    end_where = (idx_where + idx_tail) if idx_tail is not None else len(sql)

    from_raw = sql[idx_from + len('FROM'): idx_where]
    where_raw = sql[idx_where + len('WHERE'): end_where]

    # --- masquer commentaires, on garde le layout ---
    from_mask = _mask_sql_comments_keep_layout(from_raw)
    where_mask = _mask_sql_comments_keep_layout(where_raw)

    # --- parse FROM items (actifs) ---
    items = _split_top_level_commas(from_mask.strip())
    items = [it for it in items if it and not it.strip().startswith('--')]
    alias_to_text = {}
    aliases = set()
    for it in items:
        m = re.match(
            r'^\s*([A-Za-z0-9_."#]+(?:\s*\.\s*[A-Za-z0-9_."#]+)*)\s*(?:AS\s+)?([A-Za-z0-9_."#]+)?\s*$',
            it, flags=re.IGNORECASE
        )
        if not m:
            continue
        tbl = m.group(1).strip()
        alias = (m.group(2).strip() if m.group(2) else re.split(r'\s*\.\s*', tbl)[-1].strip('"'))
        alias_l = alias.lower()
        alias_to_text[alias_l] = it.strip()
        aliases.add(alias_l)

    if not items or not aliases:
        return sql

    # --- split WHERE conditions actives ---
    conds_spans = _split_top_level_and_spans(where_mask)

    # groupement des joins
    join_groups: dict[tuple, list[str]] = {}
    consumed_spans = set()
    filters: list[str] = []
    extra_joins: list[str] = []  # JOIN CTE dérivées des macros

    def _alias_of(expr: str) -> str | None:
        for al in sorted(aliases, key=len, reverse=True):
            if re.search(rf'(?<![A-Za-z0-9_]){re.escape(al)}\s*\.', expr, flags=re.IGNORECASE):
                return al
        return None

    def _strip_leading_and(txt: str) -> str:
        return re.sub(r'^\s*and\b', '', txt, flags=re.IGNORECASE).strip()

    def _clean_side(s: str) -> str:
        return _strip_leading_and(s.replace('(+)',' ').strip())

    # Jinja macro : {{ module.func(args) }}
    JINJA_CALL_RE = re.compile(r"\{\{\s*([A-Za-z_][\w\.]*)\s*\((.*?)\)\s*\}\}")

    for _, c_start, c_end in conds_spans:
        raw = where_raw[c_start:c_end]
        raw_nocom = _mask_sql_comments_keep_layout(raw).strip()

        # coupe sur '=' top-level
        in_single = in_double = False
        depth = 0
        eq_pos = -1
        for i, ch in enumerate(raw_nocom):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == '=' and depth == 0:
                    eq_pos = i; break

        if eq_pos < 0:
            val = _strip_leading_and(raw_nocom)
            if val:
                filters.append(val)
            continue

        lhs_raw = raw_nocom[:eq_pos].strip()
        rhs_raw = raw_nocom[eq_pos+1:].strip()
        lhs_plus = '(+)' in lhs_raw
        rhs_plus = '(+)' in rhs_raw

        lhs = _clean_side(lhs_raw)
        rhs = _clean_side(rhs_raw)

        # --- macro Jinja détectée ? ---
        m_left = JINJA_CALL_RE.search(lhs)
        m_right = JINJA_CALL_RE.search(rhs)
        if m_left or m_right:
            mj = m_left or m_right
            macro_full_name = mj.group(1)         # ex: silver_funcs.get_country_code_func
            macro_call = mj.group(0)              # ex: {{ silver_funcs.get_country_code_func(KDOHR.BILCOUNTRY) }}
            cte_name, cte_alias, out_col = derive_cte_name_and_alias(macro_full_name)

            # Injecte un LEFT JOIN sur la CTE générique : {{ macro(...) }} = cte_alias.out_col
            extra_joins.append(
                f"\nLEFT JOIN {cte_name} {cte_alias} ON {macro_call} = {cte_alias}.{out_col}"
            )
            # On remplace la macro par cte_alias.out_col pour la suite (afin que la condition restante soit joinable)
            if m_left:
                lhs = f"{cte_alias}.{out_col}"
            else:
                rhs = f"{cte_alias}.{out_col}"

            # Ajoute l'alias aux alias connus (sinon _alias_of ne le verra pas)
            aliases.add(cte_alias.lower())
            alias_to_text.setdefault(cte_alias.lower(), f"{cte_name} {cte_alias}")

        # Détection d'alias
        lhs_alias = _alias_of(lhs)
        rhs_alias = _alias_of(rhs)

        # Comparaison vers constante avec (+)
        if (lhs_alias and not rhs_alias or rhs_alias and not lhs_alias) and (lhs_plus or rhs_plus):
            if drop_plus_constant_filters:
                consumed_spans.add((c_start, c_end))
                continue
            filters.append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # OUTER Oracle
        if lhs_alias and rhs_alias and lhs_alias != rhs_alias and (lhs_plus ^ rhs_plus):
            key = (lhs_alias, rhs_alias, 'LEFT') if (rhs_plus and not lhs_plus) else (rhs_alias, lhs_alias, 'RIGHT')
            join_groups.setdefault(key, []).append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # INNER implicite
        if rewrite_inner and lhs_alias and rhs_alias and lhs_alias != rhs_alias and not lhs_plus and not rhs_plus:
            a, b = sorted([lhs_alias, rhs_alias])
            key = (a, b, 'INNER')
            join_groups.setdefault(key, []).append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # sinon -> filtre
        filters.append(f"{lhs} = {rhs}")

    # --- construction FROM + JOINs ---
    first_item = items[0].strip()
    m_first = re.match(
        r'^\s*(?:[A-Za-z0-9_."#]+(?:\s*\.\s*[A-Za-z0-9_."#]+)*)\s*(?:AS\s+)?([A-Za-z0-9_."#]+)?',
        first_item, flags=re.IGNORECASE
    )
    if m_first and m_first.group(1):
        base_alias = m_first.group(1).strip().strip('"').lower()
    else:
        base_alias = re.split(r'\s*\.\s*', first_item)[-1].strip('"').lower()

    from_parts = [first_item]
    if extra_joins:
        from_parts.extend(extra_joins)  # on met les JOIN-CTE dès le début
    joined = {base_alias}
    for j in extra_joins:
        # marque leurs alias comme rejoints
        m_al = re.search(r"\sJOIN\s+[A-Za-z0-9_\.\"#]+\s+([A-Za-z0-9_\"#]+)\s+ON", j, flags=re.IGNORECASE)
        if m_al:
            joined.add(m_al.group(1).strip('"').lower())

    remaining = dict(join_groups)
    progressed = True
    while progressed and remaining:
        progressed = False
        for key, cond_list in list(remaining.items()):
            a_al, b_al, jtype = key
            # on nettoie 'AND' en tête dans chaque condition
            conds = [re.sub(r'^\s*and\b', '', c, flags=re.IGNORECASE).strip() for c in cond_list]
            cond_txt = " AND ".join([c for c in conds if c])

            if jtype == 'LEFT':
                l_al, r_al = a_al, b_al
                if (l_al in joined) and (r_al not in joined):
                    tbl_txt = alias_to_text.get(r_al, r_al)
                    from_parts.append(f"\nLEFT JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(r_al)
                    remaining.pop(key)
                    progressed = True

            elif jtype == 'RIGHT':
                l_al, r_al = a_al, b_al
                if (r_al in joined) and (l_al not in joined):
                    tbl_txt = alias_to_text.get(l_al, l_al)
                    from_parts.append(f"\nRIGHT JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(l_al)
                    remaining.pop(key)
                    progressed = True

            elif jtype == 'INNER':
                if (a_al in joined) ^ (b_al in joined):
                    other = b_al if a_al in joined else a_al
                    tbl_txt = alias_to_text.get(other, other)
                    from_parts.append(f"\nINNER JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(other)
                    remaining.pop(key)
                    progressed = True
                elif (a_al in joined) and (b_al in joined):
                    filters.append(cond_txt)
                    remaining.pop(key)
                    progressed = True

    # Fallback : CROSS JOIN pour ce qui reste
    for al_l, txt in alias_to_text.items():
        if al_l not in joined:
            from_parts.append(f"\nCROSS JOIN {txt}")
            joined.add(al_l)

    new_from = ' '.join(from_parts)

    # --- WHERE restant ---
    leftover = []
    for _, c_start, c_end in conds_spans:
        if (c_start, c_end) in consumed_spans:
            continue
        raw = where_raw[c_start:c_end]
        raw_nocom = _mask_sql_comments_keep_layout(raw)
        val = re.sub(r'^\s*and\b', '', raw_nocom, flags=re.IGNORECASE).strip()
        if val:
            leftover.append(val)
    leftover.extend(filters)


    head = sql[:idx_from]
    tail = sql[end_where:]

    if leftover:
        clean = [re.sub(r'^\s*and\b', '', c, flags=re.IGNORECASE).strip() for c in leftover]
        clean = [c for c in clean if c]
        where_str = " AND\n  ".join(clean)
        rebuilt = f"{head}FROM {new_from}\nWHERE {where_str}\n{tail}"
    else:
        rebuilt = f"{head}FROM {new_from}\n{tail}"

    # Post-nettoyage de sécurité
    rebuilt = re.sub(r"(?i)\bON\s+AND\b", "ON ", rebuilt)               # ON AND -> ON
    rebuilt = re.sub(r"(?i)\bWHERE\s+1\s*=\s*1\s*(AND\s*)?", "WHERE ", rebuilt)
    rebuilt = re.sub(r"(?i)\bWHERE\s*(AND\s*)+\b", "WHERE ", rebuilt)   # WHERE AND -> WHERE
    rebuilt = re.sub(r"(?i)\s+AND\s*(AND\s*)+", " AND ", rebuilt)       # double AND
    rebuilt = re.sub(r"\bORDER\s+by\b", "ORDER BY", rebuilt)

    if debug:
        for (k1, k2, jt), conds in join_groups.items():
            print(f"[{jt} JOIN] {k1} <-> {k2}:")
            for c in conds:
                print(f"   ON {c}")

    return rebuilt

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

        body = rewrite_oracle_plus_joins(body)

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