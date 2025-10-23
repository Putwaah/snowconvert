import re
import os
import sys

# Fichier source
input_file = sys.argv[1]
output_dir = "procedures_modifiees"
os.makedirs(output_dir, exist_ok=True)

# Lire le contenu
with open(input_file, "r", encoding="utf-8") as f:
    content = f.read()

# Détection des entêtes de procédure
pattern = r"-- (?:Procédure|Procedure) (\d+)/(\d+): (.+?)\n"
matches = list(re.finditer(pattern, content))

# Templates
def generate_header(proc_name, table_name):
    return f"""USE SCHEMA DEV.LH2_SILVER_DEV;

CREATE OR REPLACE PROCEDURE LH2_SILVER_DEV.{proc_name}()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_rows NUMBER DEFAULT 0;
BEGIN
  /* ===== BEGIN ===== */
  CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC(
    p_env        => 'S',
    p_table_name => '{table_name}',
    p_procedure  => 'LH2_SILVER_DEV.{proc_name}',
    p_status     => 'BEGIN',
    p_stage      => '10',
    p_rowcount   => 0,
    p_start_date => CURRENT_TIMESTAMP(),
    p_end_date   => NULL
  );
  CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC();  -- snapshot BEGIN

  /* ===== TRUNCATE ===== */
  CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC(
    p_env        => 'S',
    p_table_name => '{table_name}',
    p_procedure  => 'LH2_SILVER_DEV.{proc_name}',
    p_status     => 'TRUNCATE',
    p_stage      => '20',
    p_start_date => (SELECT START_DATE FROM LH2_EXPLOIT_DEV.LOG_CTX WHERE SESSION_ID = CURRENT_SESSION()),
    p_end_date   => NULL
  );
  EXECUTE IMMEDIATE 'TRUNCATE TABLE LH2_SILVER_DEV.{table_name}';
  CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC();  -- snapshot TRUNCATE

  /* ===== INSERTING ===== */
  CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC(
    p_env        => 'S',
    p_table_name => '{table_name}',
    p_procedure  => 'LH2_SILVER_DEV.{proc_name}',
    p_status     => 'INSERTING',
    p_stage      => '30',
    p_start_date => (SELECT START_DATE FROM LH2_EXPLOIT_DEV.LOG_CTX WHERE SESSION_ID = CURRENT_SESSION()),
    p_end_date   => NULL
  );
  CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC();  -- snapshot INSERTING

"""

def generate_footer(proc_name, table_name):
    return f"""
  -- capturer le rowcount de l'INSERT
  v_rows := SQLROWCOUNT;

  /* ===== COMPLETED ===== */
  CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC(
    p_env        => 'S',
    p_table_name => '{table_name}',
    p_procedure  => '{proc_name}',
    p_status     => 'COMPLETED',
    p_stage      => '90',
    p_rowcount   => :v_rows,
    p_start_date => (SELECT START_DATE FROM LH2_EXPLOIT_DEV.LOG_CTX WHERE SESSION_ID = CURRENT_SESSION()),
    p_end_date   => CURRENT_TIMESTAMP()
  );
  CALL LH2_EXPLOIT_DEV.WRITE_LOG_PROC();  -- snapshot COMPLETED
  RETURN 'OK - rows=' || v_rows;

EXCEPTION
  WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
    CALL DEV.LH2_EXPLOIT_DEV.EXCEPTIONS_PROC(:SQLCODE, :SQLERRM);
    RETURN 'KO';
END;
$$;
"""

# Traitement de chaque procédure
for i, match in enumerate(matches):
    start = match.start()
    end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
    block = content[start:end]
    proc_name = match.group(3).strip()

    # Capturer tous les blocs INSERT INTO jusqu'au point-virgule
    insert_matches = re.findall(
        r"INSERT INTO\s+[^\s(]+.*?;(?!\s*--)",  # évite les commentaires après ;
        block,
        re.IGNORECASE | re.DOTALL
    )

    if insert_matches:
        table_name_match = re.search(r"INSERT INTO\s+([^\s(]+)", insert_matches[0], re.IGNORECASE)
        if table_name_match:
            table_name = table_name_match.group(1)
        else:
            print(f"Nom de table introuvable pour {proc_name}, procédure ignorée.")
            continue
        if table_name.endswith('_TEMP'):
            print("c'est un temp")

        # Concaténer tous les blocs INSERT
        insert_block = "\n\n".join(insert_matches)
        new_block = generate_header(proc_name, table_name) + insert_block + generate_footer(proc_name, table_name)

        with open(f"{output_dir}/{proc_name}.sql", "w", encoding="utf-8") as out_file:
            out_file.write(new_block)
    else:
        print(f"Aucun INSERT INTO trouvé pour {proc_name}, procédure ignorée.")

print(f"{len(matches)} procédures analysées. Fichiers générés dans '{output_dir}'")