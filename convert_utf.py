#!/usr/bin/env python3
import sys
import os
import re
import argparse
from datetime import datetime


class SQLCleaner:
    """Classe pour nettoyer et preparer les fichiers SQL Oracle pour Snowflake"""
    
    def __init__(self):
        pass
    
    def transform_table_references(self, sql):
        """Transforme les references de tables avec les schemas appropries"""
        def replacer(match):
            keyword = match.group(1)
            table = match.group(2)

            # Cas spécifique pour steph_apps_FND_FLEX_VALUES#_bz
            if table.lower() == 'steph_apps_fnd_flex_values#_bz':
                return f"{keyword} DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES"

            # Cas général pour les tables finissant par _bz
            if table.lower().endswith('_bz'):
                table_name = table[:-3]
                return f"{keyword} DEV.LH2_BRONZE_DEV.{table_name}"

            # Sinon, on garde le schéma SILVER
            return f"{keyword} DEV.LH2_SILVER_DEV.{table}"
        
        return re.sub(
            r"\b(FROM|JOIN)\s+([A-Za-z0-9_#]+)", 
            replacer, 
            sql, 
            flags=re.IGNORECASE
        )

    def remove_or_comment_variables(self, data: str):
        """Met en commentaire les variables globales g_* et locales v_*, y compris v_procedure même si elle est sur la ligne de IS"""
        variables_pattern = [
            (r"^\s*g_programme\s*:=.*?;\s*", "g_programme"),
            (r"^\s*g_error_code\s*:=.*?;\s*", "g_error_code"),
            (r"^\s*g_error_msg\s*:=.*?;\s*", "g_error_msg"),
            (r"^\s*g_date_fin\s*:=.*?;\s*", "g_date_fin"),
            (r"^\s*g_rowcount\s*:=.*?;\s*", "g_rowcount"),
            (r"^\s*g_table\s*:=.*?;\s*", "g_table"),
            (r"^\s*g_date_deb\s*:=.*?;\s*", "g_date_deb"),
            (r"^\s*g_status\s*:=.*?;\s*", "g_status"),
            (r"^\s*g_etape\s*:=.*?;\s*", "g_etape"),
            (r"^\s*g_level\s*:=.*?;\s*", "g_level"),
            (r"^\s*v_date_deb_proc\s+TIMESTAMP\s*:=.*?;\s*", "v_date_deb_proc"),
        ]

        # Traitement spécial pour v_procedure sur la même ligne que IS
        data = re.sub(
            r"^(?P<indent>\s*)IS\s+(?P<decl>v_procedure\s+varchar2\s*\(\s*100\s*\)\s*:=.*?;)",
            lambda m: f"{m.group('indent')}IS -- {m.group('decl')}",
            data,
            flags=re.IGNORECASE | re.MULTILINE
        )

        # Traitement des autres variables
        for pattern, var_name in variables_pattern:
            data = re.sub(
                pattern,
                lambda m: f"-- {m.group(0).strip()}\n",
                data,
                flags=re.IGNORECASE | re.MULTILINE
            )

        return data

    def clean_sql(self, data: str):
        """
        Nettoyage conservatif du SQL
        Met en commentaire au lieu de supprimer
        """
        
        # 1. Procedures de logging - Commentees
        procedures_to_comment = [
            'EXCEPTIONS_PROC',
            'HINT_ON_PROC', 
            'Write_Log_PROC'
        ]
        
        for proc_name in procedures_to_comment:
            pattern = rf"(PROCEDURE\s+{proc_name}\b.*?END\s+{proc_name}\s*;)"
            if re.search(pattern, data, flags=re.IGNORECASE | re.DOTALL):
                data = re.sub(
                    pattern,
                    lambda m: f"/* PROCEDURE {proc_name} disabled for SnowConvert\n{m.group(1)}\n*/\n",
                    data,
                    flags=re.IGNORECASE | re.DOTALL
                )
        
        # 2. Appels aux procedures - Commentes
        procedure_calls = [
            r"(?<!--)^\s*(Write_Log_PROC\s*;)",
            r"(?<!--)^\s*(Exceptions_PROC\s*;)",
            r"(?<!--)^\s*(HINT_ON_PROC\s*;)",
            r"(?<!--)^\s*(LH2_SILVER_ADMIN_PKG\.GATHER_TABLE_STATS_PROC\s*\(.*?\);)",
        ]
        
        for pattern in procedure_calls:
            data = re.sub(
                pattern,
                r"-- \1",
                data,
                flags=re.IGNORECASE | re.MULTILINE
            )
        
        # 3. Commit - Commentees
        data = re.sub(
            r"(?<!--)^\s*(COMMIT\s*;)",
            r"-- \1",
            data,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        data = re.sub(
            r"(?<!--)^\s*(ROLLBACK\s*;)",
            r"-- \1",
            data,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        # 4. Hints Oracle - Supprimes
        data = re.sub(r"/\*\+.*?\*/", "", data, flags=re.DOTALL)
        
        # 5. DBMS_OUTPUT - Commentes
        data = re.sub(
            r"(DBMS_OUTPUT\.PUT_LINE\s*\(.*?\);)",
            r"-- \1",
            data,
            flags=re.IGNORECASE
        )
        data = re.sub(
            r"(DBMS_OUTPUT\.ENABLE\s*\(.*?\);)",
            r"-- \1",
            data,
            flags=re.IGNORECASE
        )
        
        # 6. EXECUTE IMMEDIATE - Commentes
        execute_patterns = [
            r"(EXECUTE\s+IMMEDIATE\s+'ALTER SESSION[^']*';)",
            r"(EXECUTE\s+IMMEDIATE\s+'ALTER TABLE[^']*';)",
            r"(EXECUTE\s+IMMEDIATE\s+'TRUNCATE TABLE[^']*';)",
            r"(EXECUTE\s+IMMEDIATE\s+'CREATE INDEX[^']*';)",
        ]
        
        for pattern in execute_patterns:
            data = re.sub(
                pattern,
                r"-- \1",
                data,
                flags=re.IGNORECASE
            )
        
        return data
    
    def extract_procedures(self, data: str):
        """Extrait toutes les procedures du fichier SQL"""
        # Pattern pour procedures Snowflake dejà converties
        pattern = re.compile(
            r"(CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+[A-Za-z0-9_\.]+\s*\(.*?\)\s+RETURNS.*?^\$\$;)",
            re.IGNORECASE | re.DOTALL | re.MULTILINE
        )
        
        procedures = []
        for match in pattern.finditer(data):
            procedure_content = match.group(1)
            # Extraction du nom de procedure
            name_match = re.search(r"PROCEDURE\s+([A-Za-z0-9_\.]+)", procedure_content, re.IGNORECASE)
            if name_match:
                procedure_name = name_match.group(1).split('.')[-1]
                procedures.append((procedure_name, procedure_content))
        
        # Si aucune procedure Snowflake trouvee, essayer le pattern Oracle
        if not procedures:
            pattern = re.compile(
                r"(PROCEDURE\s+([A-Za-z0-9_]+)\b.*?END\s+\2\s*;)",
                re.IGNORECASE | re.DOTALL
            )
            for match in pattern.finditer(data):
                procedure_content = match.group(1)
                procedure_name = match.group(2)
                procedures.append((procedure_name, procedure_content))
        
        return procedures


def load_sql_file(path: str):
    """Charge un fichier SQL en gerant differents encodages"""
    encodings = ['utf-8-sig', 'utf-8', 'latin-1']
    
    for encoding in encodings:
        try:
            with open(path, 'r', encoding=encoding) as file:
                content = file.read()
                # Supprime le BOM s'il existe
                if content.startswith('\ufeff'):
                    content = content[1:]
                return content
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Erreur avec encodage {encoding}: {e}")
            continue
    
    print(f" Unable to read the file with the encodings: {encodings}")
    return None


def save_procedures(procedures, output_dir):
    """Sauvegarde toutes les procédures dans un seul fichier"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    file_path = os.path.join(output_dir, "all_procedures_cleaned.sql")
    with open(file_path, "w", encoding="utf-8") as f:
        for i, (proc_name, proc_content) in enumerate(procedures):
            f.write(f"-- Procedure {i+1}/{len(procedures)}: {proc_name}\n")
            f.write(f"-- {'-'*60}\n\n")
            f.write(proc_content.strip() + "\n\n")
    
    print(f"{len(procedures)} procedure(s) recorded in: {file_path}")


def main():
    # Vérification des arguments
    if len(sys.argv) < 2:
        print("Usage: python convert_utf_snowconvert.py <fichier_sql> [output_dir]")
        return 1
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output_cleaned"
    
    # Validation du fichier d'entrée
    if not os.path.isfile(input_file):
        print(f"Error: File '{input_file}' not exist.")
        return 1
    
    print(f"Traitement de: {input_file}")
    
    # Chargement du fichier
    content = load_sql_file(input_file)
    if content is None:
        return 1
    
    # Initialisation du cleaner
    cleaner = SQLCleaner()
    
    # Extraction des procédures
    procedures = cleaner.extract_procedures(content)
    
    if not procedures:
        print("No procedure found. Processing the entire file...")
        
        # Traitement du fichier complet
       # cleaned = cleaner.clean_sql(content)
      #  cleaned = cleaner.remove_or_comment_variables(cleaned)
        cleaned = cleaner.transform_table_references(cleaned)
        
        # Sauvegarde
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_file = os.path.join(output_dir, "cleaned.sql")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(cleaned)
        
        return 0
    
    print(f"{len(procedures)} procedure(s) found")
    
    # Traitement de chaque procedure
    cleaned_procedures = []
    for i, (proc_name, proc_content) in enumerate(procedures, 1):
        #cleaned = cleaner.clean_sql(proc_content)
       # cleaned = cleaner.remove_or_comment_variables(proc_content)
        cleaned = cleaner.transform_table_references(proc_content)
        cleaned_procedures.append((proc_name, cleaned))
    
    # Sauvegarde
    save_procedures(cleaned_procedures, output_dir)
    print(f"\n Conversion complete!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())