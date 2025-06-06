# JSON_DB_Importer.py

import sys
import os
import datetime
import shutil
import re
import logging
import argparse
import traceback
import configparser
import json
from typing import List, Tuple, Optional, Dict, Any, Set

# Attempt to import the required database driver library
try:
    import pyodbc
except ImportError:
    print("FATAL: The 'pyodbc' library is not installed. Please install it using 'pip install pyodbc'", file=sys.stderr, flush=True)
    sys.exit(1)


# --- Static Configuration ---
APP_NAME = "PhoneQA_DB_Importer"
CONFIG_FILE_NAME = "config.ini"
EXT_LIST_FILE_NAME = "ExtList.data"
DEFAULT_CALLS_ROOT_DIR = r"C:\Calls"
PROCESSED_PREFIX = "Stored-"
FAILED_PREFIX = "BadData-"
COMBINED_REPORT_FILENAME = "Combined_Analysis_Report.json"


# --- Determine Script Directory ---
try:
    script_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else os.path.abspath(__file__))
except NameError:
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

# --- Global Logger Instance ---
logger = logging.getLogger(APP_NAME)


def setup_logger(log_dir: str):
    """Configures the global logger instance."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{APP_NAME}_{datetime.datetime.now():%Y%m%d_%H%M%S}.log")

    logger.setLevel(logging.DEBUG)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)
    
    logger.info(f"Logger initialized. Log file at: {log_file}")


def load_config(config_path: str) -> configparser.ConfigParser:
    """Loads the configuration from the specified .ini file."""
    if not os.path.exists(config_path):
        logger.critical(f"Configuration file not found at '{config_path}'.")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    logger.info(f"Configuration loaded from {config_path}")
    return config


def parse_arguments(config: configparser.ConfigParser) -> argparse.Namespace:
    """Parses command-line arguments, using config for defaults."""
    default_root_from_config = config.get('Paths', 'SourceRoot', fallback=DEFAULT_CALLS_ROOT_DIR)

    parser = argparse.ArgumentParser(description="PhoneQA JSON to SQL Server Importer.")
    parser.add_argument(
        '--path',
        type=str,
        help="Specify the full path to a 'Week of YY-MM-DD' folder to process. Overrides default behavior."
    )
    parser.add_argument(
        '--root',
        type=str,
        default=default_root_from_config,
        help="The root directory where 'Week of...' folders are located. Default is from config.ini or fallback."
    )
    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {args}")
    return args


def find_latest_week_folder(root_dir: str) -> Optional[str]:
    """Finds the most recent 'Week of YY-MM-DD' directory."""
    week_folders = []
    date_pattern = re.compile(r'Week of (\d{4}-\d{2}-\d{2})')
    
    try:
        for item in os.listdir(root_dir):
            full_path = os.path.join(root_dir, item)
            if os.path.isdir(full_path):
                match = date_pattern.search(item)
                if match:
                    try:
                        folder_date = datetime.datetime.strptime(match.group(1), '%Y-%m-%d').date()
                        week_folders.append((folder_date, full_path))
                    except ValueError:
                        logger.warning(f"Found folder '{item}' with matching pattern but invalid date.")
        
        if not week_folders:
            logger.warning(f"No directories matching 'Week of YY-MM-DD' found in '{root_dir}'.")
            return None
            
        latest_folder = sorted(week_folders, key=lambda x: x[0], reverse=True)[0]
        logger.info(f"Found latest week folder: {latest_folder[1]}")
        return latest_folder[1]
        
    except FileNotFoundError:
        logger.error(f"Root directory '{root_dir}' not found.")
        return None
    except Exception as e:
        logger.error(f"Error scanning for latest week folder in '{root_dir}': {e}", exc_info=True)
        return None


def get_db_connection(config: configparser.ConfigParser) -> pyodbc.Connection:
    """Establishes and returns a pyodbc connection to the SQL Server database."""
    try:
        db_config = config['Database']
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['Server']};"
            f"DATABASE={db_config['Database']};"
            f"UID={db_config['User']};"
            f"PWD={db_config['Password']};"
        )
        conn = pyodbc.connect(conn_str)
        logger.info(f"Successfully connected to database '{db_config['Database']}' on server '{db_config['Server']}'.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.critical(f"Database connection failed. SQLSTATE: {sqlstate}. Error: {ex}")
        raise
    except Exception as e:
        logger.critical(f"Failed to establish database connection due to a configuration or system error: {e}")
        raise


def parse_extlist_data(extlist_path: str) -> Dict[str, Dict[str, str]]:
    """Parses the ExtList.data file into a dictionary keyed by EXTENSION."""
    members_by_ext = {}
    logger.info(f"Parsing agent data from: {extlist_path}")
    if not os.path.exists(extlist_path):
        logger.error(f"Agent data file not found: '{extlist_path}'. Cannot map agents.")
        return {}
    try:
        with open(extlist_path, 'r', encoding='utf-8') as f:
            for line in f:
                s_line = line.strip()
                if not s_line or s_line.startswith('#'): continue
                parts = s_line.split('\t')
                if len(parts) == 3:
                    ext, name, email = parts[0].strip(), parts[1].strip(), parts[2].strip()
                    if ext: members_by_ext[ext] = {"full_name": name, "email": email, "extension": ext}
        logger.info(f"Successfully parsed {len(members_by_ext)} members, keyed by extension.")
        return members_by_ext
    except Exception as e:
        logger.error(f"Error parsing agent data file '{extlist_path}': {e}", exc_info=True)
        return {}


def extract_extension_from_path(file_path: str) -> Optional[str]:
    """Extracts the 4-digit extension from the file's directory path."""
    match = re.search(r"Week of \d{4}-\d{2}-\d{2}[\\/](\d{4})[\\/]", file_path)
    if match:
        extension = match.group(1)
        logger.debug(f"Extracted extension '{extension}' from path '{file_path}'.")
        return extension
    
    logger.warning(f"Could not extract extension from path: '{file_path}'.")
    return None


def get_or_create_agent(cursor: pyodbc.Cursor, agent_details: Dict[str, str]) -> Optional[int]:
    """
    Gets the AgentID for a given agent, creating them if they don't exist.
    Uses the extension as the primary key for lookup.
    """
    agent_name = agent_details.get("full_name")
    extension = agent_details.get("extension")

    if not agent_name or not extension:
        logger.error(f"Agent details missing name or extension: {agent_details}. Cannot proceed.")
        return None

    try:
        cursor.execute("SELECT AgentID FROM Agents WHERE Extension = ?", extension)
        row = cursor.fetchone()
        if row:
            return row.AgentID

        logger.info(f"Agent '{agent_name}' with extension '{extension}' not found. Creating new record.")
        email = agent_details.get('email')
        
        insert_sql = "INSERT INTO Agents (AgentName, EmailAddress, Extension) OUTPUT INSERTED.AgentID VALUES (?, ?, ?);"
        new_agent_id = cursor.execute(insert_sql, agent_name, email, extension).fetchval()
        logger.info(f"Created new agent '{agent_name}' with AgentID: {new_agent_id}.")
        return new_agent_id
        
    except pyodbc.IntegrityError as e:
        logger.warning(f"IntegrityError for agent '{agent_name}' (Ext: {extension}), re-querying. Error: {e}")
        lookup_col = "Extension" if "Extension" in str(e) else "AgentName"
        lookup_val = extension if lookup_col == "Extension" else agent_name
        cursor.execute(f"SELECT AgentID FROM Agents WHERE {lookup_col} = ?", lookup_val)
        row = cursor.fetchone()
        if row: return row.AgentID
        else:
            logger.error(f"Failed to create or find agent '{agent_name}' after IntegrityError.")
            raise
    except Exception as e:
        logger.error(f"Database error while getting/creating agent '{agent_name}': {e}", exc_info=True)
        raise


def get_or_create_quality_points(cursor: pyodbc.Cursor, qp_texts: Set[str]) -> Dict[str, int]:
    """Efficiently gets IDs for existing quality points and creates non-existent ones."""
    qp_map = {}
    if not qp_texts: return qp_map

    try:
        placeholders = ', '.join(['?'] * len(qp_texts))
        sql_select = f"SELECT QualityPointText, QualityPointID FROM QualityPointsMaster WHERE QualityPointText IN ({placeholders})"
        cursor.execute(sql_select, *list(qp_texts))
        for row in cursor.fetchall():
            qp_map[row.QualityPointText] = row.QualityPointID
        
        new_qps_to_insert = [(text, 1 if "[BONUS]" in text.upper() else 0) for text in qp_texts if text not in qp_map]
        
        if new_qps_to_insert:
            logger.info(f"Found {len(new_qps_to_insert)} new quality points to insert.")
            sql_insert = "INSERT INTO QualityPointsMaster (QualityPointText, IsBonus) VALUES (?, ?)"
            cursor.fast_executemany = True
            cursor.executemany(sql_insert, new_qps_to_insert)
            logger.info("Successfully batch-inserted new quality points.")

            cursor.execute(sql_select, *list(qp_texts))
            for row in cursor.fetchall():
                qp_map[row.QualityPointText] = row.QualityPointID
        
        return qp_map
    except Exception as e:
        logger.error(f"Database error while getting/creating quality points: {e}", exc_info=True)
        raise

def process_individual_json(cursor: pyodbc.Cursor, json_data: Dict, file_path: str, agent_id: int, qp_map: Dict):
    """Processes a single individual analysis JSON and inserts data into the database."""
    summary = json_data.get('call_summary', {})
    remarks = json_data.get('concluding_remarks', {})
    
    sql_insert_analysis = """
        INSERT INTO IndividualCallAnalyses (
            AgentID, TechDispatcherNameRaw, OriginalAudioFileName, CallDuration, ClientName,
            ClientFacilityCompany, TicketNumber, ClientCallbackNumber, TicketStatusType,
            CallSubjectSummary, ConcludingRemarks_Positive, ConcludingRemarks_Negative,
            ConcludingRemarks_Coaching
        ) OUTPUT INSERTED.AnalysisID VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    params = (
        agent_id, summary.get('tech_dispatcher_name'), os.path.basename(file_path).replace('_analysis.json', '.wav'),
        summary.get('call_duration'), summary.get('client_name'), summary.get('client_facility_company'),
        summary.get('ticket_number'), summary.get('client_callback_number'), summary.get('ticket_status_type'),
        summary.get('call_subject_summary'), remarks.get('summary_positive_findings'),
        remarks.get('summary_negative_findings'), remarks.get('coaching_plan_for_growth')
    )
    analysis_id = cursor.execute(sql_insert_analysis, params).fetchval()
    logger.debug(f"Inserted IndividualCallAnalyses record with ID: {analysis_id}")
    
    eval_items = json_data.get('detailed_evaluation', [])
    eval_params = [(analysis_id, qp_map.get(item.get('quality_point')), item.get('finding'), item.get('explanation_snippets')) for item in eval_items if qp_map.get(item.get('quality_point')) is not None]
    
    if eval_params:
        sql_insert_items = "INSERT INTO IndividualEvaluationItems (AnalysisID, QualityPointID, Finding, ExplanationSnippets) VALUES (?, ?, ?, ?)"
        cursor.fast_executemany = True
        cursor.executemany(sql_insert_items, eval_params)
        logger.debug(f"Inserted {len(eval_params)} evaluation items for AnalysisID {analysis_id}.")

def process_combined_json(cursor: pyodbc.Cursor, json_data: Dict, agent_id: int, qp_map: Dict):
    """Processes the combined analysis JSON and inserts data into the database."""
    header = json_data.get('report_header', {})
    snapshot = json_data.get('overall_performance_snapshot', {})
    
    sql_insert_combined = """
        INSERT INTO CombinedAnalyses (
            AgentID, AnalysisPeriodNote, NumberOfReportsProvided, NumberOfReportsSuccessfullyAnalyzed,
            Snapshot_TotalCallsContributing, Snapshot_PositiveCount, Snapshot_NegativeCount, Snapshot_NeutralCount
        ) OUTPUT INSERTED.CombinedAnalysisID VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    params = (
        agent_id, header.get('analysis_period_note'), header.get('number_of_reports_provided'),
        header.get('number_of_reports_successfully_analyzed'), snapshot.get('total_calls_contributing_to_aggregates'),
        snapshot.get('aggregate_findings_counts', {}).get('positive_count'),
        snapshot.get('aggregate_findings_counts', {}).get('negative_count'),
        snapshot.get('aggregate_findings_counts', {}).get('neutral_count')
    )
    combined_id = cursor.execute(sql_insert_combined, params).fetchval()
    logger.debug(f"Inserted CombinedAnalyses record with ID: {combined_id}")

    qual_summary = json_data.get('qualitative_summary_and_coaching_plan', {})
    cursor.fast_executemany = True
    
    if strengths := qual_summary.get('overall_strengths_observed', []):
        cursor.executemany("INSERT INTO CombinedAnalysisStrengths (CombinedAnalysisID, StrengthText) VALUES (?, ?)", [(combined_id, s) for s in strengths])

    if dev_areas := qual_summary.get('overall_areas_for_development', []):
        cursor.executemany("INSERT INTO CombinedAnalysisDevelopmentAreas (CombinedAnalysisID, DevelopmentAreaText) VALUES (?, ?)", [(combined_id, d) for d in dev_areas])

    for focus_item in qual_summary.get('consolidated_coaching_focus', []):
        if area_text := focus_item.get('area'):
            focus_id = cursor.execute("INSERT INTO CombinedAnalysisCoachingFocus (CombinedAnalysisID, AreaText) OUTPUT INSERTED.CoachingFocusID VALUES (?, ?);", combined_id, area_text).fetchval()
            if actions := focus_item.get('specific_actions', []):
                cursor.executemany("INSERT INTO CombinedAnalysisCoachingActions (CoachingFocusID, ActionText) VALUES (?, ?)", [(focus_id, a) for a in actions])
    
    qp_detail_params = []
    for detail in json_data.get('detailed_quality_point_analysis', []):
        if qp_id := qp_map.get(detail.get('quality_point')):
            findings = detail.get('findings_summary', {})
            qp_detail_params.append((
                combined_id, qp_id, findings.get('positive_count'), findings.get('negative_count'),
                findings.get('neutral_count'), detail.get('trend_observation')
            ))
    if qp_detail_params:
        sql_insert_qp_detail = "INSERT INTO CombinedAnalysisQualityPointDetails (CombinedAnalysisID, QualityPointID, FindingsSummary_Positive, FindingsSummary_Negative, FindingsSummary_Neutral, TrendObservation) VALUES (?, ?, ?, ?, ?, ?)"
        cursor.executemany(sql_insert_qp_detail, qp_detail_params)
        logger.debug(f"Inserted {len(qp_detail_params)} detailed QP analysis records.")


def process_folder(target_folder: str, config: configparser.ConfigParser):
    """Orchestrates the processing of all valid JSON files within a given folder."""
    logger.info(f"Starting processing for folder: {target_folder}")
    conn = None
    try:
        ext_map = parse_extlist_data(os.path.join(script_dir, EXT_LIST_FILE_NAME))
        
        # =================================================================
        # === MODIFIED LOGIC: Selectively find only valid report files
        # =================================================================
        files_to_process = []
        for root, _, files in os.walk(target_folder):
            for file in files:
                # Process only analysis files and combined reports, ignore errors and other json files
                if (file.endswith('_analysis.json') or COMBINED_REPORT_FILENAME in file) and \
                   not file.startswith((PROCESSED_PREFIX, FAILED_PREFIX)):
                    files_to_process.append(os.path.join(root, file))
        # =================================================================
        
        if not files_to_process:
            logger.info("No new, valid JSON report files to process in this folder.")
            return

        logger.info(f"Found {len(files_to_process)} new JSON reports to process.")
        
        conn = get_db_connection(config)
        cursor = conn.cursor()

        for file_path in files_to_process:
            base_name = os.path.basename(file_path)
            logger.info(f"--- Processing file: {base_name} ---")
            is_success = False
            try:
                extension = extract_extension_from_path(file_path)
                
                agent_details = None
                if extension:
                    agent_details = ext_map.get(extension)
                    if not agent_details:
                        logger.warning(f"Extension '{extension}' from path not in {EXT_LIST_FILE_NAME}. Recording as un-rostered.")
                        agent_details = {"full_name": f"Un-rostered Agent - {extension}", "email": None, "extension": extension}
                else:
                    timestamp_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
                    malformed_ext = f"UNKEYED_PATH_{timestamp_str}"
                    malformed_name = f"Unknown Agent (Unkeyed Path {timestamp_str})"
                    logger.error(f"Could not determine extension from path for '{base_name}'. Creating unique unknown agent.")
                    agent_details = {"full_name": malformed_name, "email": None, "extension": malformed_ext}

                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                conn.autocommit = False
                
                agent_id = get_or_create_agent(cursor, agent_details)
                if not agent_id: raise ValueError(f"Could not get/create ID for agent: {agent_details}")

                all_qps_in_file = {item['quality_point'] for item in json_data.get("detailed_evaluation", [])}
                all_qps_in_file.update({item['quality_point'] for item in json_data.get("detailed_quality_point_analysis", [])})
                qp_map = get_or_create_quality_points(cursor, all_qps_in_file)

                if COMBINED_REPORT_FILENAME in base_name:
                    process_combined_json(cursor, json_data, agent_id, qp_map)
                else:
                    process_individual_json(cursor, json_data, file_path, agent_id, qp_map)
                
                conn.commit()
                logger.info(f"Successfully committed changes for {base_name}.")
                is_success = True

            except Exception as e:
                logger.error(f"Failed to process file '{file_path}': {e}", exc_info=True)
                if conn and not conn.autocommit: conn.rollback()
                is_success = False
            
            finally:
                new_prefix = PROCESSED_PREFIX if is_success else FAILED_PREFIX
                try:
                    dir_name, current_base_name = os.path.split(file_path)
                    shutil.move(file_path, os.path.join(dir_name, f"{new_prefix}{current_base_name}"))
                    logger.info(f"Renamed '{current_base_name}' with prefix '{new_prefix}'.")
                except Exception as e_rename:
                    logger.error(f"CRITICAL: Failed to rename file '{file_path}'. Error: {e_rename}")
    
    except Exception as e:
        logger.critical(f"A major error occurred during folder processing: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def main():
    """Main execution function."""
    log_dir = os.path.join(script_dir, "logs", APP_NAME)
    setup_logger(log_dir)
    
    try:
        config_path = os.path.join(script_dir, CONFIG_FILE_NAME)
        config = load_config(config_path)
        args = parse_arguments(config)

        target_folder = args.path
        if target_folder:
            if not os.path.isdir(target_folder):
                logger.critical(f"Provided path '{target_folder}' is not a valid directory.")
                return
            logger.info(f"Processing user-specified folder: {target_folder}")
        else:
            target_folder = find_latest_week_folder(args.root)
            if not target_folder:
                logger.info("No folder to process. Exiting.")
                return
        
        process_folder(target_folder, config)

    except FileNotFoundError as e:
        logger.critical(f"A critical file was not found: {e}", exc_info=False)
    except Exception as e:
        logger.critical(f"An unhandled exception occurred in main: {e}", exc_info=True)
        
    logger.info(f"{APP_NAME} finished.")


if __name__ == '__main__':
    main()