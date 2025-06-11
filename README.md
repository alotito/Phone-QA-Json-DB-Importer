DEPRECATED
 
 # Phone-QA-Json-DB-Importer
Automated Phone QA System for Global Tech Solutions
&lt;p align="center">
&lt;img src="https://img.shields.io/badge/Python-3.13-3776AB?logo=python" alt="Python 3.13">
&lt;img src="https://img.shields.io/badge/MS_SQL_Server-2019-CC2927?logo=microsoft-sql-server" alt="SQL Server 2019+">
&lt;img src="https://img.shields.io/badge/AI_Engine-Google_Gemini-4285F4?logo=google-gemini" alt="Google Gemini">
&lt;img src="https://img.shields.io/badge/Deployment-Windows_Task_Scheduler-0078D6?logo=windows" alt="Windows Task Scheduler">
&lt;img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
&lt;/p>

An enterprise-grade, fully automated pipeline for ingesting, analyzing, and archiving call center data for Quality Assurance. This system leverages AI to provide deep, actionable insights into agent performance, saving hundreds of manual review hours.

Overview
The Automated Phone QA System is a suite of three interconnected Python scripts designed to provide a seamless, end-to-end solution for call quality assurance at Global Tech Solutions LLC. The system automatically collects weekly call recordings, uses the Google Gemini AI to perform a comprehensive analysis against a defined scorecard, generates reports, and archives the structured results in a centralized SQL Server database for analytics and long-term reporting via Power BI.

System Architecture
The system operates as a scheduled, sequential pipeline, with each component feeding data to the next.

📞 Script 1: Call Ingestion (PhoneQACopyTool.py)

Trigger: Runs weekly (e.g., Sunday at 1:00 AM) via Windows Task Scheduler.
Action: Scans the call recording server for the most recently completed week's calls.
Output: Copies a configured number of .wav files for each agent into a structured weekly folder (C:\Calls\Week of YYYY-MM-DD\<extension>).
🤖 Script 2: AI Analysis & Reporting (AutoQA.py)

Trigger: Runs weekly (e.g., Sunday at 2:00 AM), after the Call Ingestion task is complete.
Action: Processes the new .wav files, sending each to the Gemini AI for analysis. Generates individual and combined summary reports.
Output: Creates .json and .docx reports in the agent's weekly folder and sends a summary email to the agent and management.
🗃️ Script 3: Database Archiving (JSON_DB_Importer.py)

Trigger: Runs weekly (e.g., Sunday at 3:00 AM), after the AI Analysis task is complete.
Action: Scans for new, unprocessed .json files. Parses them and inserts the structured data into the SQL Server database.
Output: A fully populated PhoneQA database. Processed files are renamed with a Stored- prefix to prevent duplicate imports.
✨ Features
Zero-Touch Automation: Designed for fully unattended weekly execution via Windows Task Scheduler.
AI-Powered Insights: Leverages Google's Gemini 1.5 Flash model for nuanced, human-like analysis of call recordings against a detailed QA scorecard.
Robust Data Handling: The database importer is transactional, ensuring data integrity. Failed imports are safely rolled back and problematic files are automatically quarantined.
Dynamic & Resilient: Automatically creates new records for agents and QA scorecard items as they appear, reducing administrative overhead. Gracefully handles un-rostered agents and malformed data paths.
Comprehensive Reporting: Generates both machine-readable (.json) and human-readable (.docx) reports for individual calls and weekly agent summaries.
Proactive Notifications: Automatically emails performance summaries and reports to agents and managers.
Configuration-Driven: All critical settings (paths, API keys, database credentials, email settings) are managed in an external config.ini file for easy updates without code changes.
🛠️ Technology Stack
Backend: Python 3.13
Database: Microsoft SQL Server (2019 or newer)
AI Engine: Google Gemini API
Core Python Libraries: pyodbc, google-generativeai, python-docx
Deployment: Windows Task Scheduler, PyInstaller (for executable generation)
Prerequisites
Before you begin, ensure you have the following installed on the server where the scripts will run:

Python 3.13+
Microsoft ODBC Driver 17 for SQL Server
Access to the SQL Server instance (GTSTCH-CWR01) and the call recording file share.
🚀 Installation & Setup
Follow these steps to set up the system.

1. Clone the Repository

Bash

git clone <your-repo-url>
cd <your-repo-folder>
2. Set Up Python Environment
It is highly recommended to use a virtual environment.

Bash

python -m venv venv
.\venv\Scripts\activate
3. Install Dependencies
Install all required Python packages from the requirements.txt file.

# requirements.txt
pyodbc
google-generativeai
python-docx
pyinstaller
pywin32
Bash

pip install -r requirements.txt
4. Database Setup
Using SQL Server Management Studio (SSMS), connect to GTSTCH-CWR01 and execute the following scripts in order:

Generate tables - gemini.sql: Creates the database schema.
User Create.sql: Creates the necessary database users and permissions.
(Optional) Truncate All Tables.sql: Use this utility script if you ever need to reset all data for a fresh import.
5. Configure the System
Two files must be configured in the project's root directory:

ExtList.data: This is the master list of all agents. It is a tab-separated file.
# Extension    FullName          EmailAddress
2065           Albert Smith      albert.smith@gts.com
2126           Shana Miller      shana.miller@gts.com
config.ini: Update this file with your environment-specific settings.
Ini, TOML

[Database]
Server = GTSTCH-CWR01
Database = PhoneQA
User = PhoneQA_DataEntryUser
Password = YourStrongPasswordHere!

[Paths]
SourceRoot = \\path\to\your\call_recordings

# --- Other sections for AutoQA.py ---
[Settings]
API_Key_B64 = YourBase64EncodedGoogleApiKey

[SMTP]
Server = smtp.yourserver.com
# ... etc.
⚙️ Usage
Each script can be run manually from the command line for testing purposes.

Copy Tool: python PhoneQACopyTool.py --date YYYY-MM-DD
AI Analyzer: python AutoQA.py --date YYYY-MM-DD
DB Importer: python JSON_DB_Importer.py --path "C:\Calls\Week of YYYY-MM-DD"
📦 Building Executables
For deployment, you can compile each script into a standalone .exe file using PyInstaller. This allows the scripts to run on systems without a full Python development environment.

Important: The recommended approach is to create an executable that reads the config.ini and ExtList.data as "loose" files, making updates easy.

Run the PyInstaller command:

Bash

# For the Database Importer
pyinstaller --onefile --name "JSON_DB_Importer" .\JSON_DB_Importer.py

# For the Call Copier
pyinstaller --onefile --name "PhoneQACopyTool" .\PhoneQACopyTool.py

# For the AI Analyzer (requires bundling prompt files)
pyinstaller --onefile --name "AutoQAReporter" --add-data "config.ini;." --add-data "ExtList.data;." --add-data "IndividualPrompt.txt;." --add-data "CombinedPrompt.txt;." --add-data "EmailPrompt.txt;." AutoQA.py
Deploy the Files:

After building, go into the dist folder.
Copy the generated .exe to your deployment directory.
Copy the required config.ini and ExtList.data files into the same directory as the .exe.
🗓️ Deployment via Windows Task Scheduler
Open Task Scheduler on your Windows Server.
Create a new task for each of the three executables.
Trigger: Set each task to run "On a schedule," weekly, on Sunday. Stagger the start times (e.g., 1:00 AM, 2:00 AM, 3:00 AM).
Action: Set the action to "Start a program." Browse to the location of your .exe file.
Conditions: Ensure the "Start only if the following network connection is available" is set correctly if your scripts access network shares.
Settings: Check "Run task as soon as possible after a scheduled start is missed." For reliability, run the task under a service account with appropriate permissions to the file shares and the database.
This document reflects the final state of the project as of Friday, June 6, 2025. 
