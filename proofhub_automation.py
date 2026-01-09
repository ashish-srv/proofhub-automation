# proofhub_automation.py
import requests
import pandas as pd
import time
from datetime import datetime
import ast
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# ================= CONFIGURATION =================
COMPANY_NAME = "srvmedia"
API_KEY = os.getenv('PROOFHUB_API_KEY', '0457cfd434cf86cd71d23177ee4ed41fd442527c')

headers = {
    "X-API-KEY": API_KEY,
    "User-Agent": "ZohoIntegration (ashish.kate@srvmedia.com)",
    "Accept": "application/json"
}

base_url = f"https://{COMPANY_NAME}.proofhub.com/api/v3"

# ================= CLIENT NAME MAPPING =================
CLIENT_NAME_MAPPING = {
    'Admissions Fair': 'Afairs', 'Afairs': 'Afairs', 'Afairs - MBA Expo': 'Afairs',
    'Afairs 2024': 'Afairs', 'Afairs SM 2025': 'Afairs', 'Auro University': 'Auro University',
    'Bank Of Maharashtra': 'Bank of Maharashtra', 'BBDU': 'Babu Banarasi Das University',
    'Bharti Vidya Peeth University': 'Bharathi Vidyapeeth Pune', 'CALMU': 'CMU',
    'CGC Jhanjeri': 'CGC Jhanjeri', 'CGC Landran': 'CGC Landran',
    'chanakya university': 'Chanakya University', 'Chandigarh University': 'Chandigarh University',
    'DES PU': 'DES Pune', 'Easebuzz': 'Easebuzz',
    'GD Goenka Healthcare Franchise 2026-26': 'GD Goenka', 'GD Goenka High School 2026-27': 'GD Goenka',
    'GD Goenka School Franchise': 'GD Goenka', 'GD Goenka University 2026 - 2027': 'GD Goenka',
    'GD Goenka World School 2026-27': 'GD Goenka', 'Geta': 'Geta AI', 'Geta MSG': 'Geta AI',
    'IIM Kozhikode': 'IIM Kozhikode', 'IIM Udaipur': 'IIM Udaipur',
    'IMT [G] - PGDM ExP': 'IMT Gazhiabad', 'Jio Institute': 'Jio Institute',
    'KJ Somaiya - MBA': 'K J Somaiya', 'Krishnayan': 'Shree Krishnayan',
    'Krishnayan SM': 'Shree Krishnayan', 'MAHE': 'MAHE',
    'MIT World Peace University': 'MIT-WPU', 'MIT-VPU': 'MIT VPU',
    'Muthoot Business School': 'Muthoot Business School',
    'Nirma MBA &amp; MBA HRM': 'NIRMA', 'Nirma MBA SM': 'NIRMA',
    'Nirma MBA Social Media': 'NIRMA', 'Nitte Hospital': 'Nitte',
    'NITTE SM': 'Nitte', 'Nitte University': 'Nitte', 'Orbit group': 'Orbit',
    'Parul Ayurveda': 'Parul', 'Parul Branding AY 2025': 'Parul',
    'Parul Diploma AY 2025': 'Parul', 'Parul Goa': 'Parul',
    'Parul Hospital Ads': 'Parul', 'Parul Institute of Design': 'Parul',
    'Parul ODL': 'Parul', 'Parul Online 2025-2026': 'Parul',
    'Parul PG AY 2025': 'Parul', 'Parul Pharmaceuticals': 'Parul',
    'Parul Sevashram Hospital': 'Parul', 'Parul Study Abroad Centre': 'Parul',
    'Parul UG AY 2025': 'Parul', 'SBUP - DDSS': 'SBUP',
    'SBUP - MBA 2025': 'SBUP', 'SBUP MBA 2025 - 26': 'SBUP',
    'SBUP PhD 2025': 'SBUP', 'SBUP UG PG 2025': 'SBUP',
    'SCAC': 'SCAC', 'SCAE Python Project': 'SCAC', 'SCDL 2025': 'SCDL',
    'SCIT 2023-24': 'SCIT', 'SCIT 2024': 'SCIT', 'SCIT 2025': 'SCIT',
    'SCMC AY 2025': 'SCMC', 'SCMHRD 2024': 'SCMHRD', 'SCMHRD 2025': 'SCMHRD',
    'SCMHRD Executive MBA': 'SCMHRD', 'SCMS Bengaluru': 'SCMS Bangalore',
    'SCMS Hyd 2025': 'SCMS Hyderabad', 'SCMS Nagpur AY 2025': 'SCMS Nagpur',
    'SCMS Noida': 'SCMS Noida', 'SCMS Pune': 'SCMS Pune',
    'SCSD 2025': 'SCSD', 'SET 2026': 'SET', 'SET SM 2025': 'SET',
    'SIBM Bangalore': 'SIBM Bangalore', 'SIBM Bengaluru 2025': 'SIBM Bangalore',
    'SIBM Hyd AY 2025': 'SIBM Hyderabad', 'SIBM Nagpur AY 2025': 'SIBM Nagpur',
    'SIBM Nagpur-Mobile': 'SIBM Nagpur', 'SIBM NOIDA 2025': 'SIBM Noida',
    'SIBM PUNE 2025': 'SIBM Pune', 'SICSR 2024': 'SICSR', 'SICSR 2025': 'SICSR',
    'SID 2025': 'SID', 'SIDTM AY 2025': 'SITM', 'SIG': 'SIG',
    'SIIB 2024': 'SIIB', 'SIIB 2025': 'SIIB', 'SIMC AY 2025': 'SIMC',
    'SIOM': 'SIOM Nashik', 'SIT Hyd 2025': 'SIT Hyderabad',
    'SIT Hyderabad': 'SIT Hyderabad', 'SIT Nagpur 2025': 'SIT Nagpur',
    'SIT Pune 2024': 'SIT Pune', 'SIT Pune 2025': 'SIT Pune',
    'SIT Pune 2026': 'SIT Pune', 'SLAT': 'SLAT', 'SLAT 2024': 'SLAT',
    'SLAT SM 2025': 'SLAT', 'SLAT Social media': 'SLAT',
    'SLS Hyderabad 2025': 'SLS Hyderabad', 'SLS Hyderabad AMC': 'SLS Hyderabad',
    'SLS Nagpur 2025': 'SLS Nagpur', 'SLS Pune': 'SLS Pune',
    'SNAP 2025': 'SNAP', 'SNAP SM 2024': 'SNAP', 'SNAP SM 2025': 'SNAP',
    'Somaiya Vidyavihar University (SVU)': 'SVU', 'SPJIMR': 'SPJIMR',
    'SRV Edge': 'SRV', 'SRV Media': 'SRV',
    'SRV MEDIA &amp; EDGE social media': 'SRV', 'SRV PR': 'SRV',
    'SRV Web Masters': 'SRV', 'SSE- 2024': 'SSE Pune', 'SSI 2025': 'SSI',
    'SSSS': 'SSSS', 'SSVAP 2025': 'SSVAP', 'MCX': 'MCX',
    'Uniaptic': 'Uniaptix India', 'Welingkar PGDM 26-27': 'WeSchool',
    'WeSchool 2025': 'WeSchool', 'WPU Goa': 'MIT-WPU Goa',
    'XAT': 'XAT', 'XAT 2026 - PR Outreach': 'XAT',
    'XAT 2026 Social Media': 'XAT', 'XLRI PGDM GM': 'XAT',
    'XLRI VIL': 'XAT', 'XLRI XOL': 'XAT',
    'XIM University': 'XIM University',
    'XLRI Jamshedpur': 'XLRI Jamshedpur',
    'XLRI Jamshedpur SM': 'XLRI Jamshedpur',
    'PaySquare' : 'PaySquare',
    'NL Dalmia University' : 'N L Dalmia',
    'ISKCON' : 'ISKCON',
    'Sai Sudha_Lawn' : 'Parul',
    'Wabo.ai' : 'Wabo',
    'RV University' : 'RV University',
    'IEM' : 'IEM Kolkata',
    'IEM Kolkata' : 'IEM Kolkata',
    'IEM MBA 25-26' : 'IEM Kolkata',
    'NIF Kothrud' : 'NIF',
    'NIF Mumbai' : 'NIF',
    'ASBM - Mumbai' : 'ASBM',
    'INIFD Kothrud SM' : 'NIF',
    'NIFD Kothrud' : 'NIF',
    'NIFD Mumbai' : 'NIF',
    'QMUL Academics Promotions' : 'QMUL',
    'Queen Mary University - London' : 'QMUL',
    'DY Patil - Navi Mumbai' : 'D Y Patil',
    'IMS Unison University' : 'IMS Unison University',
    'IMS Social Media' : 'IMS Unison University',
    'IMS Unison 2026-2027': 'IMS Unison University'
}

def get_client_name(project_name):
    """Get client name from project name using mapping"""
    if pd.isna(project_name) or project_name == '':
        return ''
    project_name_str = str(project_name).strip()
    if project_name_str in CLIENT_NAME_MAPPING:
        return CLIENT_NAME_MAPPING[project_name_str]
    for key, value in CLIENT_NAME_MAPPING.items():
        if key in project_name_str:
            return value
    return ''

# ================= GOOGLE DRIVE UPLOAD =================
def upload_to_google_drive(file_path, folder_id):
    """Upload file to Google Drive Shared Drive"""
    try:
        print(f"📤 Starting Google Drive upload to Shared Drive...")
        print(f"   File: {file_path} ({os.path.getsize(file_path) / 1024:.1f} KB)")
        print(f"   Target Shared Drive Folder ID: {folder_id}")
        
        if not os.path.exists(file_path):
            print("❌ CSV file not found!")
            return False
        
        # Get service account key
        service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            print("❌ GOOGLE_SERVICE_ACCOUNT_KEY is missing!")
            return False
        
        # Parse JSON
        try:
            service_account_info = json.loads(service_account_json)
            print(f"✅ Service Account: {service_account_info.get('client_email')}")
        except json.JSONDecodeError as e:
            print(f"❌ JSON error: {e}")
            return False
        
        # Create credentials with drive scope
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        # Build Drive service
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        
        # Test access to Shared Drive
        print(f"🔍 Testing access to Shared Drive folder...")
        try:
            folder_info = service.files().get(
                fileId=folder_id,
                fields='id, name, driveId, capabilities',
                supportsAllDrives=True
            ).execute()
            print(f"✅ Can access folder: {folder_info.get('name')}")
            print(f"   Drive ID: {folder_info.get('driveId')}")
        except Exception as e:
            print(f"❌ Cannot access folder: {e}")
            print("   Make sure:")
            print("   1. Service account has access to Shared Drive")
            print("   2. Service account has 'Content manager' or 'Editor' role")
            print("   3. Folder ID is correct")
            return False
        
        # File metadata
        file_metadata = {
            'name': 'All Projects Timesheet.csv',
            'parents': [folder_id]
        }
        
        # Create media
        media = MediaFileUpload(
            file_path,
            mimetype='text/csv',
            resumable=True
        )
        
        # Search for existing file in Shared Drive
        print(f"🔍 Searching for existing file...")
        response = service.files().list(
            q=f"name='All Projects Timesheet.csv' and '{folder_id}' in parents and trashed=false",
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        if response.get('files'):
            # Update existing file
            file_id = response['files'][0]['id']
            print(f"🔄 Updating existing file (ID: {file_id})")
            file = service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            print(f"✅ Updated file in Shared Drive: {file.get('id')}")
        else:
            # Create new file
            print(f"📄 Creating new file in Shared Drive...")
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"✅ Created new file in Shared Drive: {file.get('id')}")
        
        # Get file link
        file_link = f"https://drive.google.com/drive/folders/{folder_id}"
        print(f"📎 File accessible at: {file_link}")
        
        return True
        
    except HttpError as error:
        error_details = json.loads(error.content.decode('utf-8'))
        error_msg = error_details.get('error', {}).get('message', str(error))
        print(f'❌ Google Drive API error: {error_msg}')
        
        if 'storageQuotaExceeded' in str(error):
            print("💡 Still getting quota error? Try:")
            print("   1. Double-check folder is in SHARED DRIVE (not My Drive)")
            print("   2. Service account email added to Shared Drive members")
            print("   3. Service account has 'Content manager' permission")
        
        return False
    except Exception as e:
        print(f'❌ Unexpected error: {type(e).__name__}: {e}')
        import traceback
        traceback.print_exc()
        return False


# ================= DOWNLOAD FUNCTION =================
def download_all_data(endpoint, data_key=None, max_pages=3):
    """Download all data from an endpoint with duplicate detection"""
    all_data = []
    page = 1
    seen_ids = set()
    
    while page <= max_pages:
        url = f"{base_url}/{endpoint}?page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    if data_key and data_key in data:
                        items = data[data_key]
                    else:
                        for key in [endpoint, 'data', 'items', 'results']:
                            if key in data and isinstance(data[key], list):
                                items = data[key]
                                break
                if not items:
                    break
                new_items = []
                duplicate_count = 0
                for item in items:
                    item_id = None
                    if isinstance(item, dict):
                        if 'id' in item:
                            item_id = item['id']
                        elif 'ID' in item:
                            item_id = item['ID']
                    if item_id is None:
                        new_items.append(item)
                    elif item_id not in seen_ids:
                        seen_ids.add(item_id)
                        new_items.append(item)
                    else:
                        duplicate_count += 1
                if new_items:
                    all_data.extend(new_items)
                    if duplicate_count > len(new_items) * 0.5:
                        break
                else:
                    break
                if len(items) < 50:
                    break
                page += 1
                time.sleep(0.5)
            elif response.status_code == 429:
                time.sleep(30)
                continue
            else:
                break
        except Exception as e:
            break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def extract_id_from_field(field_value):
    """Extract ID from field that might be dict/JSON string"""
    if pd.isna(field_value):
        return None
    try:
        if isinstance(field_value, str) and field_value.strip().startswith('{'):
            field_dict = ast.literal_eval(field_value.strip())
            if isinstance(field_dict, dict) and 'id' in field_dict:
                return str(field_dict['id'])
        elif isinstance(field_value, dict) and 'id' in field_value:
            return str(field_value['id'])
    except:
        pass
    return None

# ================= MAIN FUNCTION =================
def main():
    print("🚀 STARTING PROOFHUB AUTOMATION")
    print("=" * 70)
    
    # Download reference data
    print("📥 Downloading people data...")
    people_df = download_all_data("people", "people", max_pages=1)
    if 'first_name' in people_df.columns and 'last_name' in people_df.columns:
        people_df['Full_Name'] = people_df.apply(
            lambda row: f"{str(row.get('first_name', '')).strip()} {str(row.get('last_name', '')).strip()}".strip(),
            axis=1
        )
        people_df['Full_Name'] = people_df['Full_Name'].replace('', pd.NA)
        people_df['Full_Name'] = people_df['Full_Name'].fillna(people_df.get('name', ''))
    
    print("📥 Downloading roles data...")
    roles_df = download_all_data("roles", "roles", max_pages=1)
    role_mapping = {}
    if 'id' in roles_df.columns and 'name' in roles_df.columns:
        for _, row in roles_df.iterrows():
            role_mapping[str(row['id'])] = row.get('name', '')
    
    print("📥 Downloading categories data...")
    categories_df = download_all_data("categories", "categories", max_pages=1)
    category_mapping = {}
    if 'id' in categories_df.columns and 'name' in categories_df.columns:
        for _, row in categories_df.iterrows():
            category_mapping[str(row['id'])] = row.get('name', '')
    
    print("📥 Downloading projects data...")
    projects_df = download_all_data("projects", "projects", max_pages=3)
    
    # Prepare project info
    project_info_dict = {}
    for _, row in projects_df.iterrows():
        project_id = row.get('id')
        if pd.notna(project_id):
            project_id_str = str(project_id)
            project_name = row.get('title', row.get('name', f'Project_{project_id}'))
            category_id = None
            if 'category' in row and pd.notna(row['category']):
                category_id = extract_id_from_field(row['category'])
            category_name = category_mapping.get(category_id, '') if category_id else ''
            start_date = row.get('start_date', '')
            end_date = row.get('end_date', '')
            if pd.notna(start_date):
                start_date = str(start_date).split('T')[0]
            else:
                start_date = ''
            if pd.notna(end_date):
                end_date = str(end_date).split('T')[0]
            else:
                end_date = ''
            
            project_info_dict[project_id_str] = {
                'name': project_name,
                'category_name': category_name,
                'start_date': start_date,
                'end_date': end_date,
                'client_name': get_client_name(project_name)
            }
    
    # Prepare people role mapping
    people_role_mapping = {}
    if 'id' in people_df.columns and 'role' in people_df.columns:
        for _, row in people_df.iterrows():
            person_id = row['id']
            role_field = row.get('role', '')
            role_id = extract_id_from_field(role_field)
            if role_id:
                people_role_mapping[person_id] = role_mapping.get(role_id, '')
    
    def get_employee_info(creator_id):
        if pd.isna(creator_id):
            return '', ''
        person = people_df[people_df['id'] == creator_id]
        if not person.empty:
            full_name = person.iloc[0].get('Full_Name', '') or person.iloc[0].get('name', '')
            role = people_role_mapping.get(creator_id, '')
            return full_name, role
        return '', ''
    
    def get_project_details(project_id):
        project_id_str = str(project_id)
        if project_id_str in project_info_dict:
            return project_info_dict[project_id_str]
        return {
            'name': f'Project_{project_id}',
            'category_name': '',
            'start_date': '',
            'end_date': '',
            'client_name': ''
        }
    
    # ================= FETCH TIME ENTRIES =================
    print("\n⏱️  Fetching timesheets and time entries...")
    all_projects = projects_df.to_dict('records')
    all_time_entries = []
    request_count = 0
    
    for i, project in enumerate(all_projects):
        project_id = project['id']
        project_details = get_project_details(project_id)
        
        if request_count >= 20:
            time.sleep(10)
            request_count = 0
        
        timesheets_url = f"{base_url}/projects/{project_id}/timesheets"
        timesheets_response = requests.get(timesheets_url, headers=headers)
        request_count += 1
        
        if timesheets_response.status_code == 200:
            timesheets_data = timesheets_response.json()
            timesheets = []
            if isinstance(timesheets_data, list):
                timesheets = timesheets_data
            elif isinstance(timesheets_data, dict):
                for key in ['timesheets', 'data', 'items']:
                    if key in timesheets_data and isinstance(timesheets_data[key], list):
                        timesheets = timesheets_data[key]
                        break
            
            for ts in timesheets:
                timesheet_id = ts.get('id')
                if timesheet_id:
                    page = 1
                    while True:
                        if request_count >= 20:
                            time.sleep(10)
                            request_count = 0
                        
                        entries_url = f"{base_url}/projects/{project_id}/timesheets/{timesheet_id}/time?page={page}"
                        entries_response = requests.get(entries_url, headers=headers)
                        request_count += 1
                        
                        if entries_response.status_code == 200:
                            entries_data = entries_response.json()
                            entries = entries_data if isinstance(entries_data, list) else entries_data.get('time_entries', [])
                            
                            if not entries:
                                break
                            
                            for entry in entries:
                                entry_date = entry.get('date', '')
                                if entry_date and entry_date >= '2025-01-01':
                                    creator_id = None
                                    employee_name = ''
                                    emp_role = ''
                                    
                                    if 'creator' in entry and isinstance(entry['creator'], dict):
                                        creator_id = entry['creator'].get('id', '')
                                        if creator_id:
                                            employee_name, emp_role = get_employee_info(creator_id)
                                    
                                    entry['creator_id'] = creator_id
                                    entry['employee_name'] = employee_name
                                    entry['emp_role'] = emp_role
                                    entry['project_id'] = project_id
                                    entry['project_name'] = project_details['name']
                                    entry['category_name'] = project_details['category_name']
                                    entry['project_start_date'] = project_details['start_date']
                                    entry['project_end_date'] = project_details['end_date']
                                    entry['client_name'] = project_details['client_name']
                                    entry['timesheet_id'] = timesheet_id
                                    entry['timesheet_title'] = ts.get('title', '')
                                    
                                    all_time_entries.append(entry)
                            
                            if len(entries) < 100:
                                break
                            page += 1
                            time.sleep(0.5)
                        elif entries_response.status_code == 429:
                            time.sleep(30)
                            continue
                        else:
                            break
        
        time.sleep(0.5)
    
    # ================= PROCESS DATA =================
    if all_time_entries:
        df = pd.DataFrame(all_time_entries)
        
        # Extract task details
        if 'task' in df.columns:
            def extract_task_details(task_obj):
                if isinstance(task_obj, dict):
                    return {
                        'task_list_id': task_obj.get('list_id', ''),
                        'task_list_name': task_obj.get('list_name', ''),
                        'task_id': task_obj.get('task_id', ''),
                        'task_name': task_obj.get('task_name', '')
                    }
                return {'task_list_id': '', 'task_list_name': '', 'task_id': '', 'task_name': ''}
            
            task_details = df['task'].apply(lambda x: pd.Series(extract_task_details(x)))
            df = pd.concat([df, task_details], axis=1)
        
        # Calculate totals
        df['logged_hours'] = pd.to_numeric(df['logged_hours'], errors='coerce').fillna(0)
        df['logged_mins'] = pd.to_numeric(df['logged_mins'], errors='coerce').fillna(0)
        df['total_mins'] = (df['logged_hours'] * 60) + df['logged_mins']
        df['total_hours'] = df['total_mins'] / 60.0
        df['total_hours'] = df['total_hours'].round(2)
        
        # Clean dates
        date_cols = ['project_start_date', 'project_end_date', 'date']
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.split('T').str[0]
                df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], '')
        
        # Update status
        if 'status' in df.columns:
            df['status'] = df['status'].apply(lambda x: 'billable' if str(x).lower() == 'billed' else str(x))
        
        # Ensure client_name column
        if 'client_name' not in df.columns:
            df['client_name'] = ''
        if 'project_name' in df.columns:
            df['client_name'] = df['project_name'].apply(get_client_name)
        
        # Final columns
        final_columns = [
            'creator_id', 'employee_name', 'emp_role', 'date', 'description',
            'logged_hours', 'logged_mins', 'total_mins', 'total_hours',
            'project_id', 'project_name', 'client_name', 'category_name',
            'project_start_date', 'project_end_date', 'status',
            'timesheet_id', 'timesheet_title',
            'task_list_id', 'task_list_name', 'task_id', 'task_name'
        ]
        
        existing_columns = [col for col in final_columns if col in df.columns]
        final_df = df[existing_columns].copy()
        
        # Save CSV locally
        output_filename = "All Projects Timesheet.csv"
        final_df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\n✅ DATA PROCESSING COMPLETE!")
        print(f"📊 Time entries: {len(final_df)}")
        print(f"📁 Local file saved: {output_filename}")
        
        return output_filename
    
    else:
        print("❌ No time entries found")
        return None

if __name__ == "__main__":
    # Run the main function
    csv_file = main()
    
    # Upload to Google Drive if file was created
    if csv_file:
        print("\n📤 UPLOADING TO GOOGLE DRIVE...")
        
        # Get folder ID from environment variable
        drive_folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
        
        if drive_folder_id:
            success = upload_to_google_drive(csv_file, drive_folder_id)
            if success:
                print("✅ File successfully uploaded to Google Drive!")
                print(f"📁 Path: My Drive/Zoho Analytics/ProofHub/All Projects Timesheet.csv")
            else:
                print("❌ Failed to upload to Google Drive")
        else:
            print("⚠️  No Google Drive folder ID provided. File saved locally only.")
    else:
        print("❌ No CSV file to upload")
