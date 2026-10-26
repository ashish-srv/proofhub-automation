# proofhub_automation.py
import requests
import pandas as pd
import time
import ast
import os
import sys
import json
from collections import deque
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# ================= CONFIGURATION =================
COMPANY_NAME = "srvmedia"
API_KEY = os.getenv('PROOFHUB_API_KEY')   # no hardcoded fallback - set the env var / GitHub secret
DATE_FROM = '2025-01-01'                   # entries before this date are dropped (intentional)

# ProofHub limit: 25 requests per 10 seconds per account + IP.
# We stay at 20 so other tools on the same account (n8n, timesheet filler, etc.) don't push us over.
RATE_LIMIT_REQUESTS = 20
RATE_LIMIT_WINDOW = 10.0
MAX_RETRIES = 8
REQUEST_TIMEOUT = 60

headers = {
    "X-API-KEY": API_KEY or "",
    "User-Agent": "ZohoIntegration (ashish.kate@srvmedia.com)",
    "Accept": "application/json"
}

base_url = f"https://{COMPANY_NAME}.proofhub.com/api/v3"

# ================= CLIENT NAME MAPPING =================
CLIENT_NAME_MAPPING = {
'#6791: scmhrd.edu files': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7055: Fwd: Eligibility of SCOP || Bachelors of Physiotherapy (BPT)': 'Symbiosis College of Physiotherapy (SCOP)',
'#7093: Update Blog - SRV Media': 'SRV',
'#7115: Website Updation || scmhrd.edu': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7125: Update Blog - SRV Media': 'SRV',
'#7159:  Update of Committee & SIG Member Photographs on Website': 'SIG - Symbiosis Institute  of Geo Informatics',
'#7159: Update of Committee & SIG Member Photographs on Website': 'SIG - Symbiosis Institute  of Geo Informatics',
'#7203: Change Blog Publication Date - SRV': 'SRV',
'#7204: Update Blog - SRV Media': 'SRV',
'#7221: Request to Upload "IIC Annual Report AY 2025-26" on SIBM-Noida Website': 'SIBM - Symbiosis Institute of Business Management - Pune',
'#7241: Request to Remove Banner & Link on SIBM Noida Website': 'SIBM Noida - Symbiosis Institute of Business Management Noida',
'#7243: ICESAB 2025 & SIIB International MUN 2026': 'SIIB - Symbiosis Institute of International Business',
'#7251:  SIBM Nagpur || Semrush Audit Fixes - May 2026': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'#7251: SIBM Nagpur || Semrush Audit Fixes - May 2026': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'#7289: Fwd: blog.siib.ac.in Site vulnerabilities found': 'SIIB - Symbiosis Institute of International Business',
'#7342: SRV Edge – May Month First Blog Topic (Approved Content)': 'SRV',
'#7349:  Update Blog - SRV Media': 'SRV',
'#7373: Home Page Redirection - SCMHRD': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7405: Actylis Lab Solutions | Issue with LinkedIn Preview for Published Blog': 'Actylis',
'#7443: Urgent Pending Updates for Committees and SIGs Section Website Pages': 'SIG - Symbiosis Institute  of Geo Informatics',
'Actylis - Pulse': 'Actylis',
'Actylislab (Finar Website) AMC': 'Actylis',
'Admission Fair': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Admissions Fair': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Admissions Fair 2026': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Afairs': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Afairs - MBA Expo': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Afairs 2024': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Afairs SM 2025': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'AMC _SRV_EDGE': 'SRV',
'AMC_Actylis Lab': 'Actylis',
'AMC_Blog SIIB': 'SIIB - Symbiosis Institute of International Business',
'AMC_D.Y.Patil': 'PADMASREE DR.D.Y.PATIL UNIVERSITY',
'AMC_ELTIS': 'ELTIS - English Language Teaching Institute of Symbiosis',
'AMC_Geta': 'GETA AI LABS PRIVATE LIMITED',
'AMC_IMS': 'IMS Unison University',
'AMC_ISKCON_PUNE': 'INTERNATIONAL SOCIETY FOR KRISHNA CONSCIOUSNESS',
'AMC_Mahindra University': 'MAHINDRA UNIVERSITY',
'AMC_N.L. Dalima': 'N. L. Dalmia Institute of Management Studies and Research',
'AMC_NIBM': 'NIBM - National Institute of Bank Management',
'AMC_Parul university_Website': 'Parul',
'AMC_Parul_PID': 'Parul',
'AMC_Prescient': 'PRESCIENT TECHNOLOGIES PRIVATE LIMITED',
'AMC_Scalasar_Slspune': 'SLS Pune - Symbiosis Law  School - Pune',
'AMC_SCCCS': 'SCCCS-Symbiosis Center for Climate Change and Sustainability',
'AMC_SCIT Blog': 'SCIT - Symbiosis Center  for Information Technology',
'AMC_SCMHRD': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'AMC_SCMS Hyd': 'SCMS HYD - Symbiosis Center for Management Studies - Hyderabad',
'AMC_SCMS Hyd Blog': 'SCMS HYD - Symbiosis Center for Management Studies - Hyderabad',
'AMC_SCMS Noida': 'SCMS Noida - Symbiosis Centre for  Management Studies,Noida',
'AMC_SCMS_NOIDA': 'SCMS Noida - Symbiosis Centre for  Management Studies,Noida',
'AMC_SCRI': 'SCRI - Symbiosis Centre  of Research & Innovation',
'AMC_Set': 'SET',
'AMC_SIBM hyd': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'AMC_SIBM Nagpur': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'AMC_SIBM Noida': 'SIBM Noida - Symbiosis Institute of Business Management Noida',
'AMC_SIBM Pune': 'SIBM - Symbiosis Institute of Business Management - Pune',
'AMC_SIBM Pune Blog': 'SIBM - Symbiosis Institute of Business Management - Pune',
'AMC_SIIB website': 'SIIB - Symbiosis Institute of International Business',
'AMC_SIT Hyd': 'SIT Hyderabad- Symbiosis Institute of Technology',
'AMC_SIT Nagpur': 'SIT Nagpur-Symbiosis Institute of Technology',
'AMC_SIU Dubai': 'Symbiosis International (Deemed University) Registrar, SIU',
'AMC_SLAT': 'SLAT',
'AMC_Snap': 'SNAP',
'AMC_SRV': 'SRV',
'AMC_SRV MEdia Blog': 'SRV',
'AMC_SRV_PR': 'SRV',
'AMC_Symlaw (SLS PUNE)': 'SLS Pune - Symbiosis Law  School - Pune',
'AMC_Thermax': 'Thermax Limited',
'AMC_WIDMA': 'KENNAMETAL INDIA LIMITED',
'AMC_Winsoft Tech': 'Winsoft Technologies India Pvt Ltd',
'AMC_XATOnline': 'XAT',
'ASBM': 'ASBM University',
'ASBM - Mumbai': 'ASBM University',
'ASBM - PR Mandate (2025 -26)': 'ASBM University',
'ASBM LP': 'ASBM University',
'AURAK': 'AURAK',
'Auro University': 'Auro University',
'Babu Bansari Das Uni': 'BABU BANARASI DAS UNIVERSITY',
'Bangkok University': 'BANGKOK UNIVERSITY',
'Bank Of Maharashtra': 'Bank of Maharashtra',
'BBDU': 'BABU BANARASI DAS UNIVERSITY',
'Bharti Vidya Peeth University': 'BHARATI VIDYAPEETH',
'Bharti Vidyapeeth': 'BHARATI VIDYAPEETH',
'Bhawanipur Global Campus': 'Bhawanipur Global Campus',
'Bhawanipur Global Campus LP': 'Bhawanipur Global Campus',
'Birbeck University, London': 'Birkbeck University',
'BLOG_SCMHRD_AMC': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'CALMU': 'California Miramar University (CMU)',
'CGC Jhanjeri': 'Chandigarh Educational Society Jhanjeri',
'CGC Landran': 'CGC,Landran - Chandigarh Group  of Colleges',
'Chanakya University': 'THE CHANAKYA UNIVERSITY',
'chanakya university': 'THE CHANAKYA UNIVERSITY',
'Chandigarh University': 'Chandigarh University',
'DES PU': 'DES Pune University Pune',
'DESPU LP': 'DES Pune University Pune',
'DY Patil - Navi Mumbai': 'PADMASHREE DR.D.Y.PATIL UNIVERSITY',
'DYPBS': 'Dr. D.Y. Patil B-School',
'DYPU': 'PADMASHREE DR.D.Y.PATIL UNIVERSITY',
'Easebuzz': 'Easebuzz Private Limited',
'EDII': 'ENTREPRENEURSHIP DEVELOPMENT INSTITUTE OF INDIA',
'EDII - Performance': 'ENTREPRENEURSHIP DEVELOPMENT INSTITUTE OF INDIA',
'ELITS - SIFIL': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'Eltis': 'ELTIS - English Language Teaching Institute of Symbiosis',
'ELTIS SIFIL': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'ESMOD Dubai': 'ESMOD Dubai',
'ESMOD LP': 'ESMOD Dubai',
'Finar': 'Actylis',
'Finnacle Institute': 'FINNACLE INSTITUTE PRIVATE LIMITED',
'Finnacle Shah': 'FINNACLE INSTITUTE PRIVATE LIMITED',
'Forbes Marshall': 'FORBES MARSHALL PVT LTD',
'Galgotia University': 'Galgotias University',
'GD Goenka': 'GD Goenka',
'GD Goenka Healthcare Franchise 2026-26': 'GD Goenka',
'GD Goenka High School 2026-27': 'GD Goenka',
'GD Goenka School Franchise': 'GD Goenka',
'GD Goenka University 2026 - 2027': 'G D GOENKA UNIVERSITY',
'GD Goenka World School 2026-27': 'GD Goenka',
'Geta': 'GETA AI LABS PRIVATE LIMITED',
'Geta MSG': 'GETA AI LABS PRIVATE LIMITED',
'GRD IMT': 'GRD Institute of Management & Technology',
'HITAM': 'Hyderabad Institute of Technology and Management-HITAM',
'Hyundai SM': 'HD HYUNDAI CONSTRUCTION EQUIPMENT INDIA PRIVATE LIMITED',
'IEM': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEM Kolkata - JEE': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEM MBA 25-26': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEMJEE -B.Tech 2026': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IIM Kozhikode': 'INDIAN INSTITUTE OF MANAGEMENT KOZHIKODE',
'IIM Udaipur': 'Indian Institute of Management Udaipur- IIM Udaipur',
'IIT Kharagpur Law': 'IIT Kharagpur',
'IITB Monash 2026': 'IITB-Monash Research Academy',
'IMS': 'IMS Unison University',
'IMS Noida': 'IMS NOIDA',
'IMS Social Media': 'IMS Unison University',
'IMS Unison 2026-2027': 'IMS Unison University',
'IMS Unison University': 'IMS Unison University',
'IMT [G] - PGDM ExP': 'M/S INSTITUTE OF MANAGEMENT TECHNOLOGY',
'Jio Institute': 'RELIANCE FOUNDATION INSTITUTION OF EDUCATION AND RESEARCH (JIO)',
'JIO University': 'RELIANCE FOUNDATION INSTITUTION OF EDUCATION AND RESEARCH (JIO)',
'K. K. Nag': 'K K NAG PRIVATE LIMITED',
'Kennametal (WIDMA)': 'KENNAMETAL INDIA LIMITED',
'KJ Somaiya': 'K J Somaiya Institute of Management',
'KJ Somaiya - MBA': 'K J Somaiya Institute of Management',
'KK NAG Landing page': 'K K NAG PRIVATE LIMITED',
'Krishnayan': 'Shree Krishnayan',
'Krishnayan SM': 'Shree Krishnayan',
'LIBA': 'LOYOLA INSTITUTE OF BUSINESS ADMINISTRATION (A UNIT OF LOYOLA COLLEGE SOCIETY)',
'LP_ PCU Microsite GMG PCU': 'Pimpri Chinchwad University',
'LP_AMC_Afairs': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'LP_AMC_BalajiDDSS': 'Sri Balaji University, Pune',
'LP_Kennametal': 'KENNAMETAL INDIA LIMITED',
'LP_MITWPU_AMC': 'Dr.Vishwanath Karad MIT World Peace University',
'LP_NITTE': 'NITTE',
'LP_QMUL_AMC': 'Queen Mary University of London',
'LP_TEDx@BalaJi': 'Sri Balaji University, Pune',
'LP_Way2Admssion': 'Way 2 Admission Pvt Ltd',
'Mahe': 'MANIPAL ACADEMY OF HIGHER EDUCATION',
'MAHE': 'MANIPAL ACADEMY OF HIGHER EDUCATION',
'MAHE Bengaluru': 'MANIPAL ACADEMY OF HIGHER EDUCATION',
'MAHE Dubai': 'MANIPAL DUBAI',
'MAHE Dubai Campus': 'MANIPAL DUBAI',
'MCX': 'MCX INVESTOR PROTECTION FUND',
'MIT VPU (Solapur)': 'MIT VISHWAPRAYAG UNIVERSITY',
'MIT VPU 2026': 'MIT VISHWAPRAYAG UNIVERSITY',
'MIT World Peace University': 'Dr.Vishwanath Karad MIT World Peace University',
'MIT WPU': 'Dr.Vishwanath Karad MIT World Peace University',
'MIT-VPU': 'MIT VISHWAPRAYAG UNIVERSITY',
'Muthoot Business School': 'Muthoot Business School',
'N L Dalmia Ads': 'N. L. Dalmia Institute of Management Studies and Research',
'NFSU': 'NFSU - NATIONAL FORENSIC SCIENCES UNIVERSITY',
'NIF Kothrud': 'NIF',
'NIF Mumbai': 'NIF',
'NIFD Kothrud': 'NIF',
'NIFD Mumbai': 'NIF',
'Nirma MBA &amp; MBA HRM': 'NIRMA',
'Nirma MBA & MBA HRM': 'NIRMA',
'Nirma MBA SM': 'NIRMA',
'Nirma MBA Social Media': 'NIRMA',
'Nirma University': 'NIRMA',
'Nitte Hospital': 'Nitte Hospital',
'NITTE SM': 'NITTE University',
'Nitte University': 'NITTE University',
'NL Dalmia University': 'N. L. Dalmia Institute of Management Studies and Research',
'Orbit group': 'ORBIT',
'Orbit Urban Park Commercial LP 2026': 'ORBIT',
'Orbit Urban Park Residential LP 2026': 'ORBIT',
'Parul Ayurveda': 'Parul',
'Parul Goa': 'Parul',
'Parul Hospital Ads': 'Parul Hospital',
'Parul Sevashram Hospital': 'Parul Hospital',
'PaySquare': 'PAYSQUARE CONSULTANCY LIMITED',
'PBS': 'Pune Business School',
'PBS 2026': 'Pune Business School',
'PCET': 'PCET International',
'PCET - 2026': 'PCET International',
'PCET International': 'PCET International',
'PCU': 'Pimpri Chinchwad University',
'PCU - 2026': 'Pimpri Chinchwad University',
'PDEU': 'PANDIT DEENDAYAL ENERGY UNIVERSITY',
'PDEU Brand': 'PANDIT DEENDAYAL ENERGY UNIVERSITY',
'PIU 2026': 'Plastindia International University',
'Plaksha University': 'PLAKSHA UNIVERSITY',
'PlastIndia': 'Plastindia International University',
'Prescient': 'PRESCIENT TECHNOLOGIES PRIVATE LIMITED',
'Prescient Technologies': 'PRESCIENT TECHNOLOGIES PRIVATE LIMITED',
'Pune Business School': 'Pune Business School',
'QMUL Academics Promotions': 'Queen Mary University of London',
'Queen Mary University - London': 'Queen Mary University of London',
'RV University': 'RV UNIVERSITY',
'SAI Sudha Lawn': 'Parul Sai Sudha Lawns',
'SAII': 'Symbiosis Artificial Intelligence Institute (SAII)',
'SAII Website': 'Symbiosis Artificial Intelligence Institute (SAII)',
'SBUP - DDSS': 'Sri Balaji University, Pune',
'SBUP - MBA 2025': 'Sri Balaji University, Pune',
'SBUP MBA 2025 - 26': 'Sri Balaji University, Pune',
'SBUP PhD 2025': 'Sri Balaji University, Pune',
'SBUP UG PG 2025': 'Sri Balaji University, Pune',
'SCAC': 'SCAC- Principal Symbiosis College of Arts and Commerce',
'SCIT': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2023-24': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2024': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2025': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT Pune': 'SCIT - Symbiosis Center  for Information Technology',
'SCMC 2025': 'SCMC - Symbiosis Centre for Media & Communication',
'SCMC AY 2025': 'SCMC - Symbiosis Centre for Media & Communication',
'SCMHRD': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'SCMHRD 2024': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'SCMHRD 2026': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'SCMHRD Executive MBA': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'SCMS Bengaluru': 'Symbiosis Centre for Management Studies-SCMS Bengaluru',
'SCMS Hyd 2025': 'SCMS HYD - Symbiosis Center for Management Studies - Hyderabad',
'SCMS Hyderabad': 'SCMS HYD - Symbiosis Center for Management Studies - Hyderabad',
'SCMS Nagpur AY 2025': 'SCMS Symbiosis Centre of Management Studies - Nagpur',
'SCMS Noida': 'SCMS Noida - Symbiosis Centre for  Management Studies,Noida',
'SCMS Pune': 'SCMS - Symbiosis Center for Management Studies Pune',
'SCOP': 'Symbiosis College of Physiotherapy (SCOP)',
'SCOP 2025': 'Symbiosis College of Physiotherapy (SCOP)',
'SCRI Pune 2025': 'SCRI - Symbiosis Centre  of Research & Innovation',
'SCSD 2025': 'SCSD - Symbiosis Center for Skill Development Nagpur',
'SCY': 'Symbiosis Center for Yoga (SCY)',
'SET': 'SET',
'SET 2026': 'SET',
'SET SM 2025': 'SET',
'SET Website': 'SET',
'SIBM Bangalore': 'SIBM B - Symbiosis Institute of Business Management, Bangalore',
'SIBM Bengaluru 2025': 'SIBM B - Symbiosis Institute of Business Management, Bangalore',
'SIBM Hyd AY 2025': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'SIBM Hyderabad': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'SIBM Nagpur': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM Nagpur AY 2025': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM Nagpur-Mobile': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM NOIDA 2026': 'SIBM Noida - Symbiosis Institute of Business Management Noida',
'SIBM Pune': 'SIBM - Symbiosis Institute of Business Management - Pune',
'SIBM PUNE 2025': 'SIBM - Symbiosis Institute of Business Management - Pune',
'SICSR': 'SICSR - Symbiosis Institute of  Computer Studies & Research',
'SICSR 2024': 'SICSR - Symbiosis Institute of  Computer Studies & Research',
'SICSR 2025': 'SICSR - Symbiosis Institute of  Computer Studies & Research',
'SID 2025': 'SID - Symbiosis Institute of Design',
'SIDTM AY 2025': 'SIDTM - Symbiosis Institute of Digital and Telecom Management',
'SIES SBS': 'The South Indian Education Society (SIES) & SIES School of Business Studies',
'SIESSBS': 'The South Indian Education Society (SIES) & SIES School of Business Studies',
'SIFIL': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'SIFIL & Eltis (SEO)': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'SIFIL Blog': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'SIG': 'SIG - Symbiosis Institute  of Geo Informatics',
'SIIB 2024': 'SIIB - Symbiosis Institute of International Business',
'SIIB 2026': 'SIIB - Symbiosis Institute of International Business',
'SIMC AY 2025': 'SIMC - Symbiosis Institute of Media & Communication',
'SIOM': 'SIOM - Symbiosis Institute  of Operation Management',
'SITEEE SM 2025': 'Symbiosis Institute of Technology Engineering Entrance Examination',
'SIT Hyd 2026': 'SIT Hyderabad- Symbiosis Institute of Technology',
'SIT Hyderabad': 'SIT Hyderabad- Symbiosis Institute of Technology',
'SIT Nagpur 2025': 'SIT Nagpur-Symbiosis Institute of Technology',
'SIT Pune 2024': 'SIT - Symbiosis Institute of Technology Pune',
'SIT Pune 2025': 'SIT - Symbiosis Institute of Technology Pune',
'SIT Pune 2026': 'SIT - Symbiosis Institute of Technology Pune',
'SLAT': 'SLAT',
'SLAT 2024': 'SLAT',
'SLAT SM 2025': 'SLAT',
'SLAT Social media': 'SLAT',
'SLS Hyderabad 2025': 'SLS Hyderabad - Symbiosis Law School Hyderabad',
'SLS Hyderabad AMC': 'SLS Hyderabad - Symbiosis Law School Hyderabad',
'SLS Nagpur 2025': 'SLS Nagpur - Symbiosis Law School Nagpur',
'SLS Pune': 'SLS Pune - Symbiosis Law  School - Pune',
'SNAP': 'SNAP',
'SNAP 2025': 'SNAP',
'SNAP SM 2024': 'SNAP',
'SNAP SM 2025': 'SNAP',
'Somaiya Vidyavihar University (SVU)': 'Somaiya Vidyavihar University',
'SPJIMR': 'SPJIMR-S. P. Jain Institute of Management and Research',
'SPJIMR PGPM': 'SPJIMR-S. P. Jain Institute of Management and Research',
'Sri Balaji PHD': 'Sri Balaji University, Pune',
'SRV': 'SRV',
'SRV Dubai': 'SRV',
'SRV Edge': 'SRV',
'SRV Edge website blog KT': 'SRV',
'SRV Media': 'SRV',
'SRV MEDIA & EDGE social media': 'SRV',
'SRV Operations': 'SRV',
'SRV PR': 'SRV',
'SRV Web Masters': 'SRV',
'SSBF 2025': 'SSBF - Symbiosis School of Banking of Finance',
'SSBS 2026': 'Symbiosis School of Biological Sciences-SSBS',
'SSE': 'Symbiosis School of Economics-SSE',
'SSE - Blog': 'Symbiosis School of Economics-SSE',
'SSE- 2024': 'Symbiosis School of Economics-SSE',
'SSI 2025': 'SSI - Symbiosis Statistical Institute',
'SSIS, Pune': 'SSIS-Symbiosis School of International Studies',
'SSL': 'SSLA - Symbiosis School of Liberal Arts',
'SSODL': 'SSODL- Symbiosis School for Online and Digital Learning',
'SSODL - PR Mandate': 'SSODL- Symbiosis School for Online and Digital Learning',
'SSSS': 'SSSS - Symbiosis School of  Sport Sciences',
'SSVAP 2026': 'SSVAP - Symbiosis School of  Visual Arts And Photography',
'Study From India': 'Study From India',
'Study from India': 'Study From India',
'Study from UAE': 'Study from UAE',
'Study From UAE SM': 'Study from UAE',
'Symbiosis International School': 'Symbiosis International School (SIS)',
'Symbiosis School of Biological Sciences (SSBS)': 'Symbiosis School of Biological Sciences-SSBS',
'PMRDA 2025-26': 'Pune Metropolitan Region Development Authority, Pune-PMRDA',
'SCMS Pune 2026': 'SCMS - Symbiosis Center for Management Studies Pune',
'SIBM Bengaluru - 2026': 'SIBM B - Symbiosis Institute of Business Management, Bangalore',
'SIBM Hyd 2026': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'SIBM Nagpur 2026': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM PUNE 2026': 'SIBM - Symbiosis Institute of Business Management - Pune',
'SIDTM 2026': 'SIDTM - Symbiosis Institute of Digital and Telecom Management',
'SIMC 2026': 'SIMC - Symbiosis Institute of Media & Communication',
'SLS Hyderabad 2026': 'SLS Hyderabad - Symbiosis Law School Hyderabad',
'SLS Nagpur 2026': 'SLS Nagpur - Symbiosis Law School Nagpur',
'SNAP 2026': 'SNAP',
'SRV MEDIA &amp; EDGE social media': 'SRV',
'SSBF 2026': 'SSBF - Symbiosis School of Banking of Finance',
'Swarrnim University Social Media': 'SWARRNIM SCHOOL (GYAPRASAD JAIN CHERITABLE TRUST)',
'Techademy': 'TECHADEMY LEARNING SOLUTIONS PRIVATE LIMITED',
'TSLAS (Thapar) 26-27': 'THAPAR INSTITUTE OF ENGINEERING & TECHNOLOGY',
'TSLAS LP': 'THAPAR INSTITUTE OF ENGINEERING & TECHNOLOGY',
'TSM  2026-27': 'TSM Madurai',
'Uniaptic': 'Uniaptix India',
'Victorious Kidss Educares (VKE)': 'VICTORIOUS KIDSS EDUCARES PRIVATE LIMITED',
'Way2 Admission': 'Way 2 Admission Pvt Ltd',
'Way2Admission': 'Way 2 Admission Pvt Ltd',
'Way2Admission 2026': 'Way 2 Admission Pvt Ltd',
'Welingkar PGDM 26-27': 'Prin. L.N. Welingkar Inst. of Mgt. Development & Research',
'WeSchool 2025': 'Prin. L.N. Welingkar Inst. of Mgt. Development & Research',
'WeSchool FTM 2026': 'Prin. L.N. Welingkar Inst. of Mgt. Development & Research',
'WPU GOA': 'Dr.Vishwanath Karad MIT World Peace University',
'WPU Goa': 'Dr.Vishwanath Karad MIT World Peace University',
'XAT': 'XAT',
'XAT 2026 - PR Outreach': 'XAT',
'XAT 2026 Social Media': 'XAT',
'XIM': 'XIM UNIVERSITY',
'XIM University': 'XIM UNIVERSITY',
'XLRI Jamshedpur': 'XAT',
'XLRI Jamshedpur SM': 'XAT',
'XLRI PGDM GM': 'XAT',
'XLRI VIL': 'XAT',
'XLRI XOL': 'XAT',
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

        service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            print("❌ GOOGLE_SERVICE_ACCOUNT_KEY is missing!")
            return False

        try:
            service_account_info = json.loads(service_account_json)
            print(f"✅ Service Account: {service_account_info.get('client_email')}")
        except json.JSONDecodeError as e:
            print(f"❌ JSON error: {e}")
            return False

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)

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

        file_metadata = {
            'name': 'All Projects Timesheet.csv',
            'parents': [folder_id]
        }

        media = MediaFileUpload(
            file_path,
            mimetype='text/csv',
            resumable=True
        )

        print(f"🔍 Searching for existing file...")
        response = service.files().list(
            q=f"name='All Projects Timesheet.csv' and '{folder_id}' in parents and trashed=false",
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        if response.get('files'):
            file_id = response['files'][0]['id']
            print(f"🔄 Updating existing file (ID: {file_id})")
            file = service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            print(f"✅ Updated file in Shared Drive: {file.get('id')}")
        else:
            print(f"📄 Creating new file in Shared Drive...")
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"✅ Created new file in Shared Drive: {file.get('id')}")

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


# ================= PROOFHUB API (rate limit + retry) =================
_request_times = deque()


def _wait_for_slot():
    """Sliding-window limiter: never more than RATE_LIMIT_REQUESTS in any RATE_LIMIT_WINDOW seconds."""
    while True:
        now = time.monotonic()
        while _request_times and now - _request_times[0] >= RATE_LIMIT_WINDOW:
            _request_times.popleft()
        if len(_request_times) < RATE_LIMIT_REQUESTS:
            _request_times.append(now)
            return
        time.sleep(RATE_LIMIT_WINDOW - (now - _request_times[0]) + 0.05)


def api_get(endpoint, params=None):
    """
    GET a ProofHub endpoint. Retries 429 (honouring Retry-After), 5xx and network errors.
    Returns (data, None) on success, (None, error_text) on failure. Never fails silently.
    """
    url = f"{base_url}/{endpoint}"
    last_error = ''
    for attempt in range(1, MAX_RETRIES + 1):
        _wait_for_slot()
        try:
            r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = f"network error {type(e).__name__}: {e}"
            wait = min(5 * attempt, 60)
            print(f"   ⚠️  {endpoint} {params or ''} → {last_error} | retry {attempt}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
            continue

        if r.status_code == 200:
            try:
                return r.json(), None
            except ValueError:
                last_error = f"HTTP 200 but invalid JSON: {r.text[:200]}"
                wait = min(5 * attempt, 60)
                print(f"   ⚠️  {endpoint} {params or ''} → {last_error} | retry {attempt}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                continue

        if r.status_code == 429:
            try:
                wait = max(int(float(r.headers.get('Retry-After', 15))), 1)
            except ValueError:
                wait = 15
            last_error = "HTTP 429 rate limited"
            print(f"   ⏳ {endpoint} {params or ''} → 429, waiting {wait}s (retry {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        if r.status_code >= 500:
            last_error = f"HTTP {r.status_code}: {r.text[:200]}"
            wait = min(10 * attempt, 120)
            print(f"   ⚠️  {endpoint} {params or ''} → {last_error} | retry {attempt}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
            continue

        # Other 4xx: retrying won't help
        return None, f"HTTP {r.status_code}: {r.text[:200]}"

    return None, f"gave up after {MAX_RETRIES} retries ({last_error})"


def extract_list(data, keys):
    """Pull the list of records out of a response. Returns None if the shape is unexpected."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in keys:
            if isinstance(data.get(k), list):
                return data[k]
    return None


def fetch_list(endpoint, keys, params=None, max_pages=1000):
    """
    Page through an endpoint until a page is empty or brings nothing new
    (projects/people/timesheets ignore ?page and resend the same list; time entries
    really paginate, 500 per page). Returns (items, error). On error, items holds
    whatever was fetched before the failure.
    """
    all_items, seen = [], set()
    page = 1
    while page <= max_pages:
        p = {'page': page}
        if params:
            p.update(params)
        data, err = api_get(endpoint, p)
        if err:
            return all_items, f"page {page}: {err}"

        items = extract_list(data, keys)
        if items is None:
            shape = list(data.keys())[:10] if isinstance(data, dict) else type(data).__name__
            return all_items, f"page {page}: unexpected response shape {shape}"
        if not items:
            break

        new, has_ids = [], False
        for it in items:
            item_id = it.get('id') if isinstance(it, dict) else None
            if item_id is None:
                new.append(it)
                continue
            has_ids = True
            if str(item_id) not in seen:
                seen.add(str(item_id))
                new.append(it)

        if has_ids and not any(isinstance(it, dict) and it.get('id') is not None for it in new):
            break  # page repeated what we already have
        all_items.extend(new)
        if not has_ids:
            break  # can't detect repeats without IDs, so don't risk looping
        page += 1

    return all_items, None


def extract_id_from_field(field_value):
    """Extract ID from field that might be dict/JSON string"""
    if field_value is None:
        return None
    try:
        if isinstance(field_value, dict):
            return str(field_value['id']) if field_value.get('id') is not None else None
        if pd.isna(field_value):
            return None
        if isinstance(field_value, str) and field_value.strip().startswith('{'):
            field_dict = ast.literal_eval(field_value.strip())
            if isinstance(field_dict, dict) and field_dict.get('id') is not None:
                return str(field_dict['id'])
    except Exception:
        pass
    return None


def fetch_reference(endpoint, keys, params=None, label=None):
    """Reference data must be complete, otherwise the whole output is wrong - stop the run."""
    label = label or endpoint
    items, err = fetch_list(endpoint, keys, params)
    if err:
        print(f"❌ Could not fetch {label}: {err}")
        print("   Stopping without overwriting the existing file in Drive.")
        sys.exit(1)
    print(f"   ✅ {label}: {len(items)}")
    return items


# ================= MAIN FUNCTION =================
def main():
    print("🚀 STARTING PROOFHUB AUTOMATION")
    print("=" * 70)

    if not API_KEY:
        print("❌ PROOFHUB_API_KEY environment variable is not set")
        sys.exit(1)

    failures = []

    # ---------- Reference data ----------
    print("📥 Downloading reference data...")
    people = fetch_reference("people", ["people", "data", "items", "users"], label="people")
    roles = fetch_reference("roles", ["roles", "data", "items"], label="roles")
    categories = fetch_reference("categories", ["categories", "data", "items"], label="categories")
    active_projects = fetch_reference("projects", ["projects", "data", "items"], label="projects (active)")
    archived_projects = fetch_reference("projects", ["projects", "data", "items"],
                                        params={"archived": "true"}, label="projects (archived)")

    role_mapping = {str(r['id']): r.get('name', '') for r in roles if isinstance(r, dict) and r.get('id') is not None}
    category_mapping = {str(c['id']): c.get('name', '') for c in categories if isinstance(c, dict) and c.get('id') is not None}

    people_by_id = {}
    for p in people:
        if not isinstance(p, dict) or p.get('id') is None:
            continue
        full_name = f"{str(p.get('first_name') or '').strip()} {str(p.get('last_name') or '').strip()}".strip()
        role_id = extract_id_from_field(p.get('role'))
        people_by_id[str(p['id'])] = {
            'name': full_name or p.get('name', '') or '',
            'role': role_mapping.get(role_id, '') if role_id else '',
            'email': p.get('email', '') or '',
        }

    def get_employee_info(creator_id):
        person = people_by_id.get(str(creator_id))
        if person:
            return person['name'], person['role'], person['email']
        return '', '', ''

    # Merge active + archived projects (archived ones were never fetched before)
    projects = {}
    for p in active_projects + archived_projects:
        if isinstance(p, dict) and p.get('id') is not None and str(p['id']) not in projects:
            projects[str(p['id'])] = p
    print(f"   📦 Total unique projects: {len(projects)}")

    project_info_dict = {}
    for project_id_str, row in projects.items():
        project_name = row.get('title') or row.get('name') or f'Project_{project_id_str}'
        category_id = extract_id_from_field(row.get('category'))
        start_date = str(row.get('start_date') or '').split('T')[0]
        end_date = str(row.get('end_date') or '').split('T')[0]
        project_info_dict[project_id_str] = {
            'name': project_name,
            'category_name': category_mapping.get(category_id, '') if category_id else '',
            'start_date': start_date,
            'end_date': end_date,
            'client_name': get_client_name(project_name),
        }

    # ---------- Time entries ----------
    print("\n⏱️  Fetching timesheets and time entries...")
    all_time_entries = []
    total = len(projects)

    for i, (project_id_str, project) in enumerate(projects.items(), start=1):
        project_id = project['id']
        details = project_info_dict[project_id_str]
        label = f"  [{i}/{total}] {project_id} '{details['name']}'"

        timesheets, err = fetch_list(f"projects/{project_id}/timesheets", ["timesheets", "data", "items"])
        if err:
            failures.append({'project_id': project_id, 'project_name': details['name'],
                             'stage': 'timesheets', 'timesheet_id': '', 'error': err})
            print(f"{label} ❌ timesheets failed: {err}")
            continue

        project_entries = 0
        for ts in timesheets:
            timesheet_id = ts.get('id') if isinstance(ts, dict) else None
            if not timesheet_id:
                continue

            entries, err = fetch_list(f"projects/{project_id}/timesheets/{timesheet_id}/time",
                                      ["time_entries", "time", "data", "items"])
            if err:
                failures.append({'project_id': project_id, 'project_name': details['name'],
                                 'stage': 'time entries', 'timesheet_id': timesheet_id, 'error': err})
                print(f"{label} ❌ timesheet {timesheet_id} entries failed "
                      f"(kept {len(entries)} fetched before the error): {err}")

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_date = str(entry.get('date') or '')
                if not entry_date or entry_date < DATE_FROM:
                    continue

                creator_id = None
                employee_name, emp_role, emp_email = '', '', ''
                if isinstance(entry.get('creator'), dict):
                    creator_id = entry['creator'].get('id', '')
                    if creator_id:
                        employee_name, emp_role, emp_email = get_employee_info(creator_id)

                entry['creator_id'] = creator_id
                entry['employee_name'] = employee_name
                entry['emp_role'] = emp_role
                entry['Email Address'] = emp_email
                entry['project_id'] = project_id
                entry['project_name'] = details['name']
                entry['category_name'] = details['category_name']
                entry['project_start_date'] = details['start_date']
                entry['project_end_date'] = details['end_date']
                entry['client_name'] = details['client_name']
                entry['timesheet_id'] = timesheet_id
                entry['timesheet_title'] = ts.get('title', '')

                all_time_entries.append(entry)
                project_entries += 1

        print(f"{label} → {len(timesheets)} timesheets, {project_entries} entries since {DATE_FROM}")

    # ================= PROCESS DATA =================
    if not all_time_entries:
        print("❌ No time entries found")
        return None, failures

    df = pd.DataFrame(all_time_entries)

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

    df['logged_hours'] = pd.to_numeric(df['logged_hours'], errors='coerce').fillna(0)
    df['logged_mins'] = pd.to_numeric(df['logged_mins'], errors='coerce').fillna(0)
    df['total_mins'] = (df['logged_hours'] * 60) + df['logged_mins']
    df['total_hours'] = (df['total_mins'] / 60.0).round(2)

    for col in ['project_start_date', 'project_end_date', 'date']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.split('T').str[0]
            df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], '')

    if 'status' in df.columns:
        df['status'] = df['status'].apply(lambda x: 'billable' if str(x).lower() == 'billed' else str(x))

    if 'project_name' in df.columns:
        df['client_name'] = df['project_name'].apply(get_client_name)
    elif 'client_name' not in df.columns:
        df['client_name'] = ''

    # Same columns as before, so the Zoho import is unaffected
    final_columns = [
        'creator_id', 'employee_name', 'Email Address', 'emp_role', 'date', 'description',
        'logged_hours', 'logged_mins', 'total_mins', 'total_hours',
        'project_id', 'project_name', 'client_name', 'category_name',
        'project_start_date', 'project_end_date', 'status',
        'timesheet_id', 'timesheet_title',
        'task_list_id', 'task_list_name', 'task_id', 'task_name'
    ]
    existing_columns = [col for col in final_columns if col in df.columns]
    final_df = df[existing_columns].copy()

    output_filename = "All Projects Timesheet.csv"
    final_df.to_csv(output_filename, index=False, encoding='utf-8')

    print(f"\n✅ DATA PROCESSING COMPLETE!")
    print(f"📊 Time entries: {len(final_df)}")
    print(f"📊 Projects with entries: {final_df['project_id'].nunique()}")
    print(f"📊 Employees with entries: {final_df['creator_id'].nunique()}")
    unmatched = final_df.loc[final_df['employee_name'].fillna('') == '', 'creator_id'].dropna().unique()
    if len(unmatched):
        print(f"⚠️  Creator IDs not found in /people (name/email blank): {list(unmatched)}")
    print(f"📁 Local file saved: {output_filename}")

    return output_filename, failures


if __name__ == "__main__":
    csv_file, failures = main()
    exit_code = 0

    if csv_file:
        print("\n📤 UPLOADING TO GOOGLE DRIVE...")
        drive_folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
        if drive_folder_id:
            if upload_to_google_drive(csv_file, drive_folder_id):
                print("✅ File successfully uploaded to Google Drive!")
            else:
                print("❌ Failed to upload to Google Drive")
                exit_code = 1
        else:
            print("⚠️  No Google Drive folder ID provided. File saved locally only.")
    else:
        print("❌ No CSV file to upload")
        exit_code = 1

    if failures:
        print("\n" + "=" * 70)
        print(f"❌ {len(failures)} FETCH FAILURE(S) - data below is MISSING or PARTIAL in this run:")
        print("=" * 70)
        for f in failures:
            ts = f" timesheet {f['timesheet_id']}" if f['timesheet_id'] else ''
            print(f"  • {f['project_id']} '{f['project_name']}' [{f['stage']}{ts}] → {f['error']}")
        exit_code = 1
    else:
        print("\n✅ All projects and timesheets fetched with no failures.")

    sys.exit(exit_code)
