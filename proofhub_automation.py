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
    '#6791: scmhrd.edu files': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7055: Fwd: Eligibility of SCOP || Bachelors of Physiotherapy (BPT)': 'Symbiosis College of Physiotherapy (SCOP)',
'#7093: Update Blog - SRV Media': 'SRV',
'#7115: Website Updation || scmhrd.edu': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7115: Website Updation || scmhrd.edu': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'#7125: Update Blog - SRV Media': 'SRV',
'#7159:  Update of Committee & SIG Member Photographs on Website': 'SIG - Symbiosis Institute  of Geo Informatics',
'#7159: Update of Committee & SIG Member Photographs on Website': 'SIG - Symbiosis Institute  of Geo Informatics',
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
'#7405: Actylis Lab Solutions | Issue with LinkedIn Preview for Published Blog': 'Actylis',
'#7405: Actylis Lab Solutions | Issue with LinkedIn Preview for Published Blog': 'Actylis',
'#7443: Urgent Pending Updates for Committees and SIGs Section Website Pages': 'SIG - Symbiosis Institute  of Geo Informatics',
'Actylis - Pulse': 'Actylis',
'Actylislab (Finar Website) AMC': 'Actylis',
'Admission Fair': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Admissions Fair': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Admissions Fair 2026': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
'Afairs': 'AFAIRS EXHIBITIONS & MEDIA PRIVATE LIMITED',
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
'AURAK': 'AURAK',
'Auro University': 'Auro University',
'Babu Bansari Das Uni': 'BABU BANARASI DAS UNIVERSITY',
'Bangkok University': 'BANGKOK UNIVERSITY',
'Bangkok University': 'BANGKOK UNIVERSITY',
'Bank Of Maharashtra': 'Bank of Maharashtra',
'BBDU': 'BABU BANARASI DAS UNIVERSITY',
'Bharti Vidya Peeth University': 'BHARATI VIDYAPEETH',
'Bharti Vidyapeeth': 'BHARATI VIDYAPEETH',
'Bhawanipur Global Campus': 'Bhawanipur Global Campus',
'Bhawanipur Global Campus LP': 'Bhawanipur Global Campus',
'BLOG_SCMHRD_AMC': 'SCMHRD - Symbiosis Center for Management & Human Resource Development',
'CALMU': 'California Miramar University (CMU)',
'CGC Jhanjeri': 'Chandigarh Educational Society Jhanjeri',
'CGC Landran': 'CGC,Landran - Chandigarh Group  of Colleges',
'CGC Landran': 'CGC,Landran - Chandigarh Group  of Colleges',
'Chanakya University': 'THE CHANAKYA UNIVERSITY',
'chanakya university': 'THE CHANAKYA UNIVERSITY',
'Chandigarh University': 'Chandigarh University',
'DES PU': 'DES Pune University Pune',
'DESPU LP': 'DES Pune University Pune',
'DY Patil - Navi Mumbai': 'PADMASREE DR.D.Y.PATIL UNIVERSITY',
'DYPBS': 'PADMASREE DR.D.Y.PATIL UNIVERSITY',
'DYPU': 'PADMASREE DR.D.Y.PATIL UNIVERSITY',
'Easebuzz': 'Easebuzz Private Limited',
'EDII': 'ENTREPRENEURSHIP DEVELOPMENT INSTITUTE OF INDIA',
'EDII - Performance': 'ENTREPRENEURSHIP DEVELOPMENT INSTITUTE OF INDIA',
'ELITS - SIFIL': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'Eltis': 'ELTIS - English Language Teaching Institute of Symbiosis',
'ELTIS SIFIL': 'Symbiosis Institute of Foreign and Indian Languages (SIFIL),',
'ESMOD Dubai': 'ESMOD Dubai',
'ESMOD LP': 'ESMOD Dubai',
'Finar': 'FINAR LIMITED',
'Finnacle Institute': 'FINNACLE INSTITUTE PRIVATE LIMITED',
'Finnacle Shah': 'FINNACLE INSTITUTE PRIVATE LIMITED',
'Forbes Marshall': 'FORBES MARSHALL PVT LTD',
'Galgotia University': 'Galgotias University',
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
'GRD IMT': 'GRD Institute of Management & Technology',
'HITAM': 'Hyderabad Institute of Technology and Management-HITAM',
'HITAM': 'Hyderabad Institute of Technology and Management-HITAM',
'Hyundai SM': 'HD HYUNDAI CONSTRUCTION EQUIPMENT INDIA PRIVATE LIMITED',
'IEM': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEM Kolkata - JEE': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEM MBA 25-26': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IEMJEE -B.Tech 2026': 'INSTITUTE OF ENGINEERING AND MANAGEMENT TRUST',
'IIM Kozhikode': 'INDIAN INSTITUTE OF MANAGEMENT KOZHIKODE',
'IIM Udaipur': 'Indian Institute of Management Udaipur- IIM Udaipur',
'IIT Kharagpur Law': 'IIT Kharagpur',
'IMS': 'IMS Unison University',
'IMS Noida': 'IMS NOIDA',
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
'Nirma MBA & MBA HRM': 'NIRMA',
'Nirma MBA SM': 'NIRMA',
'Nirma MBA Social Media': 'NIRMA',
'Nirma University': 'NIRMA',
'Nitte Hospital': 'Nitte Hospital',
'NITTE SM': 'NITTE University',
'Nitte University': 'NITTE University',
'Nitte University': 'NITTE University',
'NL Dalmia University': 'N. L. Dalmia Institute of Management Studies and Research',
'Orbit group': 'Orbit',
'Orbit Urban Park Commercial LP 2026': 'Orbit',
'Orbit Urban Park Residential LP 2026': 'Orbit',
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
'RV University': 'RV UNIVERSITY',
'SAI Sudha Lawn': 'Parul Sai Sudha Lawns',
'SAII': 'Symbiosis Artificial Intelligence Institute (SAII)',
'SAII': 'Symbiosis Artificial Intelligence Institute (SAII)',
'SAII Website': 'Symbiosis Artificial Intelligence Institute (SAII)',
'SBUP - DDSS': 'Sri Balaji University, Pune',
'SBUP - MBA 2025': 'Sri Balaji University, Pune',
'SBUP MBA 2025 - 26': 'Sri Balaji University, Pune',
'SBUP PhD 2025': 'Sri Balaji University, Pune',
'SBUP UG PG 2025': 'Sri Balaji University, Pune',
'SCAC': 'SCAC- Principal Symbiosis College of Arts and Commerce',
'SCAC': 'SCAC- Principal Symbiosis College of Arts and Commerce',
'SCIT': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2023-24': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2024': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT 2025': 'SCIT - Symbiosis Center  for Information Technology',
'SCIT Pune': 'SCIT - Symbiosis Center  for Information Technology',
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
'SIBM Bangalore': 'SIBM Bangalore',
'SIBM Bengaluru 2025': 'SIBM Bangalore',
'SIBM Hyd AY 2025': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'SIBM Hyderabad': 'SIBM HYD - Symbiosis Institute of Business Management - Hyd',
'SIBM Nagpur': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM Nagpur': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM Nagpur AY 2025': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM Nagpur-Mobile': 'SIBM -Symbiosis Institute of Business Management Nagpur',
'SIBM NOIDA 2026': 'SIBM Noida - Symbiosis Institute of Business Management Noida',
'SIBM Pune': 'SIBM - Symbiosis Institute of Business Management - Pune',
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
'SIT Hyd 2026': 'SIT Hyderabad- Symbiosis Institute of Technology',
'SIT Hyderabad': 'SIT Hyderabad- Symbiosis Institute of Technology',
'SIT Hyderabad': 'SIT Hyderabad- Symbiosis Institute of Technology',
'SIT Nagpur 2025': 'SIT Nagpur-Symbiosis Institute of Technology',
'SIT Nagpur 2025': 'SIT Nagpur-Symbiosis Institute of Technology',
'SIT Pune 2024': 'SIT - Symbiosis Institute of Technology Pune',
'SIT Pune 2025': 'SIT - Symbiosis Institute of Technology Pune',
'SIT Pune 2026': 'SIT - Symbiosis Institute of Technology Pune',
'SLAT': 'SLAT',
'SLAT': 'SLAT',
'SLAT 2024': 'SLAT',
'SLAT SM 2025': 'SLAT',
'SLAT Social media': 'SLAT',
'SLS Hyderabad 2025': 'SLS Hyderabad - Symbiosis Law School Hyderabad',
'SLS Hyderabad AMC': 'SLS Hyderabad - Symbiosis Law School Hyderabad',
'SLS Nagpur 2025': 'SLS Nagpur - Symbiosis Law School Nagpur',
'SLS Pune': 'SLS Pune - Symbiosis Law  School - Pune',
'SLS Pune': 'SLS Pune - Symbiosis Law  School - Pune',
'SLS Pune': 'SLS Pune - Symbiosis Law  School - Pune',
'SLS Pune': 'SLS Pune - Symbiosis Law  School - Pune',
'SNAP': 'SNAP',
'SNAP 2025': 'SNAP',
'SNAP SM 2024': 'SNAP',
'SNAP SM 2025': 'SNAP',
'Somaiya Vidyavihar University (SVU)': 'Somaiya Vidyavihar University',
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
'SSBF 2025': 'Symbiosis School of Banking and Finance (SSBF)',
'SSBS 2026': 'Symbiosis School of Biological Sciences-SSBS',
'SSE': 'Symbiosis School of Economics-SSE',
'SSE - Blog': 'Symbiosis School of Economics-SSE',
'SSE- 2024': 'Symbiosis School of Economics-SSE',
'SSI 2025': 'SSI - Symbiosis Statistical Institute',
'SSIS, Pune': 'SSIS-Symbiosis School of International Studies',
'SSL': 'SSLA - Symbiosis School of Liberal Arts',
'SSODL': 'SSODL- Symbiosis School for Open & Distance Learning',
'SSODL - PR Mandate': 'SSODL- Symbiosis School for Open & Distance Learning',
'SSSS': 'SSSS - Symbiosis School of  Sport Sciences',
'SSVAP 2026': 'SSVAP - Symbiosis School of  Visual Arts And Photography',
'Study From India': 'Study From India',
'Study from India': 'Study From India',
'Study from UAE': 'Study from UAE',
'Study from UAE': 'Study from UAE',
'Study From UAE SM': 'Study from UAE',
'Symbiosis International School': 'Symbiosis International School (SIS)',
'Symbiosis School of Biological Sciences (SSBS)': 'Symbiosis School of Biological Sciences-SSBS',
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
                print(f"❌ API error on {endpoint} page {page}: HTTP {response.status_code} → {response.text[:200]}")
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

    # --- CHANGE 1: also capture email alongside Full_Name ---
    if 'first_name' in people_df.columns and 'last_name' in people_df.columns:
        people_df['Full_Name'] = people_df.apply(
            lambda row: f"{str(row.get('first_name', '')).strip()} {str(row.get('last_name', '')).strip()}".strip(),
            axis=1
        )
        people_df['Full_Name'] = people_df['Full_Name'].replace('', pd.NA)
        people_df['Full_Name'] = people_df['Full_Name'].fillna(people_df.get('name', ''))

    if 'email' not in people_df.columns:
        people_df['email'] = ''
    
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

    # --- CHANGE 2: return email from get_employee_info ---
    def get_employee_info(creator_id):
        if pd.isna(creator_id):
            return '', '', ''
        person = people_df[people_df['id'] == creator_id]
        if not person.empty:
            full_name = person.iloc[0].get('Full_Name', '') or person.iloc[0].get('name', '')
            role = people_role_mapping.get(creator_id, '')
            email = person.iloc[0].get('email', '')
            return full_name, role, email
        return '', '', ''
    
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
    print(f"📦 Total projects fetched: {len(all_projects)}")
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
                # ADD THIS LINE HERE ↓
                print(f"  [{i+1}/{len(all_projects)}] Project {project_id} → HTTP {timesheets_response.status_code} → {len(timesheets)} timesheets")
            
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
                                    emp_email = ''  # --- CHANGE 3: initialise email ---

                                    if 'creator' in entry and isinstance(entry['creator'], dict):
                                        creator_id = entry['creator'].get('id', '')
                                        if creator_id:
                                            # --- CHANGE 4: unpack email from get_employee_info ---
                                            employee_name, emp_role, emp_email = get_employee_info(creator_id)
                                    
                                    entry['creator_id'] = creator_id
                                    entry['employee_name'] = employee_name
                                    entry['emp_role'] = emp_role
                                    entry['Email Address'] = emp_email  # --- CHANGE 5: store email in entry ---
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
        df['total_hours'] = df['total_mins'] / 60.0
        df['total_hours'] = df['total_hours'].round(2)
        
        date_cols = ['project_start_date', 'project_end_date', 'date']
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.split('T').str[0]
                df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], '')
        
        if 'status' in df.columns:
            df['status'] = df['status'].apply(lambda x: 'billable' if str(x).lower() == 'billed' else str(x))
        
        if 'client_name' not in df.columns:
            df['client_name'] = ''
        if 'project_name' in df.columns:
            df['client_name'] = df['project_name'].apply(get_client_name)

        # --- CHANGE 6: added emp_email to final columns ---
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
        print(f"📁 Local file saved: {output_filename}")
        
        return output_filename
    
    else:
        print("❌ No time entries found")
        return None

if __name__ == "__main__":
    csv_file = main()
    
    if csv_file:
        print("\n📤 UPLOADING TO GOOGLE DRIVE...")
        
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
