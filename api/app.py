"""
FADA ETL Pipeline - Flask Web API
Premium dashboard for downloading FADA vehicle retail data by month/year.

Features:
- Premium dark-themed dashboard matching finarch_multiselect.py design
- Month/Year selection for targeted data extraction
- Real-time progress via Server-Sent Events
- Direct master file download
"""

import os
import sys
import time
import queue
import threading
import io
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, render_template_string, request, jsonify, Response, send_file

from config import OUTPUT_DIR, PDF_DIR, EXCEL_DIR, GOOGLE_SHEETS_CONFIG
from utils.logger import get_logger, setup_logger
from scraper.fetch_pdf_links import fetch_pdf_links, get_available_months
from downloader.download_pdfs import download_pdfs
from extractor.pdf_to_excel import process_all_pdfs
from transformer.build_master_excel import build_master_excel_for_month, build_consolidated_master
from filters.date_filter import filter_by_month_year, filter_by_date_range, find_latest_period
from utils.google_sheets_handler import GoogleSheetsHandler

app = Flask(__name__)

# Setup logging
logger = setup_logger()

# Progress queue for SSE
progress_queue = queue.Queue()

# Store active sessions
active_sessions = {}


# ============== DASHBOARD HTML TEMPLATE ==============
# UI REDESIGN: Updated with premium glassmorphism design and multi-select timeline
# All existing functionality preserved - only UI structure and styling changed
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta content="width=device-width, initial-scale=1.0" name="viewport"/>
<title>FADA Data Export Dashboard</title>
<script src="https://cdn.tailwindcss.com?plugins=forms,typography"></script>
<link href="https://fonts.googleapis.com" rel="preconnect"/>
<link crossorigin="" href="https://fonts.gstatic.com" rel="preconnect"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Playfair+Display:ital,wght@0,600;0,700;1,600&display=swap" rel="stylesheet"/>
<link href="https://fonts.googleapis.com/icon?family=Material+Icons+Round" rel="stylesheet"/>
<script>
    tailwind.config = {
        darkMode: "class",
        theme: {
            extend: {
                colors: {
                    primary: "#6366f1", 
                    accent: "#06b6d4",
                    dark: {
                        950: "#02040a",
                        900: "#0f172a",
                        800: "#1e293b",
                        700: "#334155", 
                    },
                },
                fontFamily: {
                    sans: ['Inter', 'sans-serif'],
                    display: ['Playfair Display', 'serif'],
                },
                backgroundImage: {
                    'grid-pattern': "url(\\"data:image/svg+xml,%3Csvg width='40' height='40' viewBox='0 0 40 40' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='%236366f1' fill-opacity='0.05' fill-rule='evenodd'%3E%3Cpath d='M0 40L40 0H20L0 20M40 40V20L20 40'/%3E%3C/g%3E%3C/svg%3E\\")",
                    'gradient-dark': 'linear-gradient(to bottom right, #0f172a, #1e1b4b, #000000)',
                },
                boxShadow: {
                    'glow': '0 0 20px rgba(99, 102, 241, 0.5)',
                    'glow-accent': '0 0 15px rgba(6, 182, 212, 0.4)',
                }
            },
        },
    };
</script>
<style>
    .glass-panel {
        background: rgba(30, 41, 59, 0.4);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(99, 102, 241, 0.2);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
    }
    .btn-grid-selected {
        background: rgba(6, 182, 212, 0.15);
        border-color: #06b6d4;
        color: #22d3ee;
        box-shadow: 0 0 10px rgba(6, 182, 212, 0.2);
    }
    .btn-3d {
        background: linear-gradient(135deg, #4f46e5, #06b6d4);
        box-shadow: 
            0 4px 6px rgba(0,0,0,0.3),
            inset 0 1px 0 rgba(255,255,255,0.2),
            0 0 15px rgba(6, 182, 212, 0.4);
        border: 1px solid rgba(255,255,255,0.1);
    }
    .btn-3d:hover {
        box-shadow: 
            0 6px 10px rgba(0,0,0,0.4),
            inset 0 1px 0 rgba(255,255,255,0.3),
            0 0 25px rgba(6, 182, 212, 0.6);
        transform: translateY(-1px);
    }
    .btn-3d:disabled {
        opacity: 0.6;
        cursor: not-allowed;
        transform: none;
    }
    .loading-spinner {
        display: inline-block;
        width: 20px;
        height: 20px;
        border: 3px solid rgba(255,255,255,0.3);
        border-top-color: #fff;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    @keyframes popIn { from { transform: scale(0.8); opacity: 0; } to { transform: scale(1); opacity: 1; } }
    #progressContainer { display: none; }
    #progressContainer.show { display: block; animation: fadeIn 0.4s ease; }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(-10px); } to { opacity: 1; transform: translateY(0); } }
    #downloadBtn { display: none; }
    #downloadBtn.show { display: flex; animation: popIn 0.5s cubic-bezier(0.34,1.56,0.64,1); }
</style>
</head>
<body class="bg-dark-950 text-gray-200 min-h-screen flex flex-col font-sans transition-colors duration-300 relative overflow-x-hidden selection:bg-cyan-500 selection:text-white">

<!-- Background Effects -->
<div class="fixed inset-0 z-0 pointer-events-none bg-gradient-dark"></div>
<div class="fixed inset-0 z-0 pointer-events-none bg-grid-pattern opacity-30"></div>
<div class="fixed top-0 left-0 w-full h-full z-0 pointer-events-none bg-[radial-gradient(circle_at_50%_0%,rgba(99,102,241,0.15),transparent_50%)]"></div>
<div class="fixed -top-40 -left-40 w-96 h-96 bg-indigo-500/10 rounded-full blur-[120px] pointer-events-none"></div>
<div class="fixed bottom-0 right-0 w-[500px] h-[500px] bg-cyan-500/5 rounded-full blur-[120px] pointer-events-none"></div>

<!-- Header -->
<header class="w-full max-w-7xl mx-auto p-6 z-10 flex items-center justify-between relative">
    <div class="flex items-center gap-4 group cursor-pointer">
        <div class="w-12 h-12 rounded-xl bg-gradient-to-br from-indigo-500 to-cyan-400 flex items-center justify-center shadow-lg shadow-cyan-500/20 group-hover:shadow-cyan-500/40 transition-all duration-300 transform group-hover:scale-105 border border-white/10">
            <span class="material-icons-round text-white text-2xl">directions_car</span>
        </div>
        <div>
            <h1 class="text-xl font-bold font-display tracking-wide text-white leading-tight">FADA Pipeline</h1>
            <p class="text-[10px] uppercase tracking-[0.2em] text-cyan-400 font-semibold">Vehicle Retail Intelligence</p>
        </div>
    </div>
</header>

<!-- Main Content -->
<main class="flex-grow flex flex-col items-center justify-center px-4 py-8 z-10 w-full max-w-6xl mx-auto">
    
    <!-- Status Badge -->
    <div class="mb-8 inline-flex items-center gap-2 px-4 py-1.5 rounded-full border border-cyan-500/30 bg-cyan-900/10 backdrop-blur-sm shadow-[0_0_15px_rgba(6,182,212,0.1)]">
        <span class="w-2 h-2 rounded-full bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.8)] animate-pulse"></span>
        <span class="text-xs font-semibold tracking-widest text-cyan-300 uppercase">System Operational</span>
    </div>
    
    <!-- Hero Section -->
    <div class="text-center mb-12 max-w-2xl relative">
        <h2 class="text-5xl md:text-6xl font-bold text-white mb-6 font-display tracking-tight drop-shadow-lg">
            FADA <span class="text-cyan-400 inline-block filter drop-shadow-[0_0_10px_rgba(34,211,238,0.5)]">Data</span> Dashboard
        </h2>
        <p class="text-lg text-slate-400 leading-relaxed font-light">
            Securely extract vehicle retail data streams. Configure temporal parameters to generate consolidated intelligence reports.
        </p>
    </div>
    
    <!-- Glass Panel -->
    <div class="w-full glass-panel rounded-3xl overflow-hidden relative group transition-all duration-500 hover:shadow-[0_0_40px_rgba(99,102,241,0.15)]">
        <div class="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-cyan-500/50 to-transparent opacity-70"></div>
        <div class="absolute bottom-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-indigo-500/30 to-transparent opacity-30"></div>
        
        <div class="p-8 md:p-10 space-y-10">
            
            <!-- Selection Grid -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-10">
                
                <!-- Month Selection -->
                <div class="space-y-4">
                    <div class="flex items-center justify-between">
                        <label for="monthSelect" class="text-xs font-bold text-cyan-400 uppercase tracking-widest ml-1 flex items-center gap-2">
                            <span class="material-icons-round text-sm">calendar_month</span> Select Months
                        </label>
                        <button id="selectAllMonths" onclick="toggleAllMonths()" class="text-[10px] text-cyan-400 hover:text-cyan-300 uppercase tracking-wider cursor-pointer bg-cyan-900/20 px-2 py-1 rounded border border-cyan-500/30 hover:bg-cyan-900/40 transition-all">Select All</button>
                    </div>
                    <select id="monthSelect" multiple class="w-full h-48 bg-slate-800/50 border border-slate-700 rounded-xl text-slate-300 text-sm px-3 py-2 focus:outline-none focus:border-cyan-500/50 focus:ring-1 focus:ring-cyan-500/30 transition-all cursor-pointer">
                        <option value="1">January</option>
                        <option value="2">February</option>
                        <option value="3">March</option>
                        <option value="4">April</option>
                        <option value="5">May</option>
                        <option value="6">June</option>
                        <option value="7">July</option>
                        <option value="8">August</option>
                        <option value="9">September</option>
                        <option value="10">October</option>
                        <option value="11">November</option>
                        <option value="12">December</option>
                    </select>
                    <p class="text-[10px] text-slate-500 ml-1">Hold Ctrl/Cmd to select multiple months</p>
                </div>
                
                <!-- Year Selection -->
                <div class="space-y-4">
                    <div class="flex items-center justify-between">
                        <label for="yearSelect" class="text-xs font-bold text-cyan-400 uppercase tracking-widest ml-1 flex items-center gap-2">
                            <span class="material-icons-round text-sm">history</span> Select Years
                        </label>
                        <span class="text-[10px] text-slate-500 uppercase tracking-wider">Fiscal Periods</span>
                    </div>
                    <select id="yearSelect" multiple class="w-full h-48 bg-slate-800/50 border border-slate-700 rounded-xl text-slate-300 text-sm px-3 py-2 focus:outline-none focus:border-cyan-500/50 focus:ring-1 focus:ring-cyan-500/30 transition-all cursor-pointer">
                        <option value="2026">2026</option>
                        <option value="2025">2025</option>
                        <option value="2024">2024</option>
                        <option value="2023">2023</option>
                        <option value="2022">2022</option>
                        <option value="2021">2021</option>
                        <option value="2020">2020</option>
                    </select>
                    <p class="text-[10px] text-slate-500 ml-1">Hold Ctrl/Cmd to select multiple years</p>
                </div>
            </div>
            
            <!-- Google Sheets Sync Toggle -->
            <div class="flex justify-center pt-4">
                <label class="flex items-center gap-3 cursor-pointer group">
                    <div class="relative">
                        <input type="checkbox" id="syncToSheets" checked class="sr-only peer">
                        <div class="w-11 h-6 bg-slate-700 rounded-full peer peer-checked:bg-cyan-600 transition-colors"></div>
                        <div class="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                    </div>
                    <span class="text-sm text-slate-300 group-hover:text-cyan-300 transition-colors flex items-center gap-2">
                        <span class="material-icons-round text-lg">cloud_upload</span>
                        Sync to Google Sheets
                    </span>
                </label>
            </div>
            
            <!-- Generate Button -->
            <div class="pt-6 flex justify-center">
                <button id="runBtn" onclick="runPipeline()" class="relative group overflow-hidden rounded-xl btn-3d text-white font-bold text-lg py-5 px-12 transition-all duration-300 active:scale-[0.98] w-full md:w-auto min-w-[300px]">
                    <div class="absolute inset-0 bg-white/20 translate-y-full group-hover:translate-y-0 transition-transform duration-300 ease-out skew-y-12"></div>
                    <div id="btnContent" class="relative flex items-center justify-center gap-3">
                        <span class="material-icons-round text-2xl">rocket_launch</span>
                        <span>GENERATE MASTER EXCEL</span>
                    </div>
                </button>
            </div>
            
            <!-- Status Display -->
            <div id="status" class="text-center p-4 text-slate-300 bg-slate-900/30 rounded-xl min-h-[60px] flex items-center justify-center"></div>
            
            <!-- Progress Container -->
            <div id="progressContainer" class="bg-slate-900/50 rounded-xl p-6 border border-slate-700/50">
                <div class="grid grid-cols-3 gap-4 mb-6">
                    <div class="text-center p-4 bg-slate-800/50 rounded-lg border border-slate-700/30">
                        <div class="text-2xl mb-1">📥</div>
                        <div id="downloadedCount" class="text-2xl font-bold text-cyan-400">0</div>
                        <div class="text-xs text-slate-500 uppercase tracking-wider">Downloaded</div>
                    </div>
                    <div class="text-center p-4 bg-slate-800/50 rounded-lg border border-slate-700/30">
                        <div class="text-2xl mb-1">📄</div>
                        <div id="processedCount" class="text-2xl font-bold text-cyan-400">0</div>
                        <div class="text-xs text-slate-500 uppercase tracking-wider">Processed</div>
                    </div>
                    <div class="text-center p-4 bg-slate-800/50 rounded-lg border border-slate-700/30">
                        <div class="text-2xl mb-1">⏱️</div>
                        <div id="etaCount" class="text-2xl font-bold text-cyan-400">--</div>
                        <div class="text-xs text-slate-500 uppercase tracking-wider">ETA</div>
                    </div>
                </div>
                <div class="bg-slate-800/50 h-8 rounded-full overflow-hidden shadow-inner">
                    <div id="progressBar" class="h-full bg-gradient-to-r from-indigo-500 via-cyan-500 to-emerald-500 transition-all duration-400 flex items-center justify-center text-white font-bold text-sm" style="width: 0%">0%</div>
                </div>
            </div>
            
            <!-- Download Button -->
            <div class="flex justify-center">
                <button id="downloadBtn" onclick="downloadFile()" class="items-center gap-3 py-4 px-8 bg-gradient-to-r from-emerald-600 to-cyan-600 hover:from-emerald-500 hover:to-cyan-500 text-white font-bold rounded-xl shadow-lg shadow-emerald-500/20 hover:shadow-emerald-500/40 transition-all duration-300">
                    <span class="material-icons-round">download</span>
                    <span>Download Master Excel</span>
                </button>
            </div>
            
            <!-- Info Card -->
            <div class="bg-slate-900/50 rounded-xl p-5 border border-slate-700/50 flex items-start gap-4 shadow-inner">
                <div class="flex-shrink-0 mt-0.5">
                    <span class="material-icons-round text-cyan-400">info</span>
                </div>
                <div class="text-sm text-slate-400 leading-relaxed">
                    <span class="font-medium text-slate-200">Pipeline Context:</span>
                    This system interfaces directly with <span class="text-cyan-400 font-medium">FADA data repositories</span>. Automated parsing engines extract tabular data from authorized press releases, normalizing the dataset across vehicle segments (2W, 3W, PV, CV, Tractors) into a unified analytic structure.
                </div>
            </div>
        </div>
    </div>
    
    <!-- Footer -->
    <footer class="mt-12 text-center text-sm text-slate-600 pb-6">
        <p>© 2025 FADA Data Intelligence Unit. All rights reserved.</p>
    </footer>
</main>

<script>
// ============== CENTRAL STATE MANAGEMENT ==============
// Timeline selection state - acts as single source of truth
let selectedMonths = [];
let selectedYears = [];
let eventSource = null;
let currentSessionId = null;

// ============== MONTH SELECT CHANGE LOGIC ==============
const monthSelect = document.getElementById('monthSelect');
monthSelect.addEventListener('change', function() {
    selectedMonths = Array.from(this.selectedOptions).map(opt => parseInt(opt.value));
    selectedMonths.sort((a, b) => a - b);
    updateSelectAllButtonText();
});

// ============== YEAR SELECT CHANGE LOGIC ==============
const yearSelect = document.getElementById('yearSelect');
yearSelect.addEventListener('change', function() {
    selectedYears = Array.from(this.selectedOptions).map(opt => parseInt(opt.value));
    selectedYears.sort((a, b) => b - a);
});

// ============== SET DEFAULT SELECTION (Current Month & Year) ==============
(function initializeDefaults() {
    const now = new Date();
    const currentMonth = now.getMonth() + 1;
    const currentYear = now.getFullYear();
    
    // Select current month in dropdown
    const monthOption = monthSelect.querySelector(`option[value="${currentMonth}"]`);
    if (monthOption) {
        monthOption.selected = true;
        selectedMonths.push(currentMonth);
    }
    
    // Select current year in dropdown
    const yearOption = yearSelect.querySelector(`option[value="${currentYear}"]`);
    if (yearOption) {
        yearOption.selected = true;
        selectedYears.push(currentYear);
    }
})();

// ============== SELECT ALL MONTHS TOGGLE ==============
function toggleAllMonths() {
    const btn = document.getElementById('selectAllMonths');
    const options = monthSelect.options;
    
    if (selectedMonths.length === 12) {
        // Deselect all
        for (let i = 0; i < options.length; i++) {
            options[i].selected = false;
        }
        selectedMonths.length = 0;
        btn.textContent = 'Select All';
    } else {
        // Select all
        for (let i = 0; i < options.length; i++) {
            options[i].selected = true;
        }
        selectedMonths = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        btn.textContent = 'Deselect All';
    }
}

function updateSelectAllButtonText() {
    const btn = document.getElementById('selectAllMonths');
    if (selectedMonths.length === 12) {
        btn.textContent = 'Deselect All';
    } else {
        btn.textContent = 'Select All';
    }
}

// ============== PIPELINE EXECUTION ==============
// UPDATED: Now processes ALL selected months and years
function runPipeline() {
    // Validate selection
    if (selectedMonths.length === 0 || selectedYears.length === 0) {
        document.getElementById('status').innerHTML = '⚠️ Please select at least one month and one year.';
        return;
    }
    
    // Send ALL selected months and years as comma-separated values
    const months = selectedMonths.join(',');
    const years = selectedYears.join(',');
    
    // Disable button
    const btn = document.getElementById('runBtn');
    btn.disabled = true;
    document.getElementById('btnContent').innerHTML = '<div class="loading-spinner"></div><span>Processing...</span>';
    
    // Show progress
    document.getElementById('status').innerHTML = '<div class="loading-spinner"></div> Initializing pipeline for ' + selectedMonths.length + ' month(s) × ' + selectedYears.length + ' year(s)...';
    document.getElementById('progressContainer').classList.add('show');
    document.getElementById('downloadBtn').classList.remove('show');
    
    // Reset counters
    document.getElementById('downloadedCount').textContent = '0';
    document.getElementById('processedCount').textContent = '0';
    document.getElementById('etaCount').textContent = '--';
    document.getElementById('progressBar').style.width = '0%';
    document.getElementById('progressBar').textContent = '0%';
    
    // Close existing event source
    if (eventSource) {
        eventSource.close();
    }
    
    // Start SSE connection - sends ALL months, years, and sync flag
    const syncToSheets = document.getElementById('syncToSheets').checked;
    const url = `/stream?months=${months}&years=${years}&sync=${syncToSheets}`;
    eventSource = new EventSource(url);
    
    eventSource.onmessage = function(event) {
        const parts = event.data.split('|');
        const type = parts[0];
        
        if (type === 'STATUS') {
            document.getElementById('status').innerHTML = parts[1];
        } else if (type === 'PROGRESS') {
            const downloaded = parseInt(parts[2]) || 0;
            const processed = parseInt(parts[3]) || 0;
            const total = parseInt(parts[4]) || 1;
            const eta = parts[5] || '--';
            
            document.getElementById('downloadedCount').textContent = downloaded;
            document.getElementById('processedCount').textContent = processed;
            document.getElementById('etaCount').textContent = eta;
            
            const progress = Math.round(((downloaded + processed) / (total * 2)) * 100);
            document.getElementById('progressBar').style.width = progress + '%';
            document.getElementById('progressBar').textContent = progress + '%';
        } else if (type === 'COMPLETE') {
            currentSessionId = parts[1];
            document.getElementById('status').innerHTML = '🎉 <strong>Pipeline Complete!</strong> Your master Excel file is ready.';
            document.getElementById('downloadBtn').classList.add('show');
            document.getElementById('progressBar').style.width = '100%';
            document.getElementById('progressBar').textContent = '100%';
            
            resetButton();
            eventSource.close();
        } else if (type === 'ERROR') {
            document.getElementById('status').innerHTML = '❌ Error: ' + parts[1];
            resetButton();
            eventSource.close();
        }
    };
    
    eventSource.onerror = function() {
        document.getElementById('status').innerHTML = '❌ Connection error. Please try again.';
        resetButton();
        eventSource.close();
    };
}

function resetButton() {
    const btn = document.getElementById('runBtn');
    btn.disabled = false;
    document.getElementById('btnContent').innerHTML = '<span class="material-icons-round text-2xl">rocket_launch</span><span>GENERATE MASTER EXCEL</span>';
}

// ============== DOWNLOAD FUNCTION ==============
// UNCHANGED: Uses session-based download (only filtered data)
function downloadFile() {
    if (currentSessionId) {
        window.location.href = '/download?session=' + currentSessionId;
    }
}
</script>

</body></html>'''


# ============== PIPELINE ORCHESTRATOR ==============
class PipelineRunner:
    """Orchestrates the full ETL pipeline with progress reporting.
    
    UPDATED: Now supports multiple months and years for full timeline processing.
    """
    
    def __init__(self, months: list, years: list, session_id: str, sync_to_sheets: bool = True):
        self.months = months  # List of months to process
        self.years = years    # List of years to process
        self.session_id = session_id
        self.sync_to_sheets = sync_to_sheets  # Whether to sync to Google Sheets
        self.output_file = None
    
    def run(self, progress_queue: queue.Queue):
        """Run the complete pipeline for all selected month/year combinations.
        
        UPDATED: Now fetches all data UP TO selected period and outputs only latest period.
        """
        try:
            # Step 1: Fetch PDF links
            progress_queue.put(f"STATUS|📡 Fetching PDF links from FADA website...")
            pdf_links = fetch_pdf_links()
            
            # NEW LOGIC: Determine the max selected period (end boundary)
            max_year = max(self.years)
            # Find the max month for the max year (or just use max month if single year)
            max_month = max(self.months)
            
            # Use filter_by_date_range to get ALL PDFs from beginning up to selected period
            # Using 2016 as earliest year (FADA data availability start)
            all_filtered_links = filter_by_date_range(
                pdf_links,
                start_month=1, start_year=2016,
                end_month=max_month, end_year=max_year
            )
            
            # Remove duplicates (if any)
            unique_links = list({link['url']: link for link in all_filtered_links}.values())
            
            if not unique_links:
                progress_queue.put(f"ERROR|No PDFs found up to {max_month}/{max_year}")
                return
            
            # Identify the latest available period from the fetched links
            latest_month, latest_year = find_latest_period(unique_links)
            
            if latest_month is None or latest_year is None:
                progress_queue.put(f"ERROR|Could not determine latest available period")
                return
            
            total_files = len(unique_links)
            progress_queue.put(f"STATUS|📥 Found {total_files} PDFs up to {max_month}/{max_year}. Latest available: {latest_month}/{latest_year}. Downloading...")
            
            # Step 2: Download all PDFs
            downloaded = 0
            def download_progress(completed, total, filename, success, eta):
                nonlocal downloaded
                if success:
                    downloaded = completed
                progress_queue.put(f"PROGRESS|download|{completed}|0|{total}|{eta}s")
            
            download_result = download_pdfs(unique_links, progress_callback=download_progress)
            
            progress_queue.put(f"STATUS|📄 Processing {downloaded} PDFs...")
            
            # Step 3: Extract tables from PDFs for all available months/years
            # Process all periods that were downloaded
            all_excel_files = []
            processed_periods = set()
            for link in unique_links:
                link_month = link.get('month')
                link_year = link.get('year')
                if link_month and link_year:
                    period_key = (link_month, link_year)
                    if period_key not in processed_periods:
                        processed_periods.add(period_key)
                        excel_files = process_all_pdfs(month=link_month, year=link_year)
                        all_excel_files.extend(excel_files)
            
            processed = len(all_excel_files)
            progress_queue.put(f"PROGRESS|process|{downloaded}|{processed}|{total_files}|--")
            progress_queue.put(f"STATUS|📊 Building master Excel with all data up to {latest_month}/{latest_year}...")
            
            # Step 4: Build consolidated master Excel with all data up to selected period
            self.output_file = build_consolidated_master(
                months=self.months,
                years=self.years
            )
            
            if self.output_file:
                # Store session info
                active_sessions[self.session_id] = {
                    'file': str(self.output_file),
                    'months': self.months,
                    'years': self.years,
                    'latest_period': (latest_month, latest_year),
                    'timestamp': time.time()
                }
                
                # Step 5: Sync to Google Sheets (if enabled)
                if self.sync_to_sheets and GOOGLE_SHEETS_CONFIG.get('enabled', False):
                    progress_queue.put(f"STATUS|☁️ Syncing to Google Sheets...")
                    try:
                        import pandas as pd
                        import gspread
                        from google.oauth2.service_account import Credentials
                        
                        # Read the entire Excel file as a raw matrix (no dict conversion)
                        excel_df = pd.read_excel(self.output_file, sheet_name='Master Data', header=None)
                        
                        # Convert to list of lists, replacing NaN with empty string
                        # This preserves EXACT structure of Excel
                        data_matrix = excel_df.fillna('').values.tolist()
                        
                        # Convert all values to strings for Sheets API
                        for i, row in enumerate(data_matrix):
                            data_matrix[i] = [str(val) if val != '' else '' for val in row]
                        
                        logger.info(f"Google Sheets: Syncing {len(data_matrix)} rows × {len(data_matrix[0])} columns")
                        progress_queue.put(f"STATUS|☁️ Preparing {len(data_matrix)} rows × {len(data_matrix[0])} columns...")
                        
                        # Connect to Google Sheets
                        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                        credentials = Credentials.from_service_account_file(
                            GOOGLE_SHEETS_CONFIG['credentials_file'],
                            scopes=SCOPES
                        )
                        client = gspread.authorize(credentials)
                        spreadsheet = client.open_by_key(GOOGLE_SHEETS_CONFIG['spreadsheet_id'])
                        
                        # Get or create worksheet
                        worksheet_name = GOOGLE_SHEETS_CONFIG['worksheet_name']
                        try:
                            worksheet = spreadsheet.worksheet(worksheet_name)
                            # Clear existing data
                            worksheet.clear()
                            progress_queue.put(f"STATUS|☁️ Cleared existing worksheet, writing fresh data...")
                        except gspread.WorksheetNotFound:
                            worksheet = spreadsheet.add_worksheet(
                                title=worksheet_name,
                                rows=max(len(data_matrix) + 10, 1000),
                                cols=max(len(data_matrix[0]) + 5, 100)
                            )
                            progress_queue.put(f"STATUS|☁️ Created new worksheet: {worksheet_name}")
                        
                        # Resize worksheet if needed
                        if worksheet.row_count < len(data_matrix) or worksheet.col_count < len(data_matrix[0]):
                            worksheet.resize(rows=len(data_matrix) + 10, cols=len(data_matrix[0]) + 5)
                        
                        # Write entire matrix in one batch (exact Excel replication)
                        progress_queue.put(f"STATUS|☁️ Writing {len(data_matrix)} rows to Google Sheets...")
                        worksheet.update('A1', data_matrix, value_input_option='RAW')
                        
                        # Format header row (row 2 = index 1) as bold
                        worksheet.format('2:2', {'textFormat': {'bold': True}})
                        
                        logger.info(f"Google Sheets: Successfully wrote {len(data_matrix)} rows × {len(data_matrix[0])} columns")
                        progress_queue.put(f"STATUS|✅ Google Sheets sync complete! ({len(data_matrix)} rows)")
                        
                    except Exception as sheets_error:
                        logger.error(f"Google Sheets sync error: {sheets_error}")
                        import traceback
                        logger.error(traceback.format_exc())
                        progress_queue.put(f"STATUS|⚠️ Sheets sync failed: {str(sheets_error)[:50]}")
                
                progress_queue.put(f"COMPLETE|{self.session_id}")
            else:
                progress_queue.put(f"ERROR|Failed to generate consolidated master Excel file")
                
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            progress_queue.put(f"ERROR|{str(e)}")


# ============== ROUTES ==============
@app.route('/')
def index():
    """Serve the dashboard."""
    return render_template_string(DASHBOARD_HTML)


@app.route('/stream')
def stream():
    """SSE endpoint for pipeline progress.
    
    UPDATED: Now accepts multiple months and years as comma-separated values.
    """
    # Parse comma-separated months and years
    months_str = request.args.get('months', '1')
    years_str = request.args.get('years', '2025')
    sync_str = request.args.get('sync', 'true')
    
    months = [int(m) for m in months_str.split(',') if m.strip()]
    years = [int(y) for y in years_str.split(',') if y.strip()]
    sync_to_sheets = sync_str.lower() == 'true'
    
    # Generate session ID based on all periods
    session_id = f"multi_{len(months)}m_{len(years)}y_{int(time.time())}"
    
    def generate():
        local_queue = queue.Queue()
        
        # Start pipeline in background thread with ALL months and years
        runner = PipelineRunner(months, years, session_id, sync_to_sheets)
        thread = threading.Thread(target=runner.run, args=(local_queue,))
        thread.start()
        
        # Stream progress events
        while thread.is_alive() or not local_queue.empty():
            try:
                msg = local_queue.get(timeout=0.5)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                continue
        
        # Ensure final message is sent
        try:
            while not local_queue.empty():
                msg = local_queue.get_nowait()
                yield f"data: {msg}\n\n"
        except queue.Empty:
            pass
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/download')
def download():
    """Download the generated master Excel file."""
    session_id = request.args.get('session')
    
    if not session_id or session_id not in active_sessions:
        return "No file available", 404
    
    session = active_sessions[session_id]
    file_path = Path(session['file'])
    
    if not file_path.exists():
        return "File not found", 404
    
    return send_file(
        file_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=file_path.name
    )


@app.route('/available-months')
def available_months():
    """Get list of available months from FADA website."""
    try:
        months = get_available_months()
        return jsonify({'months': months})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/status')
def status():
    """Get pipeline status."""
    return jsonify({
        'active_sessions': len(active_sessions),
        'output_dir': str(OUTPUT_DIR)
    })


# ============== MAIN ==============
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"\n>>> FADA Data Dashboard starting on http://localhost:{port}\n")
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)
