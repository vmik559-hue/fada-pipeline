# FADA ETL Pipeline

A modular, web-deployable monthly ETL pipeline for extracting vehicle retail data from FADA (Federation of Automobile Dealers Associations) press releases.

## Features

- 📥 **Automated PDF Scraping** - Extracts PDF links from FADA website
- 🔄 **Concurrent Downloading** - Parallel downloads with retry logic
- 📊 **Table Extraction** - Uses pdfplumber to extract data tables
- 📈 **Master Excel Generation** - Consolidates data by month
- 🌐 **Web Dashboard** - Premium dark-themed UI for easy access
- 📡 **Real-time Progress** - Server-Sent Events for live updates

## Project Structure

```
fada_pipeline/
├── api/
│   └── app.py           # Flask web server + dashboard
├── scraper/
│   └── fetch_pdf_links.py
├── downloader/
│   └── download_pdfs.py
├── extractor/
│   └── pdf_to_excel.py
├── transformer/
│   └── build_master_excel.py
├── filters/
│   └── date_filter.py
├── utils/
│   ├── logger.py
│   └── cache.py
├── data/
│   ├── pdfs/            # Downloaded PDFs
│   ├── excel/           # Extracted Excel files
│   └── output/          # Master Excel output
├── config.py
├── requirements.txt
├── Procfile
└── README.md
```

## Local Development

### Prerequisites

- Python 3.9+
- pip

### Installation

```bash
cd fada_pipeline
pip install -r requirements.txt
```

### Running Locally

```bash
python -m api.app
```

Open browser to `http://localhost:5000`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard UI |
| `/stream?month=1&year=2024` | GET | SSE progress stream |
| `/download?session=SESSION_ID` | GET | Download master Excel |
| `/available-months` | GET | List available months |
| `/status` | GET | Pipeline status |

## Cloud Deployment

### Render / Heroku

1. Push code to GitHub
2. Connect to Render/Heroku
3. Deploy with Procfile

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | 5000 |

## Data Categories

The pipeline extracts data for these vehicle categories:

- **2W** - Two Wheelers
- **3W** - Three Wheelers (with sub-categories)
- **PV** - Passenger Vehicles
- **CV** - Commercial Vehicles (LCV, MCV, HCV)
- **TRACTOR** - Tractors
- **TOTAL** - Overall totals

## License

Private use only.
