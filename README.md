# Tiki Product Scraper

AN asynchronous web scraper for extracting product information from Tiki.vn. Built with Python and aiohttp for efficient concurrent data collection.

## Features

- **Asynchronous Processing**: High-performance scraping using aiohttp and asyncio
- **Batch Processing**: Configurable batch sizes for optimal memory management
- **Error Handling**: Comprehensive error handling for HTTP, network, and rate limiting issues
- **Progress Tracking**: Real-time progress monitoring with detailed statistics
- **Checkpoint System**: Resume scraping from where you left off
- **Comprehensive Logging**: Detailed error reporting and logging
- **Test Suite**: Full test coverage for all components

## Requirements

- Python 3.8+
- aiohttp
- asyncio
- pandas (for data preprocessing)

##  Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd tiki-product-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

##  Configuration

Edit `config/settings.json` to customize scraping parameters:

```json
{
    "batch_size": 50,
    "concurrency": 10,
    "retry_attempts": 3,
    "timeout": 30
}
```

## Project Structure

```
tiki-product-scraper/
├── config/          # Configuration files
├── input/           # Input CSV files with product IDs
├── logs/            # Log files and error reports
├── output/          # Scraped product data (JSON files)
├── src/             # Source code
│   ├── crawler.py   # Main scraping logic
│   ├── main.py      # Entry point
│   ├── preprocess.py # Data preprocessing utilities
│   └── utils.py     # Helper functions
└── tests/           # Test suite
```

## Usage

1. Prepare your input file:
   - Create a CSV file in the `input/` directory
   - Include a column named "id" with Tiki product IDs

2. Run the scraper:
```bash
python src/main.py
```

## Output Format

Each batch generates a JSON file with the following structure:

```json
[
  {
    "id": "product_id",
    "name": "Product Name",
    "price": "Price",
    "url": "Product URL",
    "category": "Product Category",
    "rating": "Average Rating",
    "review_count": "Number of Reviews"
  }
]
```

## Logging

The scraper provides comprehensive logging:

- **Console Output**: Real-time progress and statistics
- **Error Reports**: Detailed error information in `logs/error_report.txt`
- **Checkpoint Files**: Resume capability with `logs/checkpoint.json`
