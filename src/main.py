import asyncio
from pathlib import Path
from preprocess import clean_csv
from crawler import TikiCrawler

if __name__ == "__main__":
    # Clean & deduplicate
    clean_csv(Path("input/product_ids.csv"), Path("input/clean_ids.csv"))

    # Run crawler
    crawler = TikiCrawler(Path("config/settings.json"))
    asyncio.run(crawler.run(Path("input/clean_ids.csv")))
