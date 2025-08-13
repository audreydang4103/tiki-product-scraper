import asyncio
import aiohttp
import json
import csv
from pathlib import Path
from bs4 import BeautifulSoup
from utils import load_config, load_checkpoint, save_checkpoint
import logging
import time

class TikiCrawler:
    def __init__(self, config_path: Path):
        self.config = load_config(config_path)
        self.api_url = self.config["api_url"]
        self.concurrency = self.config["concurrency"]
        self.timeout = aiohttp.ClientTimeout(total=self.config["timeout"])
        self.retry = self.config["retry"]
        self.backoff_seconds = self.config.get("backoff_seconds", [5, 10])
        self.batch_size = self.config["batch_size"]

        self.checkpoint_path = Path("logs/checkpoint.json")
        # Load checkpoint if exists, otherwise start fresh
        if self.checkpoint_path.exists():
            self.done_ids = load_checkpoint(self.checkpoint_path)
            print(f"📋 Resuming from checkpoint - {len(self.done_ids)} products already processed")
        else:
            self.done_ids = set()
            print("🔄 No checkpoint found - Starting fresh from beginning!")
        self.results = []
        self.error_count = {"http_error": 0, "network_error": 0, "not_found": 0, "rate_limit": 0}
        self.total_processed = 0
        self.total_success = 0
        self.batch_processed = 0

        logging.basicConfig(
            filename="logs/crawler.log",
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )

    def clean_description(self, html_text: str) -> str:
        if not html_text:
            return ""
        soup = BeautifulSoup(html_text, "lxml")
        return soup.get_text(separator=" ", strip=True)

    async def fetch_product(self, session: aiohttp.ClientSession, product_id: str):
        if product_id in self.done_ids:
            return None

        url = self.api_url.format(id=product_id)
        last_error_type = None  # Track the last error type for final counting
        
        for attempt in range(self.retry + 1):
            try:
                async with session.get(url, timeout=self.timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.total_success += 1
                        return {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": self.clean_description(data.get("description")),
                            "images": [img.get("base_url") for img in data.get("images", [])]
                        }
                    elif resp.status == 404:
                        # 404 - no need to retry
                        self.error_count["not_found"] += 1
                        return None
                    elif resp.status == 429:
                        # 429 - rate limit, retry with backoff
                        last_error_type = "rate_limit"
                        if attempt < self.retry:
                            backoff = self.backoff_seconds[min(attempt, len(self.backoff_seconds)-1)]
                            logging.warning(f"429 Too Many Requests - Attempt {attempt + 1}/{self.retry + 1} - Sleeping {backoff}s")
                            await asyncio.sleep(backoff)
                        else:
                            # When out of retry, set rate limit flag
                            self.error_count["rate_limit"] += 1
                            return None
                    else:
                        # HTTP error - retry if attemps remain
                        last_error_type = "http_error"
                        if attempt < self.retry:
                            await asyncio.sleep(1) 
                        else:
                            self.error_count["http_error"] += 1
                            return None
            except asyncio.TimeoutError:
                # Timeout - retry if attemps remain
                last_error_type = "network_error"
                if attempt < self.retry:
                    await asyncio.sleep(1)
                else:
                    self.error_count["network_error"] += 1
                    return None
            except aiohttp.ClientError:
                # Network error - - retry if attemps remain
                last_error_type = "network_error"
                if attempt < self.retry:
                    await asyncio.sleep(1)
                else:
                    self.error_count["network_error"] += 1
                    return None
        
        
        if last_error_type and last_error_type != "rate_limit":
            self.error_count[last_error_type] += 1
            
        return None

    async def process_batch(self, ids_batch: list[str], batch_num: int):
        batch_size = len(ids_batch)
        self.batch_processed = 0
        start_time = time.time()
        
        print(f"\nBatch {batch_num:03d}/200 ({batch_size} san pham)")
        
        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(self.concurrency)

            async def bound_fetch(pid):
                async with sem:
                    return await self.fetch_product(session, pid)

            tasks = [bound_fetch(pid) for pid in ids_batch]
            completed = 0
            
            for coro in asyncio.as_completed(tasks):
                result = await coro
                completed += 1
                
                # Calculate elapsed time
                elapsed_time = time.time() - start_time
                
                # Show real-time progress on same line (overwrite)
                progress = (completed / batch_size) * 100
                print(f"\r   Progress: {completed}/{batch_size} (Success: {self.total_success}, Not Found: {self.error_count['not_found']}, Network: {self.error_count['network_error']}, Rate Limit: {self.error_count['rate_limit']})", end="", flush=True)
                
                if result:
                    self.results.append(result)
                    self.done_ids.add(str(result["id"]))
        
        print()

    def save_results(self, batch_num: int, batch_size: int):
        output_file = Path(f"output/products_{batch_num:03d}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # Calculate success rate before clearing results
        total_batch_results = len(self.results) + sum(self.error_count.values())
        success_rate = (len(self.results) / total_batch_results) * 100 if total_batch_results > 0 else 0
        
        # Clear results and save checkpoint
        self.results.clear()
        save_checkpoint(self.checkpoint_path, self.done_ids)
        # Display clean summary
        success_count = len(self.results)
        print(f"Da luu {success_count} san pham vao output/products_{batch_num:03d}.json")
        print(f" Batch {batch_num:03d} xong: {success_count}/{batch_size} thanh cong")
        
        # Save detailed summary to log file
        summary = (
            f"[Batch {batch_num}] HTTP Error: {self.error_count['http_error']}, "
            f"Network Error: {self.error_count['network_error']}, "
            f"Not Found: {self.error_count['not_found']}, "
            f"Rate Limit: {self.error_count['rate_limit']}, "
            f"Success Rate: {success_rate:.2f}%"
        )
        with open("logs/error_report.txt", "a", encoding="utf-8") as ef:
            ef.write(summary + "\n")

    async def run(self, input_csv: Path):
        with open(input_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            ids = [row["id"] for row in reader]

        batch_num = 1
        for i in range(0, len(ids), self.batch_size):
            batch_ids = ids[i:i + self.batch_size]
            batch_size = len(batch_ids)
            await self.process_batch(batch_ids, batch_num)
            self.save_results(batch_num, batch_size)
            batch_num += 1
