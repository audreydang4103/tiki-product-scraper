import asyncio
import aiohttp
import json
import csv
from pathlib import Path
from bs4 import BeautifulSoup
from utils import load_config, load_checkpoint, save_checkpoint
import logging
import time
import random

class SmartRateLimiter:
    def __init__(self):
        self.request_times = []
        self.success_rate = 1.0
        self.rate_limit_count = 0
        self.last_rate_limit_time = 0
    
    async def get_delay(self):
        if self.rate_limit_count > 0:
            base_delay = 2 ** self.rate_limit_count
            return min(base_delay, 60)  # Max 60s
        return 0.1  # Min delay
    
    def record_rate_limit(self):
        self.rate_limit_count += 1
        self.last_rate_limit_time = time.time()
    
    def record_success(self):
        if self.rate_limit_count > 0:
            self.rate_limit_count = max(0, self.rate_limit_count - 1)

class RequestSpacer:
    def __init__(self, min_interval=0.2):
        self.min_interval = min_interval
        self.last_request_time = 0
    
    async def wait_if_needed(self):
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()

class UserAgentRotator:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1"
        ]
        self.current_index = 0
    
    def get_next_ua(self):
        ua = self.user_agents[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.user_agents)
        return ua

class TikiCrawler:
    def __init__(self, config_path: Path):
        self.config = load_config(config_path)
        self.api_url = self.config["api_url"]
        self.concurrency = min(self.config["concurrency"], 50)
        self.timeout = aiohttp.ClientTimeout(total=self.config["timeout"])
        self.retry = self.config["retry"]
        self.backoff_seconds = self.config.get("backoff_seconds", [5, 10])
        self.batch_size = self.config["batch_size"]

        self.rate_limiter = SmartRateLimiter()
        self.request_spacer = RequestSpacer(min_interval=0.3)  
        self.ua_rotator = UserAgentRotator()

        self.checkpoint_path = Path("logs/checkpoint.json")
        # Load checkpoint if exists, otherwise start fresh
        if self.checkpoint_path.exists():
            checkpoint_data = load_checkpoint(self.checkpoint_path)
            if isinstance(checkpoint_data, dict):
                # New checkpoint format with batch progress
                self.done_ids = set(checkpoint_data.get("done_ids", []))
                self.last_completed_batch = checkpoint_data.get("last_completed_batch", 0)
                print(f"Resuming from checkpoint - Batch {self.last_completed_batch} completed, {len(self.done_ids)} products processed")
            else:
                # Old checkpoint format (backward compatibility)
                self.done_ids = set(checkpoint_data)
                self.last_completed_batch = 0
                print(f"Resuming from old checkpoint - {len(self.done_ids)} products processed")
        else:
            self.done_ids = set()
            self.last_completed_batch = 0
            print("No checkpoint found - Starting fresh from beginning!")
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

        # Wait for rate limiting
        await self.request_spacer.wait_if_needed()

        url = self.api_url.format(id=product_id)
        last_error_type = None  # Track the last error type for final counting
        
        for attempt in range(self.retry + 1):
            try:
                # Rotate User-Agent and add more headers
                headers = {
                    "User-Agent": self.ua_rotator.get_next_ua(),
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                }
                
                async with session.get(url, timeout=self.timeout, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.total_success += 1
                        self.rate_limiter.record_success()
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
                        # 429 - rate limit, retry with smart backoff
                        last_error_type = "rate_limit"
                        self.rate_limiter.record_rate_limit()
                        
                        if attempt < self.retry:
                            #adaptive delay
                            delay = await self.rate_limiter.get_delay()
                            logging.warning(f"429 Too Many Requests - Attempt {attempt + 1}/{self.retry + 1} - Smart delay: {delay}s")
                            await asyncio.sleep(delay)
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
        
        # Reset batch-specific counters
        batch_success = 0
        batch_errors = {"http_error": 0, "network_error": 0, "not_found": 0, "rate_limit": 0}
        
        # Store initial error counts to calculate batch-specific errors
        initial_errors = self.error_count.copy()
        
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
                
                # Count results for this batch
                if result:
                    batch_success += 1
                    self.results.append(result)
                    self.done_ids.add(str(result["id"]))
                
                current_errors = self.error_count
                batch_errors["not_found"] = current_errors["not_found"] - initial_errors["not_found"]
                batch_errors["network_error"] = current_errors["network_error"] - initial_errors["network_error"]
                batch_errors["rate_limit"] = current_errors["rate_limit"] - initial_errors["rate_limit"]
                batch_errors["http_error"] = current_errors["http_error"] - initial_errors["http_error"]
                
                # Show real-time progress on same line (overwrite)
                progress = (completed / batch_size) * 100
                error_details = []
                if batch_errors["not_found"] > 0:
                    error_details.append(f"Not Found: {batch_errors['not_found']}")
                if batch_errors["network_error"] > 0:
                    error_details.append(f"Network: {batch_errors['network_error']}")
                if batch_errors["rate_limit"] > 0:
                    error_details.append(f"Rate Limit: {batch_errors['rate_limit']}")
                if batch_errors["http_error"] > 0:
                    error_details.append(f"HTTP: {batch_errors['http_error']}")
                
                error_summary = ", ".join(error_details) if error_details else "No errors"
                print(f"\r   Progress: {completed}/{batch_size} (Success: {batch_success}, {error_summary})", end="", flush=True)
        
        print()
        
        # Update global counters
        self.total_success += batch_success

    def save_results(self, batch_num: int, batch_size: int):
        output_file = Path(f"output/products_{batch_num:03d}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # Calculate success rate before clearing results
        total_batch_results = len(self.results) + sum(self.error_count.values())
        success_rate = (len(self.results) / total_batch_results) * 100 if total_batch_results > 0 else 0
        
        # Save results count BEFORE clearing
        success_count = len(self.results)
        
        # Display clean summary
        print(f"Da luu {success_count} san pham vao output/products_{batch_num:03d}.json")
        print(f"Batch {batch_num:03d} xong: {success_count}/{batch_size} thanh cong")
        
        # Clear results and save checkpoint with batch progress
        self.results.clear()
        self.last_completed_batch = batch_num
        
        # Save enhanced checkpoint with batch progress
        checkpoint_data = {
            "done_ids": list(self.done_ids),
            "last_completed_batch": self.last_completed_batch,
            "total_success": self.total_success,
            "error_count": self.error_count,
            "timestamp": time.time()
        }
        save_checkpoint(self.checkpoint_path, checkpoint_data)
        
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

        # Resume from next batch after last completed
        batch_num = self.last_completed_batch + 1
        print(f"Starting from batch {batch_num}")
        
        for i in range(0, len(ids), self.batch_size):
            current_batch_num = (i // self.batch_size) + 1
            
            # Skip completed batches
            if current_batch_num <= self.last_completed_batch:
                continue
                
            batch_ids = ids[i:i + self.batch_size]
            batch_size = len(batch_ids)
            
            print(f"\n Processing batch {current_batch_num} (resuming from checkpoint)")
            await self.process_batch(batch_ids, current_batch_num)
            self.save_results(current_batch_num, batch_size)
