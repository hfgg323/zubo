import asyncio
import aiohttp
import re
import datetime
import requests
import os
import time
from urllib.parse import urljoin
from channel_mapping import CHANNEL_MAPPING, CHANNEL_CATEGORIES

URL_FILE = "https://raw.githubusercontent.com/hfgg323/zubo/main/ip_urls.txt"
RESULTS_PER_CHANNEL = 3

# ========== 新增：过滤配置 ==========
ERROR_PAGE_KEYWORDS = [
    "404",
    "not found",
    "找不到页面",
    "页面不存在",
    "page not found",
    "error",
    "nginx",
    "apache",
    "whitelabel error page",
    "status=404",
    "type=not found",
    "no explicit mapping",
]

ALLOWED_CONTENT_TYPES = [
    "image/x-icon",
    "application/json",
    "application/javascript",
    "text/plain",
]
# ===============================


def load_urls():
    """从 GitHub 下载 IPTV IP 段列表，并去重"""
    try:
        resp = requests.get(URL_FILE, timeout=5)
        resp.raise_for_status()
        urls = []
        seen = set()
        for line in resp.text.splitlines():
            line = line.strip()
            if line and line not in seen:
                seen.add(line)
                urls.append(line)
        print(f"📡 已加载 {len(urls)} 个基础 URL（已去重）")
        return urls
    except Exception as e:
        print(f"❌ 下载 {URL_FILE} 失败: {e}")
        exit()


def is_error_page(text: str) -> bool:
    snippet = text[:1024].lower()
    for kw in ERROR_PAGE_KEYWORDS:
        if kw in snippet:
            return True
    return False


def is_valid_content_type(content_type: str) -> bool:
    if not content_type:
        return True
    content_type = content_type.lower()
    return any(allowed in content_type for allowed in ALLOWED_CONTENT_TYPES)


async def generate_urls(url):
    modified_urls = []
    json_paths = [
        "/iptv/live/1000.json?key=txiptv",
        "/ZHGXTV/Public/json/live_interface.txt",
    ]
    ip_port, port = url.split(":")
    ip_prefix = ip_port.rsplit(".", 1)[0]
    port = f":{port}"
    for i in range(1, 256):
        ip = f"http://{ip_prefix}.{i}{port}"
        for path in json_paths:
            modified_urls.append(f"{ip}{path}")
    return modified_urls


# ========== 修改：check_url ==========
async def check_url(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:
                if resp.status != 200:
                    return None

                content_type = resp.headers.get("Content-Type", "")
                if not is_valid_content_type(content_type):
                    return None

                text = await resp.text(errors="ignore")
                if is_error_page(text):
                    return None

                return url
        except Exception:
            return None


# ========== 修改：fetch_json ==========
async def fetch_json(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:
                if resp.status != 200:
                    return []

                content_type = resp.headers.get("Content-Type", "")
                if not is_valid_content_type(content_type):
                    return []

                text = await resp.text(errors="ignore")
                if is_error_page(text):
                    return []

                data = await resp.json(content_type=None)
                if not data:
                    return []

                results = []
                for item in data.get('data', []):
                    name = item.get('name')
                    urlx = item.get('url')
                    if not name or not urlx or ',' in urlx:
                        continue
                    if not urlx.startswith("http"):
                        urlx = urljoin(url, urlx)
                    for std_name, aliases in CHANNEL_MAPPING.items():
                        if name in aliases:
                            name = std_name
                            break
                    results.append((name, urlx))
                return results
        except Exception:
            return []


async def measure_speed(session, url, semaphore):
    async with semaphore:
        start = time.time()
        try:
            async with session.head(url, timeout=1) as resp:
                if resp.status == 200:
                    return int((time.time() - start) * 400)
                else:
                    return 999999
        except Exception:
            return 999999


def is_valid_stream(url):
    if url.startswith(("rtp://", "udp://", "rtsp://")):
        return False
    if "239." in url:
        return False
    if url.startswith(("http://16.", "http://10.", "http://192.168.")):
        return False
    if "/paiptv/" in url or "/00/SNM/" in url or "/00/CHANNEL" in url:
        return False
    valid_ext = (".m3u8", ".ts", ".flv", ".mp4", ".mkv")
    return url.startswith("http") and any(ext in url for ext in valid_ext)


async def main():
    print("🚀 开始运行 ITVlist 脚本")
    semaphore = asyncio.Semaphore(500)
    urls = load_urls()
    async with aiohttp.ClientSession() as session:
        all_urls = []
        for url in urls:
            modified_urls = await generate_urls(url)
            all_urls.extend(modified_urls)
        print(f"🔍 生成待扫描 URL 共: {len(all_urls)} 个")

        print("⏳ 开始检测可用 JSON API...")
        tasks = [check_url(session, u, semaphore) for u in all_urls]
        valid_urls = [r for r in await asyncio.gather(*tasks) if r]
        print(f"✅ 可用 JSON 地址: {len(valid_urls)} 个")

        print("📥 开始抓取节目单 JSON...")
        tasks = [fetch_json(session, u, semaphore) for u in valid_urls]
        results = []
        fetched = await asyncio.gather(*tasks)
        for sublist in fetched:
            results.extend(sublist)
        print(f"📺 抓到频道总数: {len(results)} 条")

        final_results = [(name, url, 0) for name, url in results]
        final_results = [
            (name, url, speed)
            for name, url, speed in final_results
            if is_valid_stream(url)
        ]

        print("🚀 开始测速频道源...")
        speed_tasks = [measure_speed(session, url, semaphore) for (_, url, _) in final_results]
        speeds = await asyncio.gather(*speed_tasks)
        final_results = [
            (name, url, speed)
            for (name, url, _), speed in zip(final_results, speeds)
        ]
        final_results.sort(key=lambda x: x[2])

        itv_dict = {cat: [] for cat in CHANNEL_CATEGORIES}
        for name, url, speed in final_results:
            for cat, channels in CHANNEL_CATEGORIES.items():
                if name in channels:
                    itv_dict[cat].append((name, url, speed))
                    break

        for cat in CHANNEL_CATEGORIES:
            print(f"📦 分类《{cat}》找到 {len(itv_dict[cat])} 条频道")

        with open("itvlist.txt", 'w', encoding='utf-8') as f:
            for cat in CHANNEL_CATEGORIES:
                f.write(f"{cat},#genre#\n")
                for ch in CHANNEL_CATEGORIES[cat]:
                    ch_items = [x for x in itv_dict[cat] if x[0] == ch]
                    ch_items = ch_items[:RESULTS_PER_CHANNEL]
                    for item in ch_items:
                        f.write(f"{item[0]},{item[1]}\n")

        print("🎉 itvlist.txt 已生成完成！")


if __name__ == "__main__":
    asyncio.run(main())
