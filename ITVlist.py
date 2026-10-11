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

# ==================== HTML/XML 检测缓存 ====================
_content_type_cache = {}

HTML_XML_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "application/xml",
    "text/xml",
)

# ==================== URL 加载 ====================
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

# ==================== URL 生成 ====================
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

# ==================== JSON API 检测 ====================
async def check_url(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:
                if resp.status == 200:
                    return url
        except:
            return None

# ==================== 抓取节目 JSON ====================
async def fetch_json(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:
                data = await resp.json()
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
        except:
            return []

# ==================== 测速 ====================
async def measure_speed(session, url, semaphore):
    async with semaphore:
        start = time.time()
        try:
            async with session.head(url, timeout=1) as resp:
                if resp.status == 200:
                    return int((time.time() - start) * 400)
                else:
                    return 999999
        except:
            return 999999

# ==================== 流地址合法性校验 ====================
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

# ==================== HTML/XML 检测（HEAD + GET fallback） ====================
async def is_html_or_xml(session, url, semaphore):
    async with semaphore:
        if url in _content_type_cache:
            return _content_type_cache[url]

        # ---- 1. HEAD 检测 ----
        try:
            async with session.head(url, timeout=2, allow_redirects=True) as resp:
                content_type = resp.headers.get("Content-Type", "").lower()
                if content_type:
                    is_bad = any(t in content_type for t in HTML_XML_TYPES)
                    _content_type_cache[url] = is_bad
                    return is_bad
        except Exception:
            pass

        # ---- 2. GET fallback（只读前 512 字节） ----
        try:
            async with session.get(
                url,
                timeout=2,
                headers={"Range": "bytes=0-511"}
            ) as resp:
                content_type = resp.headers.get("Content-Type", "").lower()
                if any(t in content_type for t in HTML_XML_TYPES):
                    _content_type_cache[url] = True
                    return True

                # 兜底：读内容判断
                text = await resp.text(errors="ignore")
                if text.lstrip().startswith(("<!doctype", "<html", "<?xml")):
                    _content_type_cache[url] = True
                    return True
        except Exception:
            pass

        _content_type_cache[url] = False
        return False

# ==================== 主流程 ====================
async def main():
    print("🚀 开始运行 ITVlist 脚本")
    semaphore = asyncio.Semaphore(500)
    urls = load_urls()

    async with aiohttp.ClientSession() as session:
        # ---- 生成扫描 URL ----
        all_urls = []
        for url in urls:
            modified_urls = await generate_urls(url)
            all_urls.extend(modified_urls)
        print(f"🔍 生成待扫描 URL 共: {len(all_urls)} 个")

        # ---- 检测可用 JSON API ----
        print("⏳ 开始检测可用 JSON API...")
        tasks = [check_url(session, u, semaphore) for u in all_urls]
        valid_urls = [r for r in await asyncio.gather(*tasks) if r]
        print(f"✅ 可用 JSON 地址: {len(valid_urls)} 个")
        for u in valid_urls:
            print(f"  - {u}")

        # ---- 抓取节目单 ----
        print("📥 开始抓取节目单 JSON...")
        tasks = [fetch_json(session, u, semaphore) for u in valid_urls]
        results = []
        fetched = await asyncio.gather(*tasks)
        for sublist in fetched:
            results.extend(sublist)
        print(f"📺 抓到频道总数: {len(results)} 条")

        # ---- 初步过滤 ----
        final_results = [(name, url, 0) for name, url in results]
        final_results = [
            (name, url, speed)
            for name, url, speed in final_results
            if is_valid_stream(url)
        ]

        # ---- 🧪 HTML/XML 检测（HEAD + GET fallback） ----
        print("🧪 开始检测 HTML / XML 页面...")
        html_check_tasks = [
            is_html_or_xml(session, url, semaphore)
            for _, url, _ in final_results
        ]
        html_results = await asyncio.gather(*html_check_tasks)

        before_filter = len(final_results)
        final_results = [
            (name, url, speed)
            for (name, url, speed), is_html in zip(final_results, html_results)
            if not is_html
        ]
        filtered_count = before_filter - len(final_results)
        print(f"🧹 过滤 HTML/XML 页面: {filtered_count} 个")
        print(f"✅ 过滤后剩余频道: {len(final_results)} 条")

        # ---- 测速 ----
        print("🚀 开始测速频道源...")
        speed_tasks = [measure_speed(session, url, semaphore) for (_, url, _) in final_results]
        speeds = await asyncio.gather(*speed_tasks)
        final_results = [
            (name, url, speed)
            for (name, url, _), speed in zip(final_results, speeds)
        ]
        final_results.sort(key=lambda x: x[2])

        # ---- 分类整理 ----
        itv_dict = {cat: [] for cat in CHANNEL_CATEGORIES}
        for name, url, speed in final_results:
            for cat, channels in CHANNEL_CATEGORIES.items():
                if name in channels:
                    itv_dict[cat].append((name, url, speed))
                    break

        for cat in CHANNEL_CATEGORIES:
            print(f"📦 分类《{cat}》找到 {len(itv_dict[cat])} 条频道")

        # ---- 写入文件 ----
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
