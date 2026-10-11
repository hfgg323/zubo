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

def load_urls():
    """从 GitHub 下载 IPTV IP 段列表，并去重"""
    import requests
    try:
        resp = requests.get(URL_FILE, timeout=5)
        resp.raise_for_status()

        # ✅ 去重核心代码
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

async def check_url(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:#===========================JSON访问时间
                if resp.status == 200:
                    return url
        except:
            return None

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

async def measure_and_validate(session, url, semaphore):
    """
    一次请求完成：
    1. 测速
    2. 过滤假 200（HTML / JSON / 广告页）
    """
    async with semaphore:
        start = time.time()
        try:
            async with session.get(
                url,
                timeout=3,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True
            ) as resp:

                # ❌ 非 200 直接淘汰
                if resp.status != 200:
                    return None

                # ✅ Content-Type 检测
                content_type = resp.headers.get("Content-Type", "").lower()

                # 黑名单类型：直接过滤
                bad_types = (
                    "text/html",
                    "application/json",
                    "application/xml",
                    "text/xml",
                )
                if any(t in content_type for t in bad_types):
                    return None

                # ✅ 白名单类型：直接通过
                stream_types = (
                    "video/",
                    "application/vnd.apple.mpegurl",
                    "application/mpegurl",
                    "application/x-mpegurl",
                    "application/octet-stream",
                    "audio/",
                    "text/plain",
                )
                is_stream = any(t in content_type for t in stream_types)

                # ⚠️ 如果 Content-Type 不明确，再读 body 兜底
                if not is_stream:
                    chunk = await resp.content.read(512)
                    text = chunk.decode("utf-8", errors="ignore")

                    # m3u8
                    if "#EXTM3U" in text:
                        is_stream = True
                    # TS sync byte
                    elif chunk[:3] == b'\x00\x00\x01':
                        is_stream = True
                    # HTML 兜底
                    elif "<html" in text.lower():
                        return None

                    if not is_stream:
                        return None

                # ✅ 计算测速（用已下载的 chunk 时间）
                speed = int((time.time() - start) * 400)
                return speed

        except Exception:
            return None

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
    semaphore = asyncio.Semaphore(500)  # ==============================================并发限制

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
        for u in valid_urls:
            print(f"  - {u}")

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

        print("🚀 开始测速并过滤假流源（单次请求）...")

validate_tasks = [
    measure_and_validate(session, url, semaphore)
    for (_, url, _) in final_results
]

speeds = await asyncio.gather(*validate_tasks)

final_results = [
    (name, url, speed)
    for (name, url, _), speed in zip(final_results, speeds)
    if speed is not None
]

print(f"✅ 真实可用流源: {len(final_results)} 条")

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
