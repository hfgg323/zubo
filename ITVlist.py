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
# ======================
# 运营商过滤配置
# =PLACEHOLDER_1="移动"  # 可选: "移动" / "电信" / "联通" / None(不过滤)
# ======================
FILTER_ISP = "移动"


def get_isp_from_ip(ip: str) -> str:
    """
    根据 IP 前缀判断运营商（精简版，适合 IPTV 扫描场景）
    """
    # 去掉端口号
    ip_only = ip.split(":")[0]

    # ========== 中国移动 ==========
    if (ip_only.startswith("100.") or   # CGN 共享地址（移动极多）
        ip_only.startswith(("36.", "39.", "42.","110.", "111.", "112.",
                            "114.","116.","117.", "119.","120.", "125.","183.", "211.",
                            "218.", "221.", "223."))):
        return "移动"

    # ========== 中国电信 ==========
    if ip_only.startswith(("58.", "59.", "60.", "202.", "203.",
                           "210.", "220.", "222.")):
        return "电信"

    # ========== 中国联通 ==========
    if ip_only.startswith(("60.", "61.", "113.", "123.",
                           "139.", "171.", "172.", "176.")):
        return "联通"

    return "未知"

def load_urls():
    """
    从 GitHub 下载 IPTV IP 段列表
    去重 + 按运营商过滤（可配置）
    """
    import requests
    try:
        resp = requests.get(URL_FILE, timeout=5)
        resp.raise_for_status()

        urls = []
        seen = set()

        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue

            # ===== 去重 =====
            if line in seen:
                continue
            seen.add(line)

            # ===== 运营商过滤 =====
            if FILTER_ISP is not None:
                ip = line.split(":")[0]
                isp = get_isp_from_ip(ip)
                if isp != FILTER_ISP:
                    continue

            urls.append(line)

        isp_label = FILTER_ISP if FILTER_ISP else "不过滤"
        print(f"📡 已加载 {len(urls)} 个基础 URL（去重完成，运营商={isp_label}）")
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

async def measure_speed(session, url, semaphore):
    async with semaphore:
        start = time.time()
        try:
            async with session.head(url, timeout=3) as resp:  # =======================频道测速用时
                if resp.status == 200:
                    return int((time.time() - start) * 700)
                else:
                    return 999999
        except:
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
