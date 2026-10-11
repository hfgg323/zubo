"""
ITVlist —— 运营商分类 + 单次请求测速/校验版
=================================================
主要改动：
1) 加载基础 URL 时按 IP 归属打「电信 / 联通 / 移动 / 其他」标签，
   后续扫描出的所有频道自动继承该标签；
2) 测速 与 真实流校验(Content-Type + body 特征) 合并为【一次请求】，
   减少一半流量与耗时，同时过滤假 200(HTML/JSON/广告页)；
3) 输出 itvlist.txt 按运营商分组。
"""

import asyncio
import aiohttp
import re
import time
import requests
from urllib.parse import urljoin

from channel_mapping import CHANNEL_MAPPING, CHANNEL_CATEGORIES


URL_FILE = "https://raw.githubusercontent.com/hfgg323/zubo/main/ip_urls.txt"
RESULTS_PER_CHANNEL = 3

# ============ 运营商识别 ============

# 常用运营商 IP 段（可按你的 ip_urls.txt 来源扩充）
ISP_RANGES = {
    "电信": [
        "59.", "60.", "61.", "113.", "114.", "115.", "116.", "117.", "118.",
        "119.", "121.", "122.", "123.", "124.", "125.", "180.", "182.", "183.",
        "202.", "203.", "210.", "211.", "218.", "219.", "221.", "222.",
    ],
    "联通": [
        "60.", "61.", "113.", "114.", "115.", "116.", "117.", "118.", "119.",
        "122.", "123.", "124.", "125.", "166.", "167.", "171.", "175.", "176.",
        "177.", "178.", "180.", "181.", "182.", "210.", "211.", "218.", "219.",
        "221.", "222.",
    ],
    "移动": [
        "112.", "117.", "120.", "183.", "184.", "221.", "223.",
    ],
}


def detect_isp(ip_port):
    """根据 IP 前缀判断运营商，命中多条时按 电信>联通>移动 优先级返回。"""
    # 只取 IP 第一段
    ip = ip_port.split(":")[0]
    seg = ip.split(".")[0] + "."

    # 优先级：电信 > 联通 > 移动
    for isp in ("移动", "联通", "电信"):
        if seg in ISP_RANGES[isp]:
            return isp
    return "其他"


# ============ 加载基础 URL ============

def load_urls():
    """从 GitHub 下载 IPTV IP 段列表，去重并附带运营商标签"""
    try:
        resp = requests.get(URL_FILE, timeout=5)
        resp.raise_for_status()

        urls = []          # [(isp, url), ...]
        seen = set()
        for line in resp.text.splitlines():
            line = line.strip()
            if not line or line in seen:
                continue
            seen.add(line)
            # line 形如 "1.2.3.4:80"，取 IP 段判断运营商
            isp = detect_isp(line)
            urls.append((isp, line))

        from collections import Counter
        cnt = Counter(i for i, _ in urls)
        print(f"📡 已加载 {len(urls)} 个基础 URL（已去重）")
        print(f"   └─ 电信 {cnt['电信']} / 联通 {cnt['联通']} / 移动 {cnt['移动']} / 其他 {cnt['其他']}")
        return urls

    except Exception as e:
        print(f"❌ 下载 {URL_FILE} 失败: {e}")
        raise SystemExit(1)


# ============ 生成待扫描 URL ============

async def generate_urls(tagged_url):
    isp, url = tagged_url
    json_paths = [
        "/iptv/live/1000.json?key=txiptv",
        "/ZHGXTV/Public/json/live_interface.txt",
    ]
    ip_port, port = url.split(":")
    ip_prefix = ip_port.rsplit(".", 1)[0]
    port = f":{port}"

    modified = []
    for i in range(1, 256):
        ip = f"http://{ip_prefix}.{i}{port}"
        for path in json_paths:
            modified.append((isp, f"{ip}{path}"))
    return modified


async def check_url(session, tagged_url, semaphore):
    isp, url = tagged_url
    async with semaphore:
        try:
            async with session.get(url, timeout=1) as resp:
                if resp.status == 200:
                    return (isp, url)
        except Exception:
            pass
    return None


async def fetch_json(session, tagged_url, semaphore):
    isp, url = tagged_url
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

                # 频道继承所属运营商
                results.append((isp, name, urlx))
            return results
        except Exception:
            return []


# ============ 单次请求：测速 + 真实流校验 ============

async def measure_and_validate(session, tagged, semaphore):
    """
    一次请求同时完成：
    1) 测速
    2) 过滤假 200（HTML / JSON / 广告页）
    返回 (isp, name, url, speed)；不合法返回 None
    """
    isp, name, url = tagged
    async with semaphore:
        start = time.time()
        try:
            async with session.get(
                url,
                timeout=3,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
            ) as resp:

                if resp.status != 200:
                    return None

                content_type = resp.headers.get("Content-Type", "").lower()

                # ❌ 黑名单类型直接淘汰
                bad_types = (
                    "text/html",
                    "application/json",
                    "application/xml",
                    "text/xml",
                )
                if any(t in content_type for t in bad_types):
                    return None

                # ✅ 白名单类型直接通过
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

                # ⚠️ Content-Type 不明确时，读 body 兜底判断
                if not is_stream:
                    chunk = await resp.content.read(512)
                    text = chunk.decode("utf-8", errors="ignore")

                    if "#EXTM3U" in text:               # m3u8
                        is_stream = True
                    elif chunk[:3] == b'\x00\x00\x01':  # TS sync byte
                        is_stream = True
                    elif "<html" in text.lower():        # HTML 兜底
                        return None

                    if not is_stream:
                        return None

                speed = int((time.time() - start) * 400)
                return (isp, name, url, speed)

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


# ============ 主流程 ============

async def main():
    print("🚀 开始运行 ITVlist 脚本（运营商分类 + 单次请求版）")
    semaphore = asyncio.Semaphore(500)

    tagged_urls = load_urls()

    async with aiohttp.ClientSession() as session:

        # 1) 生成待扫描 URL
        all_tasks = [generate_urls(tu) for tu in tagged_urls]
        all_lists = await asyncio.gather(*all_tasks)
        all_urls = [u for sub in all_lists for u in sub]
        print(f"🔍 生成待扫描 URL 共: {len(all_urls)} 个")

        # 2) 检测可用 JSON API
        print("⏳ 开始检测可用 JSON API...")
        tasks = [check_url(session, u, semaphore) for u in all_urls]
        valid_urls = [r for r in await asyncio.gather(*tasks) if r]
        print(f"✅ 可用 JSON 地址: {len(valid_urls)} 个")
        for u in valid_urls:
            print(f"  - {u}")

        # 3) 抓取节目单
        print("📥 开始抓取节目单 JSON...")
        tasks = [fetch_json(session, u, semaphore) for u in valid_urls]
        fetched = await asyncio.gather(*tasks)
        results = [item for sub in fetched for item in sub]
        print(f"📺 抓到频道总数: {len(results)} 条")

        # 4) URL 特征过滤
        results = [r for r in results if is_valid_stream(r[2])]

        # 5) 单次请求：测速 + 真实流校验
        print("🚀 开始测速并过滤假流源（单次请求）...")
        validate_tasks = [
            measure_and_validate(session, r, semaphore) for r in results
        ]
        validated = [r for r in await asyncio.gather(*validate_tasks) if r]
        validated.sort(key=lambda x: x[3])
        print(f"✅ 真实可用流源: {len(validated)} 条")

        # 6) 按运营商分组，再按节目分类
        isp_order = ["电信", "联通", "移动", "其他"]
        grouped = {isp: {cat: [] for cat in CHANNEL_CATEGORIES} for isp in isp_order}

        for isp, name, url, speed in validated:
            for cat, channels in CHANNEL_CATEGORIES.items():
                if name in channels:
                    grouped[isp][cat].append((name, url, speed))
                    break

        # 7) 输出
        with open("itvlist.txt", "w", encoding="utf-8") as f:
            for isp in isp_order:
                f.write(f"== {isp} ==,#genre#\n")
                total = 0
                for cat in CHANNEL_CATEGORIES:
                    items = grouped[isp][cat]
                    if not items:
                        continue
                    f.write(f"{cat},#genre#\n")
                    for ch in CHANNEL_CATEGORIES[cat]:
                        ch_items = [x for x in items if x[0] == ch]
                        ch_items = ch_items[:RESULTS_PER_CHANNEL]
                        for item in ch_items:
                            f.write(f"{item[0]},{item[1]}\n")
                            total += 1
                print(f"📦 {isp} 写入 {total} 条频道")

        print("🎉 itvlist.txt 已生成完成！")


if __name__ == "__main__":
    asyncio.run(main())