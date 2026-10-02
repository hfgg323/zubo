"""
ITVlist - 诊断版（逐层计数 + 失败原因分类）
==========================================
跑一次，看最后 30 行输出，就能定位断点
"""
import asyncio
import aiohttp
import re
import time
from urllib.parse import urljoin
import requests

# ==================== 配置 ====================
URL_FILE = "https://raw.githubusercontent.com/hfgg323/zubo/main/ip_urls.txt"
RESULTS_PER_CHANNEL = 5
MAX_BYTES = 524288
MIN_BYTES = 32768
CONCURRENCY = 80
JSON_TIMEOUT = 3
SPEED_TIMEOUT = 6
CHECK_TIMEOUT = 2

# m3u8 探活旋钮
M3U8_PROBE_TS_COUNT = 3
M3U8_PROBE_TIMEOUT = 4
M3U8_PROBE_MIN_BYTES = 1024
M3U8_REQUIRE_RATIO = (2, 3)

# ==================== 频道映射（精简版，按需补全）====================
CHANNEL_MAPPING = {
    "CCTV1": ["CCTV-1", "CCTV1", "央视一套", "cctv1", "CCTV-1综合"],
    "CCTV2": ["CCTV-2", "CCTV2", "央视二套", "cctv2"],
    "CCTV3": ["CCTV-3", "CCTV3", "央视三套", "cctv3"],
    "CCTV4": ["CCTV-4", "CCTV4", "央视四套", "cctv4"],
    "CCTV5": ["CCTV-5", "CCTV5", "央视五套", "cctv5"],
    "CCTV5+": ["CCTV-5+", "CCTV5+", "cctv5+"],
    "CCTV6": ["CCTV-6", "CCTV6", "央视六套", "cctv6"],
    "CCTV7": ["CCTV-7", "CCTV7", "央视七套", "cctv7"],
    "CCTV8": ["CCTV-8", "CCTV8", "央视八套", "cctv8"],
    "CCTV9": ["CCTV-9", "CCTV9", "央视九套", "cctv9"],
    "CCTV10": ["CCTV-10", "CCTV10", "央视十套", "cctv10"],
    "CCTV11": ["CCTV-11", "CCTV11", "cctv11"],
    "CCTV12": ["CCTV-12", "CCTV12", "cctv12"],
    "CCTV13": ["CCTV-13", "CCTV13", "新闻", "cctv13"],
    "CCTV14": ["CCTV-14", "CCTV14", "少儿", "cctv14"],
    "CCTV15": ["CCTV-15", "CCTV15", "音乐", "cctv15"],
    "CCTV16": ["CCTV-16", "CCTV16", "cctv16"],
    "湖南卫视": ["湖南", "HunanTV", "hunan", "芒果"],
    "浙江卫视": ["浙江", "ZhejiangTV", "zjtv"],
    "江苏卫视": ["江苏", "JSBC", "jstv"],
    "东方卫视": ["东方", "DragonTV", "dongfang"],
    "北京卫视": ["北京", "BTV", "btv"],
    "广东卫视": ["广东", "GDTV", "gdtv"],
    "深圳卫视": ["深圳", "SZTV", "sztv"],
    "四川卫视": ["四川", "SCTV", "sctv"],
    "湖北卫视": ["湖北", "HBTV", "hbtv"],
    "辽宁卫视": ["辽宁", "LNTV", "lntv"],
    "山东卫视": ["山东", "SDTV", "sdtv"],
    "安徽卫视": ["安徽", "AHTV", "ahtv"],
    "重庆卫视": ["重庆", "CQTV", "cqtv"],
    "天津卫视": ["天津", "TJTV", "tjtv"],
    "黑龙江卫视": ["黑龙江", "HLJTV", "hljtv"],
    "江西卫视": ["江西", "JXTV", "jxtv"],
    "河南卫视": ["河南", "HNTV", "hntv"],
    "贵州卫视": ["贵州", "GZTV", "gztv"],
    "云南卫视": ["云南", "YNRTV", "ynrtv"],
    "广西卫视": ["广西", "GXRTV", "gxrtv"],
    "甘肃卫视": ["甘肃", "GSTV", "gstv"],
    "吉林卫视": ["吉林", "JLRTV", "jlrtv"],
    "山西卫视": ["山西", "SXRTV", "sxrtv"],
    "内蒙古卫视": ["内蒙古", "NMGTV", "nmg"],
    "新疆卫视": ["新疆", "XJTV", "xjtv"],
    "西藏卫视": ["西藏", "XZTV", "xztv"],
    "青海卫视": ["青海", "QHTV", "qhtv"],
    "宁夏卫视": ["宁夏", "NXTV", "nxtv"],
    "海南卫视": ["海南", "HNTV", "hainan"],
    "东南卫视": ["东南", "SETV", "setv"],
    "海峡卫视": ["海峡", "HXTV", "hxtv"],
    "厦门卫视": ["厦门", "XMTV", "xmtv"],
    "大湾区卫视": ["大湾区", "DWQ", "dwq"],
    "凤凰中文": ["凤凰中文", "Phoenix Chinese", "phoenix"],
    "凤凰资讯": ["凤凰资讯", "Phoenix Info", "phoenix info"],
    "TVBS": ["TVBS", "tvbs"],
    "台视": ["台视", "TTV", "ttv"],
    "中视": ["中视", "CTV", "ctv"],
    "华视": ["华视", "CTS", "cts"],
    "民视": ["民视", "FTV", "ftv"],
    "公视": ["公视", "PTS", "pts"],
    "BBC World": ["BBC", "bbc"],
    "CNN": ["CNN", "cnn"],
    "NHK World": ["NHK", "nhk"],
    "Discovery": ["Discovery", "discovery"],
    "HBO": ["HBO", "hbo"],
    "Cartoon Network": ["CN", "Cartoon", "cartoon"],
    "Nickelodeon": ["Nick", "nickelodeon"],
    "Disney Channel": ["Disney", "disney"],
    "ESPN": ["ESPN", "espn"],
    "Fox Sports": ["Fox Sports", "fox sports"],
    "Star Sports": ["Star Sports", "star sports"],
    "beIN Sports": ["beIN", "bein"],
    "其他": ["其他", "Other", "other"],
}

CHANNEL_CATEGORIES = {
    "央视": ["CCTV1","CCTV2","CCTV3","CCTV4","CCTV5","CCTV5+","CCTV6","CCTV7","CCTV8","CCTV9","CCTV10","CCTV11","CCTV12","CCTV13","CCTV14","CCTV15","CCTV16"],
    "卫视": ["湖南卫视","浙江卫视","江苏卫视","东方卫视","北京卫视","广东卫视","深圳卫视","四川卫视","湖北卫视","辽宁卫视","山东卫视","安徽卫视","重庆卫视","天津卫视","黑龙江卫视","江西卫视","河南卫视","贵州卫视","云南卫视","广西卫视","甘肃卫视","吉林卫视","山西卫视","内蒙古卫视","新疆卫视","西藏卫视","青海卫视","宁夏卫视","海南卫视","东南卫视","海峡卫视","厦门卫视","大湾区卫视"],
    "港澳台": ["凤凰中文","凤凰资讯","TVBS","台视","中视","华视","民视","公视"],
    "国际": ["BBC World","CNN","NHK World","Discovery","HBO","Cartoon Network","Nickelodeon","Disney Channel","ESPN","Fox Sports","Star Sports","beIN Sports"],
    "其他": ["其他"],
}

# ==================== 基础函数 ====================
def load_urls():
    try:
        resp = requests.get(URL_FILE, timeout=10)
        resp.raise_for_status()
        urls = [line.strip() for line in resp.text.splitlines() if line.strip()]
        print(f"📡 已加载 {len(urls)} 个基础 URL")
        return urls
    except Exception as e:
        print(f"❌ 下载 {URL_FILE} 失败: {e}")
        raise SystemExit(1)

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

def _url_fingerprint(url):
    """去掉临时参数做去重键"""
    clean = url.split("?")[0].split("#")[0]
    return clean.rstrip("/")

def is_valid_stream(url):
    """过滤明显不可播的 URL"""
    if url.startswith(("rtp://", "udp://", "rtsp://")):
        return False
    if "239." in url:
        return False
    if url.startswith(("http://16.", "http://10.", "http://192.168.", "http://100.64.", "http://172.")):
        return False
    if "/paiptv/" in url or "/00/SNM/" in url or "/00/CHANNEL" in url:
        return False
    valid_ext = (".m3u8", ".ts", ".flv", ".mp4", ".mkv")
    return url.startswith("http") and any(ext in url for ext in valid_ext)

def speed_grade(score):
    if score < 300:
        return "A"
    if score < 800:
        return "B"
    if score < 2000:
        return "C"
    return "D"

# ==================== 核心流程 ====================
async def main():
    t0 = time.time()
    print("🚀 开始运行 ITVlist 诊断版")
    print("=" * 60)

    semaphore = asyncio.Semaphore(CONCURRENCY)
    urls = load_urls()

    # 1) 生成候选 JSON API
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False),
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:

        print("\n[阶段1] 生成候选 JSON API 地址...")
        all_urls = []
        for url in urls:
            modified_urls = await generate_urls(url)
            all_urls.extend(modified_urls)
        print(f"  🔍 生成待扫描 URL 共: {len(all_urls)} 个")

        # 2) 快速筛选可用 JSON API
        print("\n[阶段2] 快速检测可用 JSON API...")
        async def check_url(url):
            async with semaphore:
                try:
                    async with session.get(url, timeout=CHECK_TIMEOUT) as resp:
                        if resp.status == 200:
                            return url
                        return None
                except Exception:
                    return None

        tasks = [check_url(u) for u in all_urls]
        valid_urls = [r for r in await asyncio.gather(*tasks) if r]
        print(f"  ✅ 可用 JSON 地址: {len(valid_urls)} 个")
        if valid_urls:
            for u in valid_urls[:5]:
                print(f"    - {u}")
            if len(valid_urls) > 5:
                print(f"    ... 还有 {len(valid_urls)-5} 个")

        if not valid_urls:
            print("  ❌ 没有可用 JSON API！检查 IP 段或网络")
            return

        # 3) 抓取节目单
        print("\n[阶段3] 抓取节目单 JSON...")
        async def fetch_json(url):
            async with semaphore:
                try:
                    async with session.get(url, timeout=JSON_TIMEOUT) as resp:
                        data = await resp.json(content_type=None)
                    results = []
                    for item in data.get("data", []):
                        name = item.get("name")
                        urlx = item.get("url")
                        if not name or not urlx or "," in urlx:
                            continue
                        if not urlx.startswith("http"):
                            urlx = urljoin(url, urlx)
                        # 标准化频道名
                        for std_name, aliases in CHANNEL_MAPPING.items():
                            if name in aliases:
                                name = std_name
                                break
                        results.append((name, urlx))
                    return results
                except Exception:
                    return []

        tasks = [fetch_json(u) for u in valid_urls]
        results = []
        for sublist in await asyncio.gather(*tasks):
            results.extend(sublist)
        print(f"  📺 抓到频道总数: {len(results)} 条")

        if len(results) == 0:
            print("  ❌ 节目单为空！所有 JSON API 返回的数据里没有有效频道")
            return

        # 4) 去重 + 合法流过滤
        print("\n[阶段4] 去重 + 合法流过滤...")
        seen = set()
        deduped_results = []
        filter_reasons = {"invalid_stream": 0, "dup": 0}
        for name, url in results:
            if not is_valid_stream(url):
                filter_reasons["invalid_stream"] += 1
                continue
            if (name, url) in seen:
                filter_reasons["dup"] += 1
                continue
            seen.add((name, url))
            deduped_results.append((name, url))

        print(f"  🧹 去重后: {len(deduped_results)} 条")
        print(f"  📊 过滤统计: 非法流={filter_reasons['invalid_stream']}, 重复={filter_reasons['dup']}")

        if len(deduped_results) == 0:
            print("  ❌ 去重后为空！所有源都被 is_valid_stream 过滤了")
            # 采样看看被过滤的 URL 长什么样
            print("  🔎 采样被过滤的 URL（前5个）:")
            count = 0
            for name, url in results:
                if not is_valid_stream(url):
                    print(f"    {name}: {url}")
                    count += 1
                    if count >= 5:
                        break
            return

        # 5) 真实播放测速
        print(f"\n[阶段5] 真实播放测速（{len(deduped_results)} 个源）...")
        print(f"  ⚙️  m3u8 探活: 抽 {M3U8_PROBE_TS_COUNT} 个 ts, 超时 {M3U8_PROBE_TIMEOUT}s, 门槛 {M3U8_REQUIRE_RATIO[0]}/{M3U8_REQUIRE_RATIO[1]}")

        async def measure_speed(url):
            is_m3u8 = ".m3u8" in url.lower()
            async with semaphore:
                start = time.time()
                try:
                    if is_m3u8:
                        # m3u8: 拉索引
                        headers = {"User-Agent": "VLC/3.0.20 LibVLC/3.0.20"}
                        async with session.get(url, headers=headers, timeout=SPEED_TIMEOUT) as resp:
                            if resp.status not in (200, 206):
                                return 999999
                            text = await resp.text(errors="ignore")
                            if "#EXTM3U" not in text:
                                return 999999
                            index_elapsed = time.time() - start

                        # 解析 ts
                        ts_urls = []
                        for line in text.splitlines():
                            line = line.strip()
                            if line and not line.startswith("#"):
                                if line.startswith("http"):
                                    ts_urls.append(line)
                                else:
                                    base = url.rsplit("/", 1)[0] + "/"
                                    ts_urls.append(urljoin(base, line))

                        if not ts_urls:
                            return 999999

                        probe_urls = ts_urls[:M3U8_PROBE_TS_COUNT]

                        async def probe_ts(ts_url):
                            t0 = time.time()
                            try:
                                async with session.get(
                                    ts_url,
                                    headers={"Range": "bytes=0-4097", "User-Agent": "VLC/3.0.20 LibVLC/3.0.20"},
                                    timeout=M3U8_PROBE_TIMEOUT
                                ) as r:
                                    if r.status in (200, 206):
                                        chunk = await r.read()
                                        return (len(chunk) >= M3U8_PROBE_MIN_BYTES, time.time() - t0)
                                    return (False, time.time() - t0)
                            except Exception:
                                return (False, time.time() - t0)

                        probe_results = await asyncio.gather(
                            *(probe_ts(u) for u in probe_urls),
                            return_exceptions=True
                        )

                        reachable = 0
                        probe_time = 0
                        for r in probe_results:
                            if isinstance(r, tuple):
                                ok, t = r
                                if ok:
                                    reachable += 1
                                probe_time = max(probe_time, t)

                        if reachable < M3U8_REBE_RATIO[0]:
                            return 999999

                        penalty = {3: 0, 2: 300, 1: 1500}
                        score = int(index_elapsed * 1000) + int(probe_time * 1000) + penalty.get(reachable, 2000)
                        return score

                    else:
                        # ts/flv/mp4
                        headers = {
                            "Range": f"bytes=0-{MAX_BYTES-1}",
                            "User-Agent": "VLC/3.0.20 LibVLC/3.0.20"
                        }
                        async with session.get(url, headers=headers, timeout=SPEED_TIMEOUT) as resp:
                            if resp.status not in (200, 206):
                                return 999999

                            total_read = 0
                            async for chunk in resp.content.iter_chunked(8192):
                                total_read += len(chunk)
                                if total_read >= MAX_BYTES:
                                    break

                            if total_read < MIN_BYTES:
                                return 999999

                            elapsed = time.time() - start
                            speed_kbps = (total_read / 1024) / max(elapsed, 0.001)
                            score = elapsed * 1000 + max(0, 400 - speed_kbps) * 3
                            return int(score)

                except Exception:
                    return 999999

        speed_tasks = [measure_speed(url) for (_, url) in deduped_results]
        speeds = await asyncio.gather(*speed_tasks)

        final_results = [
            (name, url, speed)
            for (name, url), speed in zip(deduped_results, speeds)
        ]

        # 统计
        alive = [(n, u, s) for n, u, s in final_results if s < 999999]
        dead = len(final_results) - len(alive)
        print(f"  📊 测速结果: 存活={len(alive)}, 死亡={dead}")

        if len(alive) == 0:
            print("  ❌ 全部源测速失败！")
            # 采样看看被淘汰的 URL
            print("  🔎 采样被淘汰的 URL（前5个）:")
            count = 0
            for name, url, speed in final_results:
                if score >= 999999:
                    print(f"    {name}: {url}")
                    count += 1
                    if count >= 5:
                        break
            return

        # 6) 全局 URL 去重
        print("\n[阶段6] 全局 URL 去重...")
        seen_urls = {}
        for name, url, speed in alive:
            fp = _url_fingerprint(url)
            if fp not in seen_urls or speed < seen_urls[fp][1]:
                seen_urls[fp] = (name, speed, url)

        global_deduped = [(name, url, speed) for (url, (name, speed, url)) in seen_urls.items()]
        global_deduped.sort(key=lambda x: x[2])
        print(f"  🌍 全局去重后: {len(global_deduped)} 条")

        if len(global_deduped) == 0:
            print("  ❌ 全局去重后为 0！")
            return

        # 7) 按分类归档
        print("\n[阶段7] 按分类归档...")
        itv_dict = {cat: [] for cat in CHANNEL_CATEGORIES}
        other = []
        for name, url, speed in global_deduped:
            placed = False
            for cat, channels in CHANNEL_CATEGORIES.items():
                if cat == "其他":
                    continue
                if name in channels:
                    itv_dict[cat].append((name, url, speed))
                    placed = True
                    break
            if not placed:
                other.append((name, url, speed))
        itv_dict["其他"] = other

        for cat in CHANNEL_CATEGORIES:
            print(f"  📦 分类《{cat}》找到 {len(itv_dict[cat])} 条频道")

        # 8) 写入 TXT
        print("\n[阶段8] 写入文件...")
        txt_path = "itvlist.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            for cat in CHANNEL_CATEGORIES:
                if not itv_dict[cat]:
                    continue
                f.write(f"{cat},#genre#\n")
                for ch in CHANNEL_CATEGORIES[cat]:
                    ch_items = list({x[1]: x for x in itv_dict[cat] if x[0] == ch}.values())
                    ch_items.sort(key=lambda x: x[2])
                    ch_items = ch_items[:RESULTS_PER_CHANNEL]
                    for item in ch_items:
                        grade = speed_grade(item[2])
                        f.write(f"{item[0]},{item[1]}  # {grade}\n")
                    if ch_items:
                        f.write("\n")

        # 9) 写入 M3U
        m3u_path = "itvlist.m3u"
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for cat in CHANNEL_CATEGORIES:
                if not itv_dict[cat]:
                    continue
                f.write(f"#EXTGRP:{cat}\n")
                for ch in CHANNEL_CATEGORIES[cat]:
                    ch_items = list({x[1]: x for x in itv_dict[cat] if x[0] == ch}.values())
                    ch_items.sort(key=lambda x: x[2])
                    ch_items = ch_items[:RESULTS_PER_CHANNEL]
                    for item in ch_items:
                        grade = speed_grade(item[2])
                        f.write(f'#EXTINF:-1 group-title="{cat}" tvg-name="{item[0]}" rating="{grade}",{item[0]}\n')
                        f.write(f"{item[1]}\n")

        elapsed = time.time() - t0
        print("\n" + "=" * 60)
        print(f"🎉 生成完成！耗时 {elapsed:.1f}s")
        print(f"   📄 TXT : {txt_path}")
        print(f"   🎯 规则: 同频道最多 {RESULTS_PER_CHANNEL} 个 URL，最流畅在前")
        print(f"   📊 最终有效源: {len(global_deduped)} 条")
        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())