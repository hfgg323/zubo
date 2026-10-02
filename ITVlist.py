"""
ITVlist - IPTV 直播源扫描 & 测速 & 去重 & 生成  (修复版 v4)
"""
import asyncio
import aiohttp
import re
import time
from urllib.parse import urljoin, urlparse, parse_qs
from collections import OrderedDict

import requests

# ==================== 配置 ====================
URL_FILE = "https://raw.githubusercontent.com/hfgg323/zubo/main/ip_urls.txt"
RESULTS_PER_CHANNEL = 5          # 同频道最多保留 URL 数
MAX_BYTES = 524288                # ts/flv/mp4 最多拉 512KB
MIN_BYTES = 32768                 # ts/flv/mp4 5s 内读不满此值 → 死链
SCAN_CONCURRENCY = 80             # 扫描 JSON API 并发
SPEED_CONCURRENCY = 60            # 测速并发
JSON_TIMEOUT = 3
SPEED_TIMEOUT = 6
CHECK_TIMEOUT = 1.5

# —— m3u8 分片探活旋钮（按需调）——
M3U8_PROBE_TS_COUNT = 3           # 抽前几个 ts 探活（2=更快但更松，3=更稳）
M3U8_PROBE_TIMEOUT = 4            # 单个 ts 探活超时（秒）
M3U8_PROBE_MIN_BYTES = 1024       # ts 探活最少有效字节（过滤空响应）
M3U8_REQUIRE_RATIO = (2, 3)       # 通过率门槛：至少 2/3 可达才算可播

CHANNEL_CATEGORIES = {
"央视频道": [
"CCTV1", "CCTV2", "CCTV3", "CCTV4", "CCTV4欧洲", "CCTV4美洲", "CCTV5", "CCTV5+", "CCTV6", "CCTV7",
"CCTV8", "CCTV9", "CCTV10", "CCTV11", "CCTV12", "CCTV13", "CCTV14", "CCTV15", "CCTV16", "CCTV17",
"兵器科技", "风云音乐", "风云足球", "风云剧场", "怀旧剧场", "第一剧场", "女性时尚", "世界地理", "央视台球", "高尔夫网球",
"央视文化精品", "卫生健康", "电视指南", "老故事", "中学生", "发现之旅", "书法频道", "国学频道", "环球奇观"
],
"卫视频道": [
"湖南卫视", "浙江卫视", "江苏卫视", "东方卫视", "深圳卫视", "北京卫视", "广东卫视", "广西卫视", "东南卫视", "海南卫视",
"河北卫视", "河南卫视", "湖北卫视", "江西卫视", "四川卫视", "重庆卫视", "贵州卫视", "云南卫视", "天津卫视", "安徽卫视",
"山东卫视", "辽宁卫视", "黑龙江卫视", "吉林卫视", "内蒙古卫视", "宁夏卫视", "山西卫视", "陕西卫视", "甘肃卫视", "青海卫视",
"新疆卫视", "西藏卫视", "三沙卫视", "兵团卫视", "延边卫视", "安多卫视", "康巴卫视", "农林卫视", "山东教育卫视",
"中国教育1台", "中国教育2台", "中国教育3台", "中国教育4台", "早期教育"
],
"数字频道": [
"CHC动作电影", "CHC家庭影院", "CHC影迷电影", "淘电影", "淘精彩", "淘剧场", "淘4K", "淘娱乐", "淘BABY", "淘萌宠", "重温经典",
"星空卫视", "CHANNEL[V]", "凤凰卫视中文台", "凤凰卫视资讯台", "凤凰卫视香港台", "凤凰卫视电影台", "求索纪录", "求索科学",
"求索生活", "求索动物", "纪实人文", "金鹰纪实", "纪实科教", "睛彩青少", "睛彩竞技", "睛彩篮球", "睛彩广场舞", "魅力足球", "五星体育",
"劲爆体育", "快乐垂钓", "茶频道", "先锋乒羽", "天元围棋", "汽摩", "梨园频道", "文物宝库", "武术世界",
"乐游", "生活时尚", "都市剧场", "欢笑剧场", "游戏风云", "金色学堂", "动漫秀场", "新动漫", "卡酷少儿", "金鹰卡通", "优漫卡通", "哈哈炫动", "嘉佳卡通",
"优优宝贝", "中国交通", "中国天气", "海看大片", "经典电影", "精彩影视", "喜剧影院", "动作影院", "精品剧场",
# 以下为新增/整合的频道
"广东经济科教", "广东南方卫视", "广东影视", "广东少儿", "广东珠江", "广东新闻", "广东民生", "广东体育",
"梅州1", "梅州2", "五华台", "岭南戏曲", "SGCS", "本港台", "翡翠台", "明珠台", "谍战剧场", "收视指南", "央视精品1",
"中甲联赛", "GTU游戏竟技", "冬奥记实", "IPTV-野外", "CCTV17-农村农业", "CCTV兵器科技"
]
}
CHANNEL_MAPPING = {
"CCTV1": ["CCTV-1", "CCTV1-综合", "CCTV-1 综合", "CCTV-1综合", "CCTV1HD", "CCTV-1高清", "CCTV-1HD", "cctv-1HD", "CCTV1综合高清", "cctv1", "CCTV1 高清", "CCTV1高清"],
"CCTV2": ["CCTV-2", "CCTV2-财经", "CCTV-2 财经", "CCTV-2财经", "CCTV2HD", "CCTV-2高清", "CCTV-2HD", "cctv-2HD", "CCTV2财经高清", "cctv2", "CCTV2 财经"],
"CCTV3": ["CCTV-3", "CCTV3-综艺", "CCTV-3 综艺", "CCTV-3综艺", "CCTV3HD", "CCTV-3高清", "CCTV-3HD", "cctv-3HD", "CCTV3综艺高清", "cctv3", "CCTV3 综艺"],
"CCTV4": ["CCTV-4", "CCTV4-国际", "CCTV-4 中文国际", "CCTV-4中文国际", "CCTV4HD", "cctv4HD", "CCTV-4HD", "CCTV4-中文国际", "CCTV4国际高清", "cctv4", "cctv4中文国际"],
"CCTV4欧洲": ["CCTV-4欧洲", "CCTV-4欧洲", "CCTV4欧洲 HD", "CCTV-4 欧洲", "CCTV-4中文国际欧洲", "CCTV4中文欧洲", "CCTV4欧洲HD", "cctv4欧洲HD", "CCTV-4欧洲HD", "cctv-4欧洲HD"],
"CCTV4美洲": ["CCTV-4美洲", "CCTV-4北美", "CCTV4美洲 HD", "CCTV-4 美洲", "CCTV-4中文国际美洲", "CCTV4中文美洲", "CCTV4美洲HD", "cctv4美洲HD", "CCTV-4美洲HD", "cctv-4美洲HD"],
"CCTV5": ["CCTV-5", "CCTV5-体育", "CCTV-5 体育", "CCTV-5体育", "CCTV5HD", "CCTV-5高清", "CCTV-5HD", "CCTV5体育", "CCTV5体育高清", "cctv5", "CCTV5 体育"],
"CCTV5+": ["CCTV-5+", "CCTV5+体育赛事", "CCTV-5+ 体育赛事", "CCTV5+体育赛事", "CCTV5+HD", "CCTV-5+高清", "CCTV-5+HD", "cctv-5+HD", "CCTV5plas", "CCTV5+体育赛视高清", "cctv5+", "CCTV5+体育"],
"CCTV6": ["CCTV-6", "CCTV6-电影", "CCTV-6 电影", "CCTV-6电影", "CCTV6HD", "CCTV-6高清", "CCTV-6HD", "cctv-6HD", "CCTV6电影高清", "cctv6", "cctv6 电影"],
"CCTV7": ["CCTV-7", "CCTV7-军农", "CCTV-7 国防军事", "CCTV-7国防军事", "CCTV7HD", "CCTV-7高清", "CCTV-7HD", "CCTV7-国防军事", "CCTV7军事高清", "cctv7", "CCTV7 国防军事"],
"CCTV8": ["CCTV-8", "CCTV8-电视剧", "CCTV-8 电视剧", "CCTV-8电视剧", "CCTV8HD", "CCTV-8高清", "CCTV-8HD", "cctv-8HD", "CCTV8电视剧高清", "cctv8", "CCTV8 高清"],
"CCTV9": ["CCTV-9", "CCTV9-纪录", "CCTV-9 纪录", "CCTV-9纪录", "CCTV9HD", "cctv9HD", "CCTV-9高清", "cctv-9HD", "CCTV9记录高清", "cctv9", "CCTV9纪录高清"],
"CCTV10": ["CCTV-10", "CCTV10-科教", "CCTV-10 科教", "CCTV-10科教", "CCTV10HD", "CCTV-10高清", "CCTV-10HD", "CCTV-10高清", "CCTV10科教高清", "cctv10", "CCTV10高清"],
"CCTV11": ["CCTV-11", "CCTV11-戏曲", "CCTV-11 戏曲", "CCTV-11戏曲", "CCTV11HD", "cctv11HD", "CCTV-11HD", "cctv-11HD", "CCTV11戏曲高清", "cctv11", "CCTV11高清"],
"CCTV12": ["CCTV-12", "CCTV12-社会与法", "CCTV-12 社会与法", "CCTV-12社会与法", "CCTV12HD", "CCTV-12高清", "CCTV-12HD", "cctv-12HD", "CCTV12社会与法高清", "cctv12", "CCTV12 高清"],
"CCTV13": ["CCTV-13", "CCTV13-新闻", "CCTV-13 新闻", "CCTV-13新闻", "CCTV13HD", "cctv13HD", "CCTV-13HD", "cctv-13HD", "CCTV13新闻高清", "cctv13", "cctv13-高清"],
"CCTV14": ["CCTV-14", "CCTV14-少儿", "CCTV-14 少儿", "CCTV-14少儿", "CCTV14HD", "CCTV-14高清", "CCTV-14HD", "CCTV少儿", "CCTV14少儿高清", "cctv14", "CCTV14 少儿"],
"CCTV15": ["CCTV-15", "CCTV15-音乐", "CCTV-15 音乐", "CCTV-15音乐", "CCTV15HD", "cctv15HD", "CCTV-15HD", "cctv-15HD", "CCTV15音乐高清", "cctv15", "CCTV15 音乐"],
"CCTV16": ["CCTV-16", "CCTV-16 HD", "CCTV-16 4K", "CCTV-16奥林匹克", "CCTV16HD", "cctv16HD", "CCTV-16HD", "cctv-16HD", "CCTV16奥林匹克高清", "cctv16", "CCTV16 奥林匹克"],
"CCTV17": ["CCTV-17", "CCTV17高清", "CCTV17 HD", "CCTV-17农业农村", "CCTV17HD", "cctv17HD", "CCTV-17HD", "cctv-17HD", "CCTV17农业农村高清", "cctv17", "CCTV17-农村农业"],
"兵器科技": ["CCTV-兵器科技", "CCTV兵器科技", "CCTV兵器科技", "CCTV兵器"],
"风云音乐": ["CCTV-风云音乐", "CCTV风云音乐", "风云音乐"],
"第一剧场": ["CCTV-第一剧场", "CCTV第一剧场"],
"风云足球": ["CCTV-风云足球", "CCTV风云足球", "风云足球"],
"风云剧场": ["CCTV-风云剧场", "CCTV风云剧场"],
"怀旧剧场": ["CCTV-怀旧剧场", "CCTV怀旧剧场", "怀旧剧场"],
"女性时尚": ["CCTV-女性时尚", "CCTV女性时尚"],
"世界地理": ["CCTV-世界地理", "CCTV世界地理", "世界地理"],
"央视台球": ["CCTV-央视台球", "CCTV央视台球"],
"高尔夫网球": ["CCTV-高尔夫网球", "CCTV高尔夫网球", "CCTV央视高网", "CCTV-高尔夫·网球", "央视高网"],
"央视文化精品": ["CCTV-央视文化精品", "CCTV央视文化精品", "CCTV文化精品", "CCTV-文化精品", "文化精品", "央视精品1"],
"卫生健康": ["CCTV-卫生健康", "CCTV卫生健康"],
"电视指南": ["CCTV-电视指南", "CCTV电视指南", "收视指南"],
"东南卫视": ["福建东南", "东南卫视高清"],
"东方卫视": ["上海卫视", "东方卫视"],
"农林卫视": ["陕西农林卫视"],
"内蒙古卫视": ["内蒙古", "内蒙卫视"],
"康巴卫视": ["四川康巴卫视"],
"山东教育卫视": ["山东教育", "山东教育卫视"],
"中国教育1台": ["CETV1", "中国教育一台", "中国教育1", "CETV", "CETV-1", "中国教育", "中国教育-1", "CETV1高清"],
"中国教育2台": ["CETV2", "中国教育二台", "中国教育2", "CETV-2 空中课堂", "CETV-2", "中国教育-2"],
"中国教育3台": ["CETV3", "中国教育三台", "中国教育3", "CETV-3 教育服务", "CETV-3"],
"中国教育4台": ["CETV4", "中国教育四台", "中国教育4", "中国教育电视台第四频道", "CETV-4"],
"CHC动作电影": ["CHC动作电影高清", "动作电影", "CHC动作电影"],
"CHC家庭影院": ["CHC家庭电影高清", "家庭影院"],
"CHC影迷电影": ["CHC高清电影", "高清电影", "影迷电影", "chc高清电影"],
"淘电影": ["IPTV淘电影", "北京IPTV淘电影", "北京淘电影"],
"淘精彩": ["IPTV淘精彩", "北京IPTV淘精彩", "北京淘精彩"],
"淘剧场": ["IPTV淘剧场", "北京IPTV淘剧场", "北京淘剧场"],
"淘4K": ["IPTV淘4K", "北京IPTV4K超清", "北京淘4K", "淘4K", "淘 4K"],
"淘娱乐": ["IPTV淘娱乐", "北京IPTV淘娱乐", "北京淘娱乐"],
"淘BABY": ["IPTV淘BABY", "北京IPTV淘BABY", "北京淘BABY", "IPTV淘baby", "北京IPTV淘baby", "北京淘baby"],
"淘萌宠": ["IPTV淘萌宠", "北京IPTV萌宠TV", "北京淘萌宠"],
"魅力足球": ["上海魅力足球"],
"睛彩青少": ["睛彩羽毛球"],
"求索纪录": ["求索记录", "求索纪录4K", "求索记录4K", "求索纪录 4K", "求索记录 4K", "求索纪录"],
"金鹰纪实": ["湖南金鹰纪实", "金鹰记实", "金鹰纪实"],
"纪实科教": ["北京纪实科教", "BRTV纪实科教", "北京纪实卫视高清"],
"星空卫视": ["星空衛視", "星空衛视", "星空卫視"],
"CHANNEL[V]": ["Channel [V]", "Channel[V]"],
"凤凰卫视中文台": ["凤凰中文", "凤凰中文台", "凤凰卫视中文", "凤凰卫视", "凤凰中文"],
"凤凰卫视香港台": ["凤凰香港台", "凤凰卫视香港", "凤凰香港"],
"凤凰卫视资讯台": ["凤凰资讯", "凤凰资讯台", "凤凰咨询", "凤凰咨询台", "凤凰卫视咨询台", "凤凰卫视资讯", "凤凰卫视咨询"],
"凤凰卫视电影台": ["凤凰电影", "凤凰电影台", "凤凰卫视电影", "鳳凰衛視電影台", " 凤凰电影"],
"茶频道": ["湖南茶频道"],
"快乐垂钓": ["湖南快乐垂钓"],
"先锋乒羽": ["湖南先锋乒羽"],
"天元围棋": ["天元围棋频道"],
"书法频道": ["书法书画"],
"环球奇观": ["环球旅游"],
"中学生": ["中学生课堂", "中学生"],
"汽摩": ["重庆汽摩", "汽摩频道", "重庆汽摩频道"],
"梨园频道": ["河南梨园频道", "梨园", "河南梨园"],
"文物宝库": ["河南文物宝库"],
"武术世界": ["河南武术世界"],
"乐游": ["乐游频道", "上海乐游频道", "乐游纪实", "SiTV乐游频道", "天天乐游"],
"欢笑剧场": ["上海欢笑剧场4K", "欢笑剧场 4K", "欢笑剧场4K", "上海欢笑剧场"],
"生活时尚": ["生活时尚4K", "SiTV生活时尚", "上海生活时尚"],
"都市剧场": ["都市剧场4K", "SiTV都市剧场", "上海都市剧场"],
"游戏风云": ["游戏风云4K", "SiTV游戏风云", "上海游戏风云", "GTU游戏竟技"],
"金色学堂": ["金色学堂4K", "SiTV金色学堂", "上海金色学堂"],
"动漫秀场": ["动漫秀场4K", "SiTV动漫秀场", "上海动漫秀场", "新动漫"],
"卡酷少儿": ["北京KAKU少儿", "BRTV卡酷少儿", "北京卡酷少儿", "卡酷动画", "北京卡通", "北京少儿", "卡酷动漫", "卡酷少儿"],
"哈哈炫动": ["炫动卡通", "上海哈哈炫动"],
"优漫卡通": ["江苏优漫卡通", "优漫漫画", "优漫卡通"],
"金鹰卡通": ["湖南金鹰卡通", "金鹰卡通"],
"嘉佳卡通": ["佳佳卡通", "嘉佳卡通"],
"中国交通": ["中国交通频道"],
"中国天气": ["中国天气频道"],
"经典电影": ["IPTV经典电影", "经典电影"],
"老故事": ["中央新影-老故事"],
"早期教育": ["早期教育"],
# 新增映射
"广东卫视": ["广东卫视"],
"广东经济科教": ["广东经济科教"],
"大湾区卫视": ["广东南方卫视"],
"广东影视": ["广东影视"],
"广东少儿": ["广东少儿"],
"广东珠江": ["广东珠江"],
"广东新闻": ["广东新闻"],
"广东民生": ["广东民生"],
"广东体育": ["广东体育"],
"梅州1": ["梅州1"],
"梅州2": ["梅州2"],
"五华台": ["五华台"],
"岭南戏曲": ["岭南戏曲"],
"SGCS": ["SGCS"],
"本港台": ["本港台"],
"翡翠台": ["翡翠台"],
"明珠台": ["明珠台"],
"谍战剧场": ["谍战剧场"],
"中甲联赛": ["中甲联赛"],
"冬奥记实": ["冬奥记实"],
"IPTV-野外": ["IPTV-野外"],
"央视精品1": ["央视精品1"],
"收视指南": ["收视指南"],
}

# ==================== 基础函数 ====================
async def load_urls():
    """从 GitHub 下载 IPTV IP 段列表"""
    try:
        resp = requests.get(URL_FILE, timeout=10)
        resp.raise_for_status()
        out = []
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            if "://" not in line:
                line = f"http://{line}"
            netloc = urlparse(line).netloc
            if ":" in netloc and netloc.split(":")[-1].isdigit():
                out.append(line)
        print(f"📡 已加载 {len(out)} 个基础 URL")
        return out
    except Exception as e:
        print(f"❌ 下载 {URL_FILE} 失败: {e}")
        raise SystemExit(1)


async def generate_urls(seed):
    """由一个 'http://ip:port' 生成 256 个候选 JSON API 地址"""
    json_paths = [
        "/iptv/live/1000.json?key=txiptv",
        "/ZHGXTV/Public/json/live_interface.txt",
    ]
    parsed = urlparse(seed)
    host = parsed.netloc or parsed.path
    if ":" not in host:
        return []
    ip, port = host.rsplit(":", 1)
    if not port.isdigit():
        return []
    ip_prefix = ip.rsplit(".", 1)[0]
    results = []
    for i in range(1, 256):
        base = f"http://{ip_prefix}.{i}:{port}"
        for path in json_paths:
            results.append(f"{base}{path}")
    return results


async def check_url(session, url, semaphore):
    """快速探测 JSON API 是否可用（HEAD 级，仅用于筛选 API 入口）"""
    async with semaphore:
        try:
            async with session.get(url, timeout=CHECK_TIMEOUT,
                                  headers={"User-Agent": "VLC/3.0.20"}) as resp:
                if resp.status == 200:
                    ct = resp.headers.get("Content-Type", "")
                    if any(k in ct for k in ("json", "javascript", "text", "xml")):
                        return url
                return None
        except Exception:
            return None


async def fetch_json(session, url, semaphore):
    """抓取节目单 JSON，返回 [(标准化频道名, 播放 URL), ...]"""
    async with semaphore:
        try:
            async with session.get(url, timeout=JSON_TIMEOUT,
                                  headers={"User-Agent": "VLC/3.0.20"}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                results = []
                for item in data.get("data", []):
                    name = (item.get("name") or "").strip()
                    urlx = (item.get("url") or "").strip()
                    if not name or not urlx or "," in urlx:
                        continue
                    if not urlx.startswith("http"):
                        urlx = urljoin(url, urlx)
                    for std_name, aliases in CHANNEL_MAPPING.items():
                        if name == std_name:
                            break
                        if any(a in name for a in aliases if len(a) >= 2):
                            name = std_name
                            break
                    results.append((name, urlx))
                return results
        except Exception:
            return []


# ==================== URL 归一化 ====================
def _url_fingerprint(url: str) -> str:
    """去掉 t=/token=/_= 等一次性参数，用于去重"""
    p = urlparse(url)
    qs = parse_qs(p.query, keep_blank_values=True)
    for k in list(qs.keys()):
        if k.lower() in ("t", "timestamp", "token", "sign", "nonce", "r", "_",
                        "callback", "jsonp", "expire", "expires", "deadline", "st", "e"):
            del qs[k]
    cleaned = "&".join(f"{k}={v[0]}" for k, v in qs.items())
    return f"{p.scheme}://{p.netloc}{p.path}?{cleaned}" if cleaned else f"{p.scheme}://{p.netloc}{p.path}"


# ==================== 真实播放成功率测速 ====================
async def measure_speed(session, url, semaphore):

    is_m3u8 = url.endswith(".m3u8")
    async with semaphore:
        t0 = time.time()
        try:
            # ---------------- m3u8 路径 ----------------
            if is_m3u8:
                headers = {"User-Agent": "VLC/3.0.20 LibVLC/3.0.20",
                           "Accept": "*/*"}
                # ★ 不发 Range：部分 CDN 对 Range 只返回 1 个 ts 片段头，
                #   导致读到的不是索引文本，#EXTM3U 匹配失败 → 真源误杀
                async with session.get(url, headers=headers,
                                      timeout=SPEED_TIMEOUT,
                                      allow_redirects=True) as resp:
                    if resp.status not in (200, 206):
                        return 999999
                    # 走解压器读取，避免服务端 gzip 后 "#EXTM3U" 匹配失败
                    text = await resp.text(errors="ignore")
                    if "#EXTM3U" not in text:
                        return 999999
                    index_elapsed = time.time() - t0

                # 解析 ts 分片 URL
                ts_urls = []
                base_dir = url.rsplit("/", 1)[0] + "/"
                for line in text.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("http"):
                        ts_urls.append(line)
                    else:
                        ts_urls.append(urljoin(base_dir, line))

                if not ts_urls:
                    return 999999   # 索引合法但无分片 → 不可播

                probe_urls = ts_urls[:M3U8_PROBE_TS_COUNT]

                # 并发探活（只拉头 4KB，模拟播放器首帧）
                async def _probe_ts(ts_url):
                    pt0 = time.time()
                    try:
                        async with session.get(
                            ts_url,
                            headers={"Range": f"bytes=0-{M3U8_PROBE_MIN_BYTES*4-1}",
                                     "User-Agent": "VLC/3.0.20 LibVLC/3.0.20"},
                            timeout=M3U8_PROBE_TIMEOUT
                        ) as r:
                            if r.status not in (200, 206):
                                return (False, time.time() - pt0)
                            if len(await r.read()) < M3U8_PROBE_MIN_BYTES:
                                return (False, time.time() - pt0)
                            return (True, time.time() - pt0)
                    except Exception:
                        return (False, time.time() - pt0)

                results = await asyncio.gather(
                    *(_probe_ts(u) for u in probe_urls),
                    return_exceptions=True
                )

                reachable = 0
                probe_time = 0.0
                for r in results:
                    if isinstance(r, tuple):
                        ok, dt = r
                        if ok:
                            reachable += 1
                        if dt > probe_time:
                            probe_time = dt

                n = len(probe_urls)
                req_n = M3U8_REQUIRE_RATIO[0]
                # 通过率不达标 → 淘汰（不是给高分，是直接判死）
                if reachable < req_n:
                    return 999999

                # 惩罚：可达越多分越低；reachable == n 时无惩罚
                # 例 n=3: reachable=3 → 0 ; 2 → 400 ; 1 → 1600
                penalty = int((n - reachable) ** 2 * 400)
                score = int(index_elapsed * 1000) + int(probe_time * 1000) + penalty
                return max(score, 1)

            # ---------------- ts / flv / mp4 路径 ----------------
            headers = {
                "Range": f"bytes=0-{MAX_BYTES - 1}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Connection": "close",
            }
            redirects = 0
            first_byte_latency = None
            total_read = 0

            async with session.get(url, headers=headers,
                                  timeout=SPEED_TIMEOUT,
                                  allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    return 999999
                redirects = len(getattr(resp, "history", None) or [])

                async for chunk in resp.content.iter_chunked(16384):
                    if first_byte_latency is None:
                        first_byte_latency = (time.time() - t0) * 1000
                    total_read += len(chunk)
                    if total_read >= MAX_BYTES:
                        break

            elapsed = max(time.time() - t0, 0.001)

            # ★ 关键修复：只对 ts/flv/mp4 用字节门槛；
            #   m3u8 是文本索引，永远 < MIN_BYTES，套用此门槛会把 100% m3u8 判死
            #   → 全局去重后 0 条（v3 的根因）
            if total_read < MIN_BYTES:
                return 999999

            latency = first_byte_latency if first_byte_latency is not None else elapsed * 1000
            speed_kbps = (total_read / 1024) / elapsed

            # 吞吐惩罚上限收敛，不再出现动辄几万的虚高分数
            throughput_penalty = max(0, 400 - speed_kbps) * 3
            redirect_penalty = redirects * 200
            score = int(latency + throughput_penalty + redirect_penalty)
            return max(score, 1)

        except (aiohttp.ClientError, asyncio.TimeoutError):
            return 999999
        except Exception:
            return 999999


def is_valid_stream(url):
    """过滤明显不可播 / 写入即失效的 URL"""
    if not url.startswith("http"):
        return False

    # 组播 / 内网
    if url.startswith(("rtp://", "udp://", "rtsp://", "igmp://")):
        return False
    if re.match(r"https?://(239\.|224\.|225\.|226\.)", url):
        return False
    if re.match(r"http://(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)", url):
        return False
    # 运营商 CGN / 内网 IPTV 入口（需鉴权，外网不可播）
    if re.match(r"http://(16\.|100\.64\.|100\.96\.)", url):
        return False

    # 常见不可播路径特征
    bad_paths = ("/paiptv/", "/00/SNM/", "/00/CHANNEL", "/live/live",
                 "/hls/live/live", "index.m3u8/index.m3u8",
                 ".m3u8.m3u8", "/wvm/", "/drm/", "/widevine/")
    if any(p in url for p in bad_paths):
        return False

    # 必须带可识别的流后缀
    valid_ext = (".m3u8", ".ts", ".flv", ".mp4", ".mkv")
    if not any(ext in url.lower() for ext in valid_ext):
        return False

    # 一次性 token URL：写入文件后几小时即失效，果断丢弃
    qs = parse_qs(urlparse(url).query)
    if any(k.lower() in ("token", "sign", "expires", "expire", "deadline", "st", "e") for k in qs):
        return False

    return True


def speed_grade(score):
    """根据分数给稳定等级（A 最稳，D 最差）"""
    if score < 400:
        return "A"
    if score < 1200:
        return "B"
    if score < 3000:
        return "C"
    return "D"

# ==================== 主流程 ====================
async def main():
    t0 = time.time()
    print("🚀 开始运行 ITVlist 脚本（真实播放测速·修复版 v4）")
    print(f"   ⚙️  m3u8 探活: 抽 {M3U8_PROBE_TS_COUNT} 个 ts, "
          f"超时 {M3U8_PROBE_TIMEOUT}s, 门槛 {M3U8_REQUIRE_RATIO[0]}/{M3U8_REQUIRE_RATIO[1]}")
    scan_sem = asyncio.Semaphore(SCAN_CONCURRENCY)
    speed_sem = asyncio.Semaphore(SPEED_CONCURRENCY)

    urls = await load_urls()

    async with aiohttp.ClientSession() as session:
        # 1) 生成候选 JSON API 地址
        all_urls = []
        for u in urls:
            all_urls.extend(await generate_urls(u))
        print(f"🔍 生成待扫描 URL 共: {len(all_urls)} 个")

        # 2) 快速筛选可用 JSON API
        print("⏳ 开始检测可用 JSON API...")
        tasks = [check_url(session, u, scan_sem) for u in all_urls]
        valid_urls = [r for r in await asyncio.gather(*tasks) if r]
        print(f"✅ 可用 JSON 地址: {len(valid_urls)} 个")
        for u in valid_urls[:20]:
            print(f"  - {u}")
        if len(valid_urls) > 20:
            print(f"  ... 及其他 {len(valid_urls) - 20} 个")

        # 3) 抓取节目单
        print("📥 开始抓取节目单 JSON...")
        tasks = [fetch_json(session, u, scan_sem) for u in valid_urls]
        results = []
        for sublist in await asyncio.gather(*tasks):
            results.extend(sublist)
        print(f"📺 抓到频道总数: {len(results)} 条")

        # 4) 去重：(name, url去参) 唯一 + 合法流
        seen = set()
        deduped_results = []
        for name, url in results:
            if not is_valid_stream(url):
                continue
            key = (name, _url_fingerprint(url))
            if key not in seen:
                seen.add(key)
                deduped_results.append((name, url))
        print(f"🧹 去重后频道总数: {len(deduped_results)} 条")

        # 5) 真实播放测速
        print("🚀 开始真实播放测速（ts 按吞吐，m3u8 按分片可达性）...")
        speed_tasks = [measure_speed(session, url, speed_sem) for (_, url) in deduped_results]
        speeds = await asyncio.gather(*speed_tasks)

        # 统计淘汰原因（调试用）
        m3u8_tested = 0
        m3u8_alive = 0
        for (name, url), speed in zip(deduped_results, speeds):
            if url.endswith(".m3u8"):
                m3u8_tested += 1
                if speed != 999999:
                    m3u8_alive += 1

        final_results = [
            (name, url, speed)
            for (name, url), speed in zip(deduped_results, speeds)
            if speed != 999999
        ]
        if m3u8_tested:
            print(f"   📊 m3u8 探活: {m3u8_alive}/{m3u8_tested} 通过 "
                  f"({m3u8_alive/m3u8_tested*100:.0f}%)")

        # 6) 全局按 url(去参) 去重，保留速度最快的源
        seen_urls = {}
        for name, url, speed in final_results:
            fp = _url_fingerprint(url)
            if fp not in seen_urls or speed < seen_urls[fp][2]:
                seen_urls[fp] = (name, url, speed)
        global_deduped = list(seen_urls.values())
        global_deduped.sort(key=lambda x: x[2])
        print(f"🌍 全局去重后频道总数: {len(global_deduped)} 条")

        # 7) 按分类归档
        itv_dict = {cat: [] for cat in CHANNEL_CATEGORIES}
        for name, url, speed in global_deduped:
            cat = "其他"
            placed = False
            for c, channels in CHANNEL_CATEGORIES.items():
                if c == "其他":
                    continue
                if name in channels or any(name.startswith(x) for x in channels if len(x) >= 2):
                    cat = c
                    placed = True
                    break
            if not placed:
                cat = _auto_category(name)
            itv_dict[cat].append((name, url, speed))

        for cat in CHANNEL_CATEGORIES:
            print(f"📦 分类《{cat}》找到 {len(itv_dict[cat])} 条频道")

        # 8) 写入 TXT（本地 / 极简播放器格式）
        txt_path = "itvlist.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            for cat in CHANNEL_CATEGORIES:
                items = itv_dict[cat]
                if not items:
                    continue
                f.write(f"{cat},#genre#\n")
                by_name = OrderedDict()
                for name, url, speed in items:
                    by_name.setdefault(name, []).append((url, speed))
                for ch, candidates in by_name.items():
                    candidates.sort(key=lambda x: x[1])
                    candidates = candidates[:RESULTS_PER_CHANNEL]
                    for url, speed in candidates:
                        grade = speed_grade(speed)
                        f.write(f"{ch},{url}  # {grade}\n")
                f.write("\n")

        # 9) 写入 M3U（标准播放器格式）
        m3u_path = "itvlist.m3u"
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for cat in CHANNEL_CATEGORIES:
                items = itv_dict[cat]
                if not items:
                    continue
                f.write(f"#EXTGRP:{cat}\n")
                by_name = OrderedDict()
                for name, url, speed in items:
                    by_name.setdefault(name, []).append((url, speed))
                for ch, candidates in by_name.items():
                    candidates.sort(key=lambda x: x[1])
                    candidates = candidates[:RESULTS_PER_CHANNEL]
                    for url, speed in candidates:
                        grade = speed_grade(speed)
                        f.write(
                            f'#EXTINF:-1 group-title="{cat}" tvg-name="{ch}" '
                            f'rating="{grade}",{ch}\n'
                        )
                        f.write(f"{url}\n")

        # 10) 质量分布统计
        dist = {"A": 0, "B": 0, "C": 0, "D": 0}
        for name, url, speed in global_deduped:
            dist[speed_grade(speed)] += 1
        elapsed = time.time() - t0
        print("=" * 50)
        print(f"🎉 生成完成！耗时 {elapsed:.1f}s")
        print(f"   📄 TXT : {txt_path}")
        print(f"   🏆 质量分布: A={dist['A']} B={dist['B']} C={dist['C']} D={dist['D']}")
        print(f"   🎯 规则: 同频道最多 {RESULTS_PER_CHANNEL} 个 URL，最流畅在前")
        if global_deduped and dist["D"] > len(global_deduped) * 0.5:
            print("   ⚠️  D 级仍偏多，可调：M3U8_PROBE_TIMEOUT↓ / MIN_BYTES↑ / 换 IP 段源")
        print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
