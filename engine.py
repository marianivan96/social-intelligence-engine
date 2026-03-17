cd ~/si && cat > engine.py << 'ENDOFFILE'
#!/usr/bin/env python3
import os, sys, json, time, hashlib, re, argparse, logging
from datetime import datetime, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    os.system("pip3 install python-dotenv -q")
    from dotenv import load_dotenv
load_dotenv()

def _pkg(name, pip=None):
    try: return __import__(name)
    except ImportError:
        os.system(f"pip3 install {pip or name} -q")
        return __import__(name)

requests = _pkg("requests")
feedparser = _pkg("feedparser")
_pkg("dateutil", "python-dateutil")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("si")

DATA_DIR = Path(os.environ.get("DATA_DIR", "./data"))
REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", "./reports"))
LOOKBACK_DAYS = 30
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

LINKEDIN_PROFILES = [
    {"name": "Ilona Kickbusch", "url": "https://www.linkedin.com/in/ilona-kickbusch/"},
    {"name": "Tamas Bereczky", "url": "https://www.linkedin.com/in/tamasbereczky/"},
    {"name": "Jacqueline Novogratz", "url": "https://www.linkedin.com/in/jacquelinenovogratz/"},
    {"name": "Paul Polman", "url": "https://www.linkedin.com/in/paulpolman/"},
    {"name": "Raj Kumar", "url": "https://www.linkedin.com/in/rajkumardevex/"},
    {"name": "David Bank", "url": "https://www.linkedin.com/in/davidbankimpact/"},
    {"name": "Aunnie Patton Power", "url": "https://www.linkedin.com/in/aunniepattonpower/"},
    {"name": "Durreen Shahnaz", "url": "https://www.linkedin.com/in/durreenshahnaz/"},
    {"name": "Laurie Lane-Zucker", "url": "https://www.linkedin.com/in/laurielanezucker/"},
    {"name": "Vu Le", "url": "https://www.linkedin.com/in/vule/"},
]
LINKEDIN_ORGS = [
    {"name": "EPHA", "url": "https://www.linkedin.com/company/european-public-health-alliance/"},
    {"name": "EHMA", "url": "https://www.linkedin.com/company/ehma/"},
    {"name": "EuroHealthNet", "url": "https://www.linkedin.com/company/eurohealthnet/"},
    {"name": "Health First Europe", "url": "https://www.linkedin.com/company/health-first-europe/"},
    {"name": "PCORI", "url": "https://www.linkedin.com/company/pcori/"},
    {"name": "Philea", "url": "https://www.linkedin.com/company/philea-philanthropy-europe/"},
    {"name": "GIIN", "url": "https://www.linkedin.com/company/the-giin/"},
    {"name": "SOCAP Global", "url": "https://www.linkedin.com/company/socap-global/"},
    {"name": "ImpactAlpha", "url": "https://www.linkedin.com/company/impactalpha/"},
    {"name": "PatientsLikeMe", "url": "https://www.linkedin.com/company/patientslikeme/"},
    {"name": "BMJ", "url": "https://www.linkedin.com/company/bmj/"},
    {"name": "Active Citizenship Network", "url": "https://www.linkedin.com/company/active-citizenship-network/"},
    {"name": "IAPO", "url": "https://www.linkedin.com/company/international-alliance-of-patients-organizations/"},
    {"name": "EPF", "url": "https://www.linkedin.com/company/european-patients-forum/"},
    {"name": "EURORDIS", "url": "https://www.linkedin.com/company/eurordis/"},
]
RSS_FEEDS = [
    {"name": "The Lancet", "url": "https://www.thelancet.com/rssfeed/lancet_online.xml", "cat": "journal"},
    {"name": "BMJ", "url": "https://www.bmj.com/rss/recent.xml", "cat": "journal"},
    {"name": "BMC Public Health", "url": "https://bmcpublichealth.biomedcentral.com/articles/most-recent/rss.xml", "cat": "journal"},
    {"name": "Health Policy", "url": "https://rss.sciencedirect.com/publication/science/01688510", "cat": "journal"},
    {"name": "Milbank Quarterly", "url": "https://onlinelibrary.wiley.com/feed/14680009/most-recent", "cat": "journal"},
    {"name": "SSIR", "url": "https://ssir.org/rss", "cat": "magazine"},
    {"name": "Devex", "url": "https://www.devex.com/news/rss", "cat": "magazine"},
    {"name": "GreenBiz", "url": "https://www.greenbiz.com/rss.xml", "cat": "magazine"},
    {"name": "Alliance Magazine", "url": "https://www.alliancemagazine.org/feed/", "cat": "magazine"},
    {"name": "Nonprofit Quarterly", "url": "https://nonprofitquarterly.org/feed/", "cat": "newsletter"},
    {"name": "Grist", "url": "https://grist.org/feed/", "cat": "newsletter"},
    {"name": "ImpactAlpha", "url": "https://impactalpha.com/feed/", "cat": "newsletter"},
    {"name": "Chronicle of Philanthropy", "url": "https://www.philanthropy.com/rss", "cat": "newsletter"},
    {"name": "Intl Health Policies", "url": "https://www.internationalhealthpolicies.org/feed/", "cat": "newsletter"},
    {"name": "Heated (climate)", "url": "https://heated.world/feed", "cat": "newsletter"},
]
PODCAST_FEEDS = [
    {"name": "Outrage + Optimism", "url": "https://feeds.simplecast.com/3MNxGJQe", "cat": "podcast"},
    {"name": "Business Fights Poverty", "url": "https://feeds.buzzsprout.com/1718017.rss", "cat": "podcast"},
    {"name": "Impact Boom", "url": "https://feeds.soundcloud.com/users/soundcloud:users:244535251/sounds.rss", "cat": "podcast"},
    {"name": "Superpowers for Good", "url": "https://feeds.libsyn.com/67060/rss", "cat": "podcast"},
    {"name": "Giving with Impact", "url": "https://feeds.buzzsprout.com/1107573.rss", "cat": "podcast"},
]
WEBSITES = [
    {"name": "WHO Civil Society Commission", "url": "https://www.who.int/groups/who-civil-society-commission"},
    {"name": "EPF News", "url": "https://www.eu-patient.eu/news/"},
    {"name": "EURORDIS News", "url": "https://www.eurordis.org/news/"},
    {"name": "NextBillion", "url": "https://nextbillion.net/"},
    {"name": "Philanthropy Impact", "url": "https://www.philanthropy-impact.org/news"},
    {"name": "EuroHealth", "url": "https://eurohealthobservatory.who.int/publications/periodical/eurohealth"},
]

def _strip(t): return re.sub(r"<[^>]+>", "", t or "").strip()

def _item(stype, sname, title, content, date, url, mtype="article", eng=None, tags=None):
    if not isinstance(date, str): date = str(date) if date else ""
    return {"id": hashlib.md5(f"{title}{url}".encode()).hexdigest()[:12],
            "source_type": stype, "source_name": sname, "title": (title or "")[:200],
            "content": (content or "")[:3000], "date": date, "url": url or "",
            "media_type": mtype, "engagement": eng or {}, "hashtags": tags or []}

def _parse_date(entry):
    for a in ("published_parsed", "updated_parsed"):
        p = getattr(entry, a, None)
        if p:
            try: return datetime(*p[:6])
            except Exception: pass
    for a in ("published", "updated"):
        raw = getattr(entry, a, None)
        if raw and isinstance(raw, str):
            try:
                from dateutil.parser import parse
                return parse(raw)
            except Exception: pass
    return None

def collect_rss():
    log.info("Collecting RSS feeds...")
    items = []
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    for f in RSS_FEEDS + PODCAST_FEEDS:
        try:
            log.info(f"  {f['name']}")
            try:
                raw = requests.get(f["url"], headers={"User-Agent": UA}, timeout=15)
                feed = feedparser.parse(raw.content)
            except Exception:
                feed = feedparser.parse(f["url"])
            n = 0
            for e in feed.entries[:25]:
                d = _parse_date(e)
                if d is not None and isinstance(d, datetime) and d < cutoff:
                    continue
                c = _strip(getattr(e, "summary", "") or getattr(e, "description", "") or getattr(e, "title", ""))
                items.append(_item(f["cat"], f["name"], getattr(e, "title", ""), c[:2000],
                    d.isoformat() if isinstance(d, datetime) else "",
                    getattr(e, "link", ""),
                    "podcast_episode" if f["cat"] == "podcast" else "article"))
                n += 1
            log.info(f"    -> {n} items (feed had {len(feed.entries)} entries)")
        except Exception as ex:
            log.warning(f"    FAILED: {ex}")
    return items

def collect_linkedin():
    tok = os.environ.get("APIFY_API_TOKEN")
    if not tok: log.warning("APIFY_API_TOKEN not set"); return []
    log.info("Collecting LinkedIn via Apify...")
    urls = [p["url"] for p in LINKEDIN_PROFILES] + [o["url"] for o in LINKEDIN_ORGS]
    items = []
    for i in range(0, len(urls), 10):
        batch = urls[i:i+10]
        log.info(f"  Batch {i//10+1} ({len(batch)} profiles)...")
        try:
            r = requests.post("https://api.apify.com/v2/acts/harvestapi~linkedin-profile-posts/run-sync-get-dataset-items",
                params={"token": tok}, json={"profileUrls": batch, "maxPosts": 15}, timeout=300)
            if r.status_code in (200, 201):
                for p in r.json():
                    if not p.get("content"): continue
                    c = p["content"]
                    ds = p.get("postedDate") or p.get("publishedAt") or p.get("postedAt") or p.get("date") or ""
                    if not isinstance(ds, str): ds = str(ds) if ds else ""
                    items.append(_item("linkedin", p.get("authorName") or p.get("companyName", "Unknown"),
                        c[:100]+"...", c[:2000], ds, p.get("linkedinUrl") or p.get("shareUrl", ""), "linkedin_post",
                        {"likes": p.get("totalReactionCount",0), "comments": p.get("commentsCount",0), "shares": p.get("repostCount",0)},
                        [w for w in c.split() if w.startswith("#")]))
                log.info(f"    -> {len(items)} posts so far")
            else: log.warning(f"    Apify {r.status_code}")
        except Exception as ex: log.warning(f"    Failed: {ex}")
        time.sleep(2)
    return items

def collect_websites():
    tok = os.environ.get("APIFY_API_TOKEN")
    if not tok: log.warning("APIFY_API_TOKEN not set"); return []
    log.info("Crawling websites via Apify...")
    items = []
    for s in WEBSITES:
        try:
            log.info(f"  {s['name']}")
            r = requests.post("https://api.apify.com/v2/acts/apify~website-content-crawler/run-sync-get-dataset-items",
                params={"token": tok}, json={"startUrls":[{"url":s["url"]}],"maxCrawlPages":5,"crawlerType":"cheerio"}, timeout=120)
            if r.status_code in (200, 201):
                for pg in r.json():
                    t = pg.get("text") or pg.get("markdown","")
                    if not t or len(t)<50: continue
                    ds = pg.get("metadata",{}).get("date","")
                    if not isinstance(ds, str): ds = ""
                    items.append(_item("website", s["name"], pg.get("metadata",{}).get("title") or s["name"],
                        t[:2000], ds, pg.get("url", s["url"]), "web_article"))
                log.info(f"    -> {len([x for x in items if x['source_name']==s['name']])} pages")
            else: log.warning(f"    Error {r.status_code}")
        except Exception as ex: log.warning(f"    Failed: {ex}")
    return items

def collect_all():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    items = collect_rss() + collect_linkedin() + collect_websites()
    seen = set(); unique = []
    for i in items:
        if i["id"] not in seen: seen.add(i["id"]); unique.append(i)
    unique.sort(key=lambda x: str(x.get("date") or ""), reverse=True)
    sc = {}; sn = set()
    for i in unique: sc[i["source_type"]] = sc.get(i["source_type"],0)+1; sn.add(i["source_name"])
    result = {"items": unique, "stats": {"total": len(unique), "by_type": sc,
        "sources": sorted(sn), "collected_at": datetime.now().isoformat(), "lookback_days": LOOKBACK_DAYS}}
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    with open(DATA_DIR/f"collected_{ts}.json","w") as f: json.dump(result,f,indent=2)
    log.info(f"Collected {len(unique)} items -> data/collected_{ts}.json")
    return result

def _claude(prompt, mt=4096):
    k = os.environ.get("ANTHROPIC_API_KEY")
    if not k: raise RuntimeError("ANTHROPIC_API_KEY not set")
    r = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key":k,"anthropic-version":"2023-06-01","Content-Type":"application/json"},
        json={"model":"claude-sonnet-4-20250514","max_tokens":mt,"messages":[{"role":"user","content":prompt}]}, timeout=180)
    if r.status_code != 200: raise RuntimeError(f"Claude {r.status_code}: {r.text[:300]}")
    return r.json()["content"][0]["text"]

def analyze(collected):
    items = collected["items"]; stats = collected["stats"]
    log.info(f"Analyzing {len(items)} items...")
    cd = [{"source":i["source_name"],"type":i["source_type"],"title":i["title"],
           "snippet":i["content"][:400],"date":i["date"],
           "engagement":i.get("engagement",{}),"hashtags":i.get("hashtags",[])} for i in items[:250]]
    today = datetime.now().strftime("%B %d, %Y")
    month = datetime.now().strftime("%B %Y")
    prompt = f"""You are an expert social intelligence analyst for global health, patient advocacy, impact investing, philanthropy, and social innovation.

Today is {today}. This report covers {month}. Analyze {len(cd)} content items from:
- Journals (Lancet, BMJ, BMC Public Health, Health Policy, Milbank)
- Magazines (SSIR, Devex, GreenBiz, Alliance Magazine)
- Newsletters (ImpactAlpha, NPQ, Grist, Chronicle of Philanthropy)
- LinkedIn (Kickbusch, Polman, Novogratz, EPHA, GIIN, etc.)
- Podcasts (Outrage+Optimism, Business Fights Poverty, Impact Boom)
- Websites (WHO, EPF, EURORDIS, NextBillion)

STATS: {json.dumps(stats)}
CONTENT: {json.dumps(cd,indent=1)}

Return ONLY valid JSON (no backticks):
{{"executive_summary":"...","top_themes":[{{"theme":"...","description":"...","evidence":["..."],"momentum":"rising|stable|declining","relevance_score":8}}],"key_discussions":[{{"topic":"...","summary":"...","key_voices":["..."],"implications":"..."}}],"sentiment_overview":{{"overall":"...","by_sector":{{"health_policy":"...","impact_investing":"...","patient_advocacy":"...","philanthropy":"...","climate_health":"..."}},"notable_shifts":"..."}},"cross_sector_connections":[{{"connection":"...","sources":["..."],"opportunity":"..."}}],"notable_voices":[{{"name":"...","message":"...","reach":"high|medium","engagement_note":"..."}}],"watch_list":[{{"item":"...","why":"...","timeline":"..."}}],"recommendations":[{{"action":"...","rationale":"...","priority":"high|medium|low"}}]}}

Return 5 themes, 3-4 discussions, 3-4 connections, 4-5 voices, 4 watch items, 5 recommendations."""
    txt = _claude(prompt)
    try:
        m = re.search(r"\{[\s\S]*\}", txt)
        return json.loads(m.group()) if m else {"raw": txt}
    except: return {"raw": txt}

def gen_report(analysis):
    log.info("Generating report...")
    mo = datetime.now().strftime("%B %Y")
    return _claude(f"""Write a Monthly Social Intelligence Briefing for {mo} in Markdown.
Audience: leadership in global health and social impact.
{json.dumps(analysis,indent=2)}
Sections: Executive summary, Top themes, Key discussions, Cross-sector connections, Notable voices,
Sentiment dashboard, Watch list, Recommended actions, Sources summary.
Return ONLY Markdown, no code fences.""")

def run_pipeline(skip_collect=False, skip_analyze=False, data_file=None):
    DATA_DIR.mkdir(parents=True,exist_ok=True); REPORTS_DIR.mkdir(parents=True,exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d")
    if skip_collect:
        p = Path(data_file) if data_file else sorted(DATA_DIR.glob("collected_*.json"))[-1]
        with open(p) as f: collected = json.load(f)
    else:
        collected = collect_all()
        if skip_analyze: return collected
    analysis = analyze(collected)
    with open(DATA_DIR/f"analysis_{ts}.json","w") as f: json.dump(analysis,f,indent=2)
    report = gen_report(analysis)
    with open(REPORTS_DIR/f"report_{ts}.md","w") as f: f.write(report)
    dash = {"analysis":analysis,"report_markdown":report,"stats":collected["stats"],
            "generated_at":datetime.now().isoformat(),"item_count":len(collected["items"])}
    for p in [DATA_DIR/"latest_dashboard.json", DATA_DIR/f"dashboard_{ts}.json"]:
        with open(p,"w") as f: json.dump(dash,f,indent=2)
    log.info("="*50); log.info("PIPELINE COMPLETE"); return dash

def serve(host="0.0.0.0", port=5111):
    _pkg("flask"); _pkg("flask_cors","flask-cors")
    from flask import Flask, jsonify, Response
    from flask_cors import CORS
    app = Flask(__name__); CORS(app)
    ha = bool(os.environ.get("ANTHROPIC_API_KEY")); hp = bool(os.environ.get("APIFY_API_TOKEN"))
    log.info(f"  ANTHROPIC_API_KEY: {'SET' if ha else 'MISSING'}")
    log.info(f"  APIFY_API_TOKEN:   {'SET' if hp else 'MISSING'}")

    @app.route("/health")
    def health(): return jsonify({"status":"ok"})

    @app.route("/api/run", methods=["POST"])
    def api_run():
        try: r=run_pipeline(); return jsonify({"status":"success","item_count":r.get("item_count",0)})
        except Exception as e: log.error(str(e),exc_info=True); return jsonify({"status":"error","message":str(e)}),500

    @app.route("/api/collect", methods=["POST"])
    def api_collect():
        try: r=collect_all(); return jsonify({"status":"success","items":r["stats"]["total"]})
        except Exception as e: return jsonify({"status":"error","message":str(e)}),500

    @app.route("/api/analyze", methods=["POST"])
    def api_analyze():
        try: run_pipeline(skip_collect=True); return jsonify({"status":"success"})
        except Exception as e: return jsonify({"status":"error","message":str(e)}),500

    @app.route("/api/dashboard")
    def api_dashboard():
        p=DATA_DIR/"latest_dashboard.json"
        if not p.exists(): return jsonify({"error":"No data yet"}),404
        with open(p) as f: return jsonify(json.load(f))

    @app.route("/api/report")
    def api_report():
        fs=sorted(REPORTS_DIR.glob("report_*.md"))
        if not fs: return Response("No reports",mimetype="text/plain"),404
        with open(fs[-1]) as f: return Response(f.read(),mimetype="text/markdown")

    @app.route("/api/history")
    def api_history():
        return jsonify([{"filename":f.name,"date":f.stem.replace("dashboard_","")} for f in sorted(DATA_DIR.glob("dashboard_*.json"))])

    log.info(f"Starting on {host}:{port}")
    app.run(host=host,port=port,debug=False)

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("command",choices=["collect","analyze","run","serve"])
    p.add_argument("--port",type=int,default=5111); p.add_argument("--data-file",type=str)
    a = p.parse_args()
    if a.command=="collect": collect_all()
    elif a.command=="analyze": run_pipeline(skip_collect=True,data_file=a.data_file)
    elif a.command=="run": run_pipeline()
    elif a.command=="serve": serve(port=a.port)
ENDOFFILE
echo "File written. Lines: $(wc -l < engine.py)"