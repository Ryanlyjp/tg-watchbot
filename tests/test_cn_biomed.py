import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import app


def feed_bytes(title: str, link: str, guid: str, published: datetime, source: str = "") -> bytes:
    source_xml = f"<source>{source}</source>" if source else ""
    return (
        "<?xml version='1.0' encoding='utf-8'?><rss version='2.0'><channel><title>Test</title>"
        f"<item><title>{title}</title><link>{link}</link><guid>{guid}</guid>"
        f"<pubDate>{format_datetime(published)}</pubDate>{source_xml}<description>{title}</description></item>"
        "</channel></rss>"
    ).encode()


class CnBiomedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.old_db_path = app.DB_PATH
        self.old_env_path = app.ENV_PATH
        self.old_config = app.config
        app.DB_PATH = Path(self.temp_dir.name) / "test.sqlite3"
        app.ENV_PATH = Path(self.temp_dir.name) / ".env"
        app.config = {
            "http": {"timeout_seconds": 5},
            "cn_biomed": {
                "enabled": True,
                "translate_titles": True,
                "max_age_hours": 36,
                "dedupe_days": 10,
                "feeds": [],
            },
        }
        app.init_db()

    def tearDown(self) -> None:
        app.DB_PATH = self.old_db_path
        app.ENV_PATH = self.old_env_path
        app.config = self.old_config
        self.temp_dir.cleanup()

    def test_default_sources_are_ivd_only(self) -> None:
        feeds = app.default_cn_biomed_feeds()
        urls = {feed["url"] for feed in feeds}
        names = {feed["name"] for feed in feeds}
        self.assertEqual(6, len(feeds))
        self.assertIn("Google News 中国 IVD", names)
        self.assertIn("Google News 中国 IVD 厂家", names)
        self.assertIn("Google News 国际 IVD", names)
        self.assertIn("Google News 国际 IVD 厂家", names)
        self.assertIn("https://investors.bd.com/news-events/press-releases/rss", urls)
        self.assertIn("https://www.medtechdive.com/feeds/news/", urls)
        self.assertNotIn("https://www.medicaldevice-network.com/feed/", urls)
        self.assertFalse(any("bioworld.com" in url or "chinanews.com.cn" in url for url in urls))
        self.assertTrue(all(feed["sources"] == [] for feed in feeds))
        self.assertTrue(all("创新药" not in feed["keywords"] for feed in feeds))
        self.assertTrue(all(feed["baseline_on_first_run"] is False for feed in feeds))
        self.assertTrue(all("when%3A1d" in url for url in urls if "news.google.com" in url))

    def test_google_news_source_is_parsed(self) -> None:
        body = feed_bytes(
            "体外诊断试剂获批 - 财联社",
            "https://news.google.com/rss/articles/1",
            "google-1",
            datetime.now(timezone.utc),
            "财联社",
        )
        item = app.parse_rss_items({"name": "Google", "type": "rss", "url": "https://news.google.com"}, body)[0]
        self.assertEqual("财联社", item.source)

    def test_only_items_from_the_last_36_hours_are_recent(self) -> None:
        now = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
        recent = app.MonitorItem("1", "recent", "https://example.com/1", "", published=format_datetime(now - timedelta(hours=35)))
        old = app.MonitorItem("2", "old", "https://example.com/2", "", published=format_datetime(now - timedelta(hours=37)))
        missing = app.MonitorItem("3", "missing", "https://example.com/3", "")
        self.assertTrue(app.cn_biomed_item_is_recent(recent, 36, now))
        self.assertFalse(app.cn_biomed_item_is_recent(old, 36, now))
        self.assertFalse(app.cn_biomed_item_is_recent(missing, 36, now))

    def test_title_normalization_removes_google_source_suffix(self) -> None:
        left = app.normalize_cn_biomed_title("同一诊断试剂获批 - 财联社", "财联社")
        right = app.normalize_cn_biomed_title("同一诊断试剂获批 — 医药魔方", "医药魔方")
        self.assertEqual(left, right)
        self.assertEqual("同一诊断试剂获批", app.cn_biomed_display_title("同一诊断试剂获批 - 财联社", "财联社"))

    def test_short_english_keywords_do_not_match_google_redirect_ids(self) -> None:
        item = app.MonitorItem(
            "1",
            "普通行业消息",
            "https://news.google.com/rss/articles/randomBDvalue",
            '<a href="https://news.google.com/rss/articles/randomBDvalue">普通行业消息</a>',
        )
        self.assertEqual([], app.cn_biomed_keyword_hits(item, ["BD", "IND"]))
        item.title = "Company signs BD agreement for an ADC"
        self.assertEqual(["BD", "ADC"], app.cn_biomed_keyword_hits(item, ["BD", "IND", "ADC"]))

    def test_translation_helpers_preserve_chinese_and_clean_model_output(self) -> None:
        self.assertFalse(app.title_needs_chinese_translation("中国诊断试剂获批"))
        self.assertTrue(app.title_needs_chinese_translation("Diagnostic assay wins clearance"))
        self.assertEqual("诊断试剂获批", app.clean_translated_title('中文标题： “诊断试剂获批”'))

    def test_chat_translation_uses_configured_compatible_api(self) -> None:
        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"choices": [{"message": {"content": "中国来源资产达成交易"}}]}

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url: str, json: dict):
                self.url = url
                self.payload = json
                return Response()

        client = Client()
        settings = {
            "base_url": "https://ai.example.com/v1",
            "api_key": "secret",
            "model": "translator",
            "interface": "chat",
            "timeout_seconds": 10,
        }
        with patch.object(app, "cn_biomed_ai_settings", return_value=settings), patch.object(
            app.httpx, "AsyncClient", return_value=client
        ):
            translated = asyncio.run(app.translate_cn_biomed_title("China-origin asset deal"))
        self.assertEqual("中国来源资产达成交易", translated)
        self.assertEqual("https://ai.example.com/v1/chat/completions", client.url)
        self.assertEqual("translator", client.payload["model"])

    def test_source_allowlist_is_exact(self) -> None:
        item = app.MonitorItem("1", "title", "https://example.com", "", source="财联社")
        self.assertTrue(app.cn_biomed_source_allowed(item, {"sources": ["财联社"]}))
        self.assertFalse(app.cn_biomed_source_allowed(item, {"sources": ["东方财富"]}))

    def test_cross_feed_duplicate_is_sent_once_with_translated_title(self) -> None:
        now = datetime.now(timezone.utc)
        first = feed_bytes("Drug wins approval - Source A", "https://example.com/a", "a", now, "Source A")
        second = feed_bytes("Drug wins approval - Source B", "https://example.com/b", "b", now, "Source B")
        feeds = [
            {"name": "Feed A", "url": "https://example.com/a.xml", "enabled": True, "baseline_on_first_run": False},
            {"name": "Feed B", "url": "https://example.com/b.xml", "enabled": True, "baseline_on_first_run": False},
        ]
        send = AsyncMock(return_value=True)
        with patch.object(app, "cn_biomed_bot_env_configured", return_value=True), patch.object(
            app, "fetch_url", new=AsyncMock(side_effect=[first, second])
        ), patch.object(app, "translate_cn_biomed_title", new=AsyncMock(return_value="药物获批")), patch.object(
            app, "send_cn_biomed_notification", new=send
        ):
            self.assertEqual(1, asyncio.run(app.run_cn_biomed_feed(feeds[0])))
            self.assertEqual(0, asyncio.run(app.run_cn_biomed_feed(feeds[1])))

        self.assertEqual(1, send.await_count)
        self.assertEqual("来源：Source A\n标题：药物获批\n链接：https://example.com/a", send.await_args.args[0])
        with app.closing(app.db()) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM monitor_events").fetchone()[0])

    def test_old_item_is_not_sent_or_translated(self) -> None:
        old = feed_bytes(
            "Old biotech title",
            "https://example.com/old",
            "old",
            datetime.now(timezone.utc) - timedelta(hours=37),
        )
        feed = {"name": "Old Feed", "url": "https://example.com/old.xml", "enabled": True, "baseline_on_first_run": False}
        translate = AsyncMock(return_value="旧闻")
        send = AsyncMock(return_value=True)
        with patch.object(app, "cn_biomed_bot_env_configured", return_value=True), patch.object(
            app, "fetch_url", new=AsyncMock(return_value=old)
        ), patch.object(app, "translate_cn_biomed_title", new=translate), patch.object(
            app, "send_cn_biomed_notification", new=send
        ):
            self.assertEqual(0, asyncio.run(app.run_cn_biomed_feed(feed)))
        translate.assert_not_awaited()
        send.assert_not_awaited()

    def test_dedicated_notification_is_not_added_to_delete_queue(self) -> None:
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=99)))
        with patch.object(app, "role_bot_client", return_value=bot), patch.object(
            app, "cn_biomed_chat_id", return_value=-100123
        ):
            self.assertTrue(asyncio.run(app.send_cn_biomed_notification("test")))
        with app.closing(app.db()) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM monitor_messages").fetchone()[0])

    def test_cn_bot_does_not_fall_back_to_shared_token(self) -> None:
        with patch.dict(app.os.environ, {"TELEGRAM_BOT_TOKEN": "shared", "CN_BIOMED_BOT_TOKEN": ""}, clear=False):
            self.assertEqual("", app.role_bot_token(app.BOT_ROLE_CN_BIOMED))

    def test_panel_has_cn_biomed_routes(self) -> None:
        paths = {route.path for route in app.create_panel_app().routes}
        self.assertIn("/cn-biomed", paths)
        self.assertIn("/cn-biomed/settings", paths)
        self.assertIn("/cn-biomed/feed/new", paths)
        self.assertIn("/cn-biomed/feed/{idx}/preview", paths)

    def test_cn_biomed_is_last_item_in_monitor_navigation(self) -> None:
        page = app.layout("Test", "")
        monitor_nav = page.split("<section><b>监控</b>", 1)[1].split("</section>", 1)[0]
        self.assertTrue(monitor_nav.endswith("<a href='/cn-biomed'>IVD 动态</a>"))

    def test_cn_biomed_feed_table_renders_without_template_fragments(self) -> None:
        cfg = {
            "cn_biomed": {
                "enabled": True,
                "translate_titles": True,
                "max_age_hours": 36,
                "dedupe_days": 10,
                "feeds": [
                    {
                        "name": "Google News 中国 IVD",
                        "url": "https://news.google.com/rss/search?q=IVD",
                        "enabled": True,
                        "interval_seconds": 600,
                        "keywords": [],
                        "sources": [],
                    }
                ],
            }
        }
        values = {
            "CN_BIOMED_BOT_TOKEN": "token",
            "CN_BIOMED_CHAT_ID": "-100123",
            "CN_BIOMED_AI_BASE_URL": "",
            "CN_BIOMED_AI_API_KEY": "",
            "CN_BIOMED_AI_MODEL": "gpt-4o-mini",
            "CN_BIOMED_AI_INTERFACE": "responses",
            "CN_BIOMED_AI_TIMEOUT_SECONDS": "30",
        }
        panel = app.create_panel_app()
        endpoint = next(route.endpoint for route in panel.routes if route.path == "/cn-biomed")
        with patch.object(app, "cfg_load_fresh", return_value=cfg), patch.object(
            app, "env_values", return_value=values
        ), patch.object(app, "list_monitor_runtime_status", return_value={}):
            page = asyncio.run(endpoint("admin"))

        self.assertNotIn('f"', page)
        self.assertIn("class=cn-feed-table", page)
        self.assertIn("class=cn-feed-actions", page)
        self.assertIn("table-layout:fixed", page)
        self.assertEqual(5, page.split("class=cn-feed-table", 1)[1].split("</table>", 1)[0].count("<td>"))


if __name__ == "__main__":
    unittest.main()
