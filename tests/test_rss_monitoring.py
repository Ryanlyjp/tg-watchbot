import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import app


def rss_bytes(items: list[tuple[str, str, str]]) -> bytes:
    rows = "".join(
        f"<item><title>{title}</title><link>{link}</link><guid>{guid}</guid>"
        f"<description>{title}</description></item>"
        for title, link, guid in items
    )
    return f"<?xml version='1.0' encoding='utf-8'?><rss version='2.0'><channel><title>Test</title>{rows}</channel></rss>".encode()


class RssParsingTest(unittest.TestCase):
    def test_exclude_keywords_match_title_and_content_only(self) -> None:
        item = app.MonitorItem(
            key="1",
            title="普通标题",
            link="https://example.com/1",
            text="正文包含 PROMOTION 内容",
            author="屏蔽作者",
            category="屏蔽分类",
        )

        blocked, reason = app.item_blocked(item, {"exclude_keywords": ["promotion"]})
        self.assertTrue(blocked)
        self.assertIn("promotion", reason)

        blocked, _ = app.item_blocked(item, {"exclude_keywords": ["屏蔽作者", "屏蔽分类"]})
        self.assertFalse(blocked)

    def test_gbk_feed_bytes_are_decoded_by_feedparser(self) -> None:
        xml = (
            "<?xml version='1.0' encoding='gbk'?><rss version='2.0'><channel>"
            "<title>吾爱破解</title><item><title>中文标题</title>"
            "<link>https://www.52pojie.cn/forum.php?mod=viewthread&amp;tid=123</link>"
            "<guid>123</guid><description>中文内容</description><author>测试作者</author>"
            "</item></channel></rss>"
        ).encode("gbk")

        items = app.parse_rss_items({"type": "rss", "url": "https://www.52pojie.cn/"}, xml)

        self.assertEqual("中文标题", items[0].title)
        self.assertEqual("测试作者", items[0].author)
        self.assertEqual("123", items[0].key)

    def test_block_pages_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "访问频率限制页"):
            app.parse_web_items(
                {"url": "https://hostloc.com/", "selectors": {}},
                "<html><div id='messagetext'>休息下，一会见</div></html>".encode(),
            )

    def test_forum_keys_cover_supported_platforms(self) -> None:
        cases = {
            "https://www.deepflood.com/post-38506-1": "38506",
            "https://www.dalao.net/thread-12345-1-1.html": "12345",
            "https://www.52pojie.cn/forum.php?mod=viewthread&tid=2117828": "2117828",
            "https://www.v2ex.com/t/1228254#reply13": "1228254",
            "https://ruby-china.org/topics/44616": "44616",
        }
        for link, expected in cases.items():
            with self.subTest(link=link):
                self.assertEqual(expected, app.canonical_forum_key(link))


class MonitorBaselineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.old_db_path = app.DB_PATH
        self.old_config = app.config
        app.DB_PATH = Path(self.temp_dir.name) / "test.sqlite3"
        app.config = {"http": {"timeout_seconds": 5}}
        app.init_db()

    def tearDown(self) -> None:
        app.DB_PATH = self.old_db_path
        app.config = self.old_config
        self.temp_dir.cleanup()

    def test_first_run_builds_baseline_without_notifications(self) -> None:
        monitor = app.rss_forum_template("测试论坛", "https://example.com/feed.xml", 60)
        first = rss_bytes([("Codex 旧帖", "https://example.com/t/100", "100")])
        second = rss_bytes(
            [
                ("Codex 新帖", "https://example.com/t/101", "101"),
                ("Codex 旧帖", "https://example.com/t/100", "100"),
            ]
        )

        with patch.object(app, "fetch_url", new=AsyncMock(side_effect=[first, second])), patch.object(
            app, "admin_send_monitor", new=AsyncMock(return_value=True)
        ) as send:
            self.assertEqual(0, asyncio.run(app.run_monitor(monitor)))
            self.assertEqual(1, asyncio.run(app.run_monitor(monitor)))

        self.assertEqual(1, send.await_count)
        self.assertIn("Codex 新帖", send.await_args.args[0])

    def test_blocked_item_is_remembered_and_not_replayed_later(self) -> None:
        monitor = app.rss_forum_template("过滤测试", "https://example.com/feed.xml", 60)
        monitor["baseline_on_first_run"] = False
        monitor["exclude_keywords"] = ["广告"]
        feed = rss_bytes([("Codex 广告帖", "https://example.com/t/200", "200")])

        with patch.object(app, "fetch_url", new=AsyncMock(side_effect=[feed, feed])), patch.object(
            app, "admin_send_monitor", new=AsyncMock(return_value=True)
        ) as send:
            self.assertEqual(0, asyncio.run(app.run_monitor(monitor)))
            monitor["exclude_keywords"] = []
            self.assertEqual(0, asyncio.run(app.run_monitor(monitor)))

        self.assertEqual(0, send.await_count)
        self.assertTrue(app.monitor_has_state("过滤测试"))


if __name__ == "__main__":
    unittest.main()
