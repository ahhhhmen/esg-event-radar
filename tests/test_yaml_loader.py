"""
tests/test_yaml_loader.py
验证 Phase 0 config/sources.yaml 可成功加载、解析并通过 schema 校验。
"""
import sys
import os
import unittest
import yaml

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config",
    "sources.yaml",
)

REQUIRED_FIELDS = {"id", "type", "query", "time_window", "limit", "enabled"}
VALID_TYPES = {"google_news_rss"}


class TestYAMLLoader(unittest.TestCase):
    """验证 config/sources.yaml 的结构与字段完整性。"""

    @classmethod
    def setUpClass(cls):
        """加载 YAML 文件。"""
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            cls.config = yaml.safe_load(fh)

    def test_file_exists(self):
        """文件存在且可读。"""
        self.assertTrue(os.path.isfile(CONFIG_PATH),
                        f"config/sources.yaml 不存在于 {CONFIG_PATH}")

    def test_top_level_sources_key(self):
        """顶层必须包含 'sources' 键。"""
        self.assertIn("sources", self.config,
                      "YAML 根缺少 'sources' 键")
        self.assertIsInstance(self.config["sources"], list,
                              "'sources' 值必须是列表")

    def test_sources_not_empty(self):
        """sources 列表不能为空。"""
        self.assertGreater(len(self.config["sources"]), 0,
                           "'sources' 列表为空，请配置至少一个数据源")

    def test_each_source_has_required_fields(self):
        """每个条目必须包含所有必填字段。"""
        for i, source in enumerate(self.config["sources"]):
            missing = REQUIRED_FIELDS - set(source.keys())
            self.assertEqual(
                len(missing), 0,
                f"源 #{i} (id='{source.get('id', '?')}') 缺少必填字段: {missing}",
            )

    def test_each_source_no_extra_fields(self):
        """每个条目不应包含未定义的额外字段。"""
        allowed_fields = REQUIRED_FIELDS
        for i, source in enumerate(self.config["sources"]):
            extra = set(source.keys()) - allowed_fields
            self.assertEqual(
                len(extra), 0,
                f"源 #{i} (id='{source.get('id', '?')}') 包含未定义字段: {extra}",
            )

    def test_id_is_unique(self):
        """id 字段必须唯一。"""
        ids = [s["id"] for s in self.config["sources"]]
        self.assertEqual(
            len(ids), len(set(ids)),
            f"发现重复的 id: {[x for x in ids if ids.count(x) > 1]}",
        )

    def test_type_is_valid(self):
        """type 字段必须是支持的类型。"""
        for i, source in enumerate(self.config["sources"]):
            self.assertIn(
                source["type"], VALID_TYPES,
                f"源 #{i} (id='{source['id']}') type='{source['type']}' 无效，"
                f"支持: {VALID_TYPES}",
            )

    def test_query_is_non_empty(self):
        """query 字段不能为空字符串。"""
        for i, source in enumerate(self.config["sources"]):
            q = source["query"].strip()
            self.assertGreater(
                len(q), 0,
                f"源 #{i} (id='{source['id']}') query 为空",
            )

    def test_time_window_positive(self):
        """time_window 必须为正整数。"""
        for i, source in enumerate(self.config["sources"]):
            tw = source["time_window"]
            self.assertIsInstance(tw, int,
                                  f"源 #{i} (id='{source['id']}') time_window 必须是 int，得到 {type(tw)}")
            self.assertGreater(tw, 0,
                               f"源 #{i} (id='{source['id']}') time_window={tw} 必须 > 0")

    def test_limit_non_negative(self):
        """limit 必须 >= 0。"""
        for i, source in enumerate(self.config["sources"]):
            lim = source["limit"]
            self.assertIsInstance(lim, int,
                                  f"源 #{i} (id='{source['id']}') limit 必须是 int，得到 {type(lim)}")
            self.assertGreaterEqual(lim, 0,
                                    f"源 #{i} (id='{source['id']}') limit={lim} 必须 >= 0")

    def test_enabled_is_boolean(self):
        """enabled 必须是布尔值。"""
        for i, source in enumerate(self.config["sources"]):
            self.assertIsInstance(
                source["enabled"], bool,
                f"源 #{i} (id='{source['id']}') enabled 必须是 bool，得到 {type(source['enabled'])}",
            )

    def test_at_least_one_enabled(self):
        """至少有一个源处于 enabled: true。"""
        enabled_count = sum(1 for s in self.config["sources"] if s["enabled"])
        self.assertGreater(
            enabled_count, 0,
            "所有源均为 enabled: false，请至少激活一个供料源",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)