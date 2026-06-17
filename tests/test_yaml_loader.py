"""
tests/test_yaml_loader.py
验证 sources.yaml（仓库根）可成功加载、解析并通过 schema 校验。
"""
import sys
import os
import unittest
import yaml

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sources.yaml",
)

REQUIRED_FIELDS = {"name", "url", "type", "active"}
VALID_TYPES = {"rss", "google_news", "html_calendar"}
VALID_REGIONS = {"global", "apac", "emea", "americas", "cn"}


class TestYAMLLoader(unittest.TestCase):
    """验证 sources.yaml 的结构与字段完整性。"""

    @classmethod
    def setUpClass(cls):
        """加载 YAML 文件。"""
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            cls.config = yaml.safe_load(fh)

    def test_file_exists(self):
        """文件存在且可读。"""
        self.assertTrue(os.path.isfile(CONFIG_PATH),
                        f"sources.yaml 不存在于 {CONFIG_PATH}")

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
        """每个条目必须包含必填字段 (name/url/type/active)。"""
        for i, source in enumerate(self.config["sources"]):
            missing = REQUIRED_FIELDS - set(source.keys())
            self.assertEqual(
                len(missing), 0,
                f"源 #{i} (name='{source.get('name', '?')}') 缺少必填字段: {missing}",
            )

    def test_name_is_unique(self):
        """name 字段必须唯一。"""
        names = [s["name"] for s in self.config["sources"]]
        duplicates = {n for n in names if names.count(n) > 1}
        self.assertEqual(
            len(duplicates), 0,
            f"发现重复的 name: {duplicates}",
        )

    def test_type_is_valid(self):
        """type 字段必须是支持的类型。"""
        for i, source in enumerate(self.config["sources"]):
            self.assertIn(
                source["type"], VALID_TYPES,
                f"源 #{i} (name='{source['name']}') type='{source['type']}' 无效，"
                f"支持: {VALID_TYPES}",
            )

    def test_url_is_non_empty(self):
        """url 字段不能为空。"""
        for i, source in enumerate(self.config["sources"]):
            url = source.get("url", "").strip()
            self.assertGreater(
                len(url), 0,
                f"源 #{i} (name='{source['name']}') url 为空",
            )

    def test_active_is_boolean(self):
        """active 必须是布尔值。"""
        for i, source in enumerate(self.config["sources"]):
            self.assertIsInstance(
                source.get("active", True), bool,
                f"源 #{i} (name='{source['name']}') active 必须是 bool，"
                f"得到 {type(source.get('active'))}",
            )

    def test_at_least_one_active(self):
        """至少有一个源 active: true。"""
        active_count = sum(1 for s in self.config["sources"] if s.get("active", True))
        self.assertGreater(
            active_count, 0,
            "所有源均为 active: false，请至少激活一个供料源",
        )

    def test_region_valid_if_present(self):
        """如提供 region，则必须是有效值。"""
        for i, source in enumerate(self.config["sources"]):
            region = source.get("region")
            if region is not None:
                self.assertIn(
                    region, VALID_REGIONS,
                    f"源 #{i} (name='{source['name']}') region='{region}' 无效，"
                    f"支持: {VALID_REGIONS}",
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
