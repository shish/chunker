import unittest2
from mock import patch, Mock
from chunker import util


class TestSha256(unittest2.TestCase):
    def test(self):
        self.assertEqual(
            util.sha256("hello world"),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )


class TestGetConfigPath(unittest2.TestCase):
    @patch("os.makedirs")
    def test(self, mkdir):
        self.assertEqual(
            util.get_config_path("main.json"),
            "/home/shish/.config/chunker/main.json"
        )

    @patch("os.makedirs")
    def test_not_exist(self, mkdir):
        self.assertEqual(
            util.get_config_path("waffles-do-not-exist/waffle3.dat"),
            "/home/shish/.config/chunker/waffles-do-not-exist/waffle3.dat"
        )
        self.assertEqual(mkdir.call_count, 1)


class TestLog(unittest2.TestCase):
    def test(self):
        util.log("This should have a timestamp")


class TestHeal(unittest2.TestCase):
    def test_empty_both(self):
        known = []
        missing = []
        self.assertEqual(util.heal(known, missing), -1)

    def test_empty_known(self):
        known = []
        missing = ['x']
        self.assertEqual(util.heal(known, missing), -1)

    def test_empty_missing(self):
        known = ['x']
        missing = []
        self.assertEqual(util.heal(known, missing), -1)

    def test_something_to_do(self):
        known = [Mock(id="x", length=10)]
        missing = [Mock(id="x", length=10)]
        self.assertEqual(util.heal(known, missing), 10)
        self.assertEqual(missing[0].save_data.call_count, 1)


class TestTSRound(unittest2.TestCase):
    def test(self):
        self.assertEqual(2, util.ts_round(1))
        self.assertEqual(2, util.ts_round(0.0001))
        self.assertEqual(2, util.ts_round(2))
        self.assertEqual(0, util.ts_round(0))
