import unittest2

from chunker import args


class TestArgs(unittest2.TestCase):
    def setUp(self):
        self.wap = args.WebArgumentParser()
        self.wap.add_argument("cmd", type=str, default="subcommand")
        self.wap.add_argument("--param", type=str, default="hello")
        self.wap.add_argument("--flag", action="store_true", default=False)
        self.wap.add_argument("--num", type=int, default=0)

    def test_command(self):
        url = "/some-command"
        self.assertEqual(
            self.wap.url_to_args(url),
            ["some-command"]
        )
        self.assertDictEqual(
            vars(self.wap.parse_args(url)),
            {"cmd": "some-command", "flag": False, "num": 0, "param": "hello"}
        )

    def test_string(self):
        url = "/some-command?param=foo"
        self.assertEqual(
            self.wap.url_to_args(url),
            ["some-command", "--param", "foo"]
        )
        self.assertDictEqual(
            vars(self.wap.parse_args(url)),
            {"cmd": "some-command", "param": "foo", "flag": False, "num": 0}
        )

    def test_bool(self):
        url = "/some-command?flag=on"
        self.assertEqual(
            self.wap.url_to_args(url),
            ["some-command", "--flag"]
        )
        self.assertDictEqual(
            vars(self.wap.parse_args(url)),
            {"cmd": "some-command", "param": "hello", "flag": True, "num": 0}
        )
