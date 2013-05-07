import unittest2
import os
import json

from chunker.repo import Repo

class RepoTests(unittest2.TestCase):
    def setUp(self):
        file("/tmp/test-repo.chunker", "w").write(json.dumps(
            {
                "name": "My Test Repo",
                "type": "static",
                "files": {
                    "hello1.txt": {
                        "versions": [{
                            "chunks": [
                                {
                                    "hash_type": "md5",
                                    "hash": "5a8dd3ad0756a93ded72b823b19dd877",
                                    "length": 6,
                                }
                            ],
                            "timestamp": 0,
                            "deleted": False,
                            }],
                    },
                    "hello2.txt": {
                        "versions": [{
                            "chunks": [
                                {
                                    "hash_type": "md5",
                                    "hash": "5a8dd3ad0756a93ded72b823b19dd877",
                                    "length": 6,
                                }
                            ],
                            "timestamp": 0,
                            "deleted": False,
                        }],
                    }
                }
            }
        , indent=4))

        if not os.path.exists("/tmp/test-repo"):
            os.makedirs("/tmp/test-repo")

        file("/tmp/test-repo/hello1.txt", "w").write("hello!")

    def tearDown(self):
        files = [
            "/tmp/test-repo/hello1.txt",
            "/tmp/test-repo/hello2.txt",
            "/tmp/test-repo",
            "/tmp/test-repo.chunker",
        ]
        for fn in files:
            if os.path.isdir(fn):
                os.rmdir(fn)
            if os.path.exists(fn):
                os.unlink(fn)

    def testCreateFromFile(self):
        r = Repo("/tmp/test-repo.chunker", "/tmp/test-repo")

    def testSelfHeal(self):
        r = Repo("/tmp/test-repo.chunker", "/tmp/test-repo")
        print r.get_known_chunks()
        print r.get_missing_chunks()
        self.assertEqual(1, len(r.get_known_chunks()))
        self.assertEqual(1, len(r.get_missing_chunks()))
        r.self_heal()
        self.assertEqual(2, len(r.get_known_chunks()))
        self.assertEqual(0, len(r.get_missing_chunks()))

    def testSaveState(self):
        r = Repo("/tmp/test-repo.chunker", "/tmp/test-repo")
        r.save_state()

