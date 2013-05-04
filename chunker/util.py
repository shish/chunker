from datetime import datetime
import os


config_dir = os.path.expanduser("~/.config/chunker")


def _mkconfigdir():
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)


def get_config_path(filename):
    _mkconfigdir()
    return os.path.join(os.path.expanduser("~/.config/chunker/"), filename)


def log(msg):
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg


def heal(known_chunks, missing_chunks):
    if known_chunks and missing_chunks:
        log("Attempting self-healing")
        for known_chunk in known_chunks:
            for missing_chunk in missing_chunks:
                if known_chunk.id == missing_chunk.id:
                    log("Copying chunk from %s to %s" % (known_chunk.file.filename, missing_chunk.file.filename))
                    missing_chunk.save_data(known_chunk.get_data())

