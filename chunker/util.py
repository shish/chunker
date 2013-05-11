from datetime import datetime
import os
import math
import sys
import hashlib


def sha256(data):
    return hashlib.sha256(str(data)).hexdigest()


def get_config_path(filename):
    filepath = os.path.join(os.path.expanduser("~/.config/chunker/"), filename)
    dirpath = os.path.dirname(filepath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    return filepath


def log(msg):
    sys.stderr.write("%s %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))


def heal(known_chunks, missing_chunks):
    # this could be much more efficient by sorting the lists
    # and runningn along both of them at once
    #   O(N*M) -> O(max(N, M))
    if known_chunks and missing_chunks:
        log("Attempting self-healing")
        saved = 0
        for known_chunk in known_chunks:
            for missing_chunk in missing_chunks:
                if known_chunk.id == missing_chunk.id:
                    log("Copying chunk from %s to %s" % (known_chunk.file.filename, missing_chunk.file.filename))
                    missing_chunk.save_data(known_chunk.get_data())
                    saved = saved + known_chunk.length
        return saved
    else:
        log("Can't self-heal (%d known vs %d unknown)" % (len(known_chunks), len(missing_chunks)))
        return -1


def ts_round(time):
    """
    FAT32 rounds up to the next multiple of 2 whole seconds, making it the
    least accurate filesystem I know. Since differences of a couple of
    seconds either way are tolerable, let's handle the lowest common
    denominator

    >>> ts_round(0.0)
    0
    >>> ts_round(0.1)
    2
    >>> ts_round(1.9)
    2
    """
    inttime = int(math.ceil(time))
    if inttime % 2 == 1:
        inttime = inttime + 1
    return inttime

