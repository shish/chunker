from datetime import datetime

def log(msg):
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg
