import chardet

def safe_read_text(file):
    data = file.read()
    if isinstance(data, bytes):
        enc = chardet.detect(data)["encoding"] or "utf-8"
        return data.decode(enc, errors="replace").splitlines()
    return data.splitlines()
