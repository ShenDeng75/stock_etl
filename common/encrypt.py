import base64

import os

from Crypto.Cipher import AES
from binascii import a2b_hex, b2a_hex

utf8 = "utf-8"

# 在环境变量中设置加密的秘钥
key = iv = os.environ['ENCRYPT_KEY'].encode(utf8)


# 加密
def encrypt(txt):
    aes = AES.new(key, AES.MODE_CBC, iv)
    txt_en = txt.encode(utf8)
    txt_mid = txt_en + (16 - len(txt_en) % 16) * '\0'.encode(utf8)
    txt_res = aes.encrypt(txt_mid)
    return str(base64.b64encode(b2a_hex(txt_res)), encoding=utf8)


# 解密
def decrypt(txt):
    txt_de = base64.b64decode(txt)
    aes = AES.new(key, AES.MODE_CBC, iv)
    txt_res = aes.decrypt(a2b_hex(txt_de))
    return bytes.decode(txt_res).rstrip('\0')


if __name__ == "__main__":
    en = encrypt('123456')
    print(en)
    de = decrypt(en)
    print(de)
