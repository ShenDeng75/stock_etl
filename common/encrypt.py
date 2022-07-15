import base64


# 加密
def encrypt(txt):
    return base64.b64encode(txt.encode('utf-8'))


# 解密
def decrypt(cipher):
    return base64.b64decode(cipher).decode('utf-8')


if __name__ == "__main__":
    en = encrypt('123456')
    print(en)
    de = decrypt(en)
    print(de)
