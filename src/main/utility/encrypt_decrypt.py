import base64

from resource.dev import  config
from Cryptodome.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
import os, sys


try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception(F"Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"error occurred. Details:{e}")
    sys.exit(0)

BS = 16
pad = lambda s: bytes(s + (BS-len(s) % BS) * chr (BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]



def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, salt, 64, 1000)
    key32 = kdf[:32]
    return key32

def encrypt(raw):
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))



def decrypt(enc):
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')

