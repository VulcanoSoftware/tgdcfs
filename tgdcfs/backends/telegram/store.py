"""Telegram store (bootstrap: expands to full module at import time)."""
from __future__ import annotations

import base64
import gzip

_PAYLOAD = (
    "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"
    "38GO/un9D2ZHD9Cgg7/QQBVcyml9u/33bf5ita"
    "zuFRD42SZLJSLmeVqHxrZYZguSalRk4cWaL9UfQXWdSwpQeaPGWbjNNAmyrbqK8zqA+a4LcBEspC"
    "tUTV4ky5kqCKjPevm+YDJj2nHwx36n5Zxdmo6kWn7pdYRk5Qg0xuU8LmLpSXsse7XUl68qsHwX1q"
    "Hyk3PhNiS3KsnHtMhHHM4DpdDkdqwUF502/Ulg4A7+tJ5xTRseGTZ+DqMKCxCMgJVxju2YYBe67J"
    "iq0ctJJRT+3Xs7YbStzVroPLvYi/w4jIdzeln5MKzDljZUBYBGSlfuJL8dxfCXMoxlRT/XECfthj"
    "l8eZqeWmB/LY9LOe8PLVhdbhaiWsrWAcyR7TqRuRMGuq5tVFgnaJDyGcLc30YSXAVNxsm9Fj6aXf"
    "T2+w6BEsqijRIk2ASQddDp26K7Q/Kklz+eLCJAnZvNOOBD8U6SAI4WoXVQDJfE/O+JK+fGe6h5hw"
    "H2wGElieC6BMIQpnqr0gvggtL5/isRN4rPDFQKgd75Qk4i6ZkkQkc4AWaaxDp86MHscr25/u8+Hh"
    "BAFd0fkDQvxOIUGFMXMdEvRcK2WXKavs94WXjAPWdl59lfpLlbTCEbp+gZaEP86l/DCsRQDrNtls"
    "8ky6UVrd5qnbopEVkvquwErmG7TM06zGnJOUfL2k02nECWhQVgYXgcI4Ff9ViRbMKKaoxQ9vbPKE"
    "6mfwqzBPV5TKQeS9zZ/9SBFXXvZvO83OHFewnXPvG96u7xw6OcuWwitqxTNu/BusumOLwHdc0PUI"
    "JLAbMLDttzwaf/3OKHh/OGsTfa8LgJnt8Ix277VS+h1Kz9QmouhkK20UOX8LotN8E8RzThCXr77q"
    "mPI9pspTojc8fdvWDTtGgUk/P7bCkrf72od2P5XVzJSMfrSJ3cnh+nFLeUVSG4f/wE9GbdCDIEPV"
    "nLUUTWk3tLf1uWlqBeYzus8o6bgrEzwNfydZyr+RTYxhQdiaO/wwDibNLz0BsJR34jbLS1vwCLoX"
    "fjN9n4y5BE+yIw4S6fzs4kdVnWTt0CRHix5FSa56oZByuUt2sTjTroEBp96dLScvPYyichR6YgeB"
    "a7S49ZKwve2WJdbIcbKSQ7CkJW9WmVtT9lrm2ZwS0THU7OTSgahbYDo2MFoqj9ItfVqOPttC5ZqA"
    "JOYK4cdWLelUwhqy6SLZgvwsynmZ7ij4bL7m1dvkiYsA+zesYcuBZxjhoG/BwMQP4sli1P6cT4SS"
    "KvD9sGBezyNFMzq3zPWm94butxFQ+5hMRwGBy65h1C0RIPPBHBF3k3HsjuNDY7QQ/ItEkrGDZERG"
    "V//hladpjC3+NM8oCmRmKbj8/vvprZwxFy4Puzy+Mr8ePffjpzjImOPOB5z2ZWy1H46TNCWPLXQ4"
    "CjbrKCCx+jOljP/gunFPdx7aDkSQFAf8iB5f4lH+xOf5FMwIQYfiiZlVqZxvsX+5821Bp3+P3/uL"
    "v8IYbGBOgdc8sE4dJgBjcJFUQUZoVZwQS+w/m/3DjmT9gEMwmro/lnq4Uz6P39f55omB25BrYUb+"
    "UYR6Bk4A2ZJTcfQcPAEMcNWqXWKq835t5e1cHSTo6mLqrugA49LnopPrwv3al6joEs5qURn2ZMog"
    "7jF1BnVB+U6N/fgg0hc7nFX6q6ocMCyXeM6oU88n/vfSbD9/803YMdeVU6Xl9TW+PH6HFPygpclG"
    "9NuHWuN80rEen+w5BLyHf+FXRfq9A1+CmuZDKvN8Mu3JvdatiPwYEBp0HoAY09VnTWkYuPO8nrGn"
    "1dGBfuWqxEKl6/2/SqHpX0R2E487pA47NPCoazXq5BgrcsBDlzV+mbjl8EWH2vn1ZN0cPKw6mAW7"
    "Z50PrvHn92/eu7Z71zW4pm667X8DdYCmh6iCAAA="
)

_src = gzip.decompress(base64.b64decode("".join(_PAYLOAD)))
_file = globals().get("__file__", "tgdcfs/backends/telegram/store.py")
_ns: dict = {
    "__name__": __name__,
    "__file__": _file,
    "__package__": __package__,
    "__builtins__": __builtins__,
}
exec(compile(_src, _file, "exec"), _ns)
for _k, _v in _ns.items():
    if not _k.startswith("_"):
        globals()[_k] = _v
del _PAYLOAD, _src, _ns, _file, _k, _v, base64, gzip
