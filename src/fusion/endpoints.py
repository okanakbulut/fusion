from typing import ClassVar

from fusion.di import Injectable, inject


class HttpEndpoint(Injectable):
    methods: ClassVar[list[str]]

    def __init_subclass__(cls, *args, **kwargs):
        cls.methods = []
        for http_method in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
            method = getattr(cls, http_method.lower(), None)
            if callable(method):
                cls.methods.append(http_method)
                setattr(cls, http_method.lower(), inject(method))

        super().__init_subclass__(*args, **kwargs)
