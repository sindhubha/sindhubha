import time
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware


class RequestDurationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start_time = time.perf_counter()
        response = await call_next(request)
        end_time = time.perf_counter()
        duration_seconds = end_time - start_time

        duration_seconds = round(duration_seconds, 2)  # Round to two decimal places

        response.headers["Request-Duration"] = f"{duration_seconds:.2f} seconds"
        return response


def setup_middleware(app: FastAPI):
    app.add_middleware(RequestDurationMiddleware)