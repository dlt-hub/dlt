import uwsgi

from dlt.hub import runtime, runner


def application(env, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"pong"]


@runtime.interactive(name="webapp", port=9090, interface="rest_api")
def start():
    """will see this in runtime"""
    uwsgi.run(application, host="127.0.0.1", port=9090)


if __name__ == "__main__":
    # local testing
    start()
