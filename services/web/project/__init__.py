from flask import jsonify, request, Response, send_from_directory
import threading
from werkzeug.exceptions import BadRequest

from .app import app
from .auditor import Auditor
from .coupler import COUPLER
from .logger import DEBUG_ROUTE, timeit, version
from .validators import DemographicsGetValidator


@app.route("/")
def hello_world():
    return jsonify(hello="world")


@app.route("/static/<path:filename>")
def staticfiles(filename: str) -> Response:
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


def get(payload: dict, endpoint: str) -> list:
    return COUPLER['query_records']['processor'](payload, endpoint=endpoint)


def post(payload: dict, endpoint: str) -> dict:
    user = payload['user']
    processor = COUPLER[endpoint]['processor']
    with Auditor(user, version, endpoint) as job_auditor:
        thread = threading.Thread(
            target=processor,
            args=(payload, job_auditor)
        )
        thread.start()
    try:
        batch_key = job_auditor.batch_id
        response = 200
    except AttributeError:
        batch_key = None
        response = 405
    response = {
        "batch_key": batch_key,
        "status": response
    }

    return response


@timeit
def process_payload():
    response = None
    endpoint = str(request.endpoint)
    method = request.method
    validator = COUPLER[endpoint]['validator']()
    if endpoint == 'demographic' and method == 'GET':
        validator = DemographicsGetValidator()
    try:
        payload_obj = request.get_json()
    except BadRequest as e:
        print(f"Request is not acceptable JSON: {e}", file=DEBUG_ROUTE)
        return jsonify(status=405, response=response)
    result, msg = validator.validate(payload_obj)
    if result:
        if method == "GET":
            response = get(payload_obj, endpoint)
        elif method == "POST":
            response = post(payload_obj, endpoint)
    else:
        print(f"Invalid request payload: {msg}", file=DEBUG_ROUTE)
        return jsonify(status=405, response=msg)
    if response is not None:
        return jsonify(status=200, response=response)
    else:
        print(
            f"Issue encountered with {method} request",
            file=DEBUG_ROUTE
        )
        return jsonify(status=405, response=response)


for end_point, couplings in COUPLER.items():
    app.add_url_rule(
        f'/api_{version}/{end_point}',
        endpoint=end_point,
        view_func=process_payload,
        methods=couplings['methods']
    )
