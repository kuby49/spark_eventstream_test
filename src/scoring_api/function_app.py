"""Azure Functions v2 HTTP entry point for per-event scoring."""

import json
import logging

import azure.functions as func

from logic import ContractError, PayloadConflict, score_request
from storage import AzureTableScoreRepository

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="score", methods=["POST"])
def score(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "The request body must be valid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        record, created = score_request(
            payload,
            req.headers.get("Idempotency-Key"),
            AzureTableScoreRepository(),
        )
        response = {
            "message_id": record.message_id,
            "run_id": record.run_id,
            "score": record.score,
        }
        return func.HttpResponse(
            json.dumps(response, separators=(",", ":")),
            status_code=201 if created else 200,
            mimetype="application/json",
        )
    except ContractError as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=400,
            mimetype="application/json",
        )
    except PayloadConflict as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=409,
            mimetype="application/json",
        )
    except Exception as exc:
        logging.error(
            "Scoring request failed; error_type=%s",
            type(exc).__name__,
        )
        return func.HttpResponse(
            json.dumps({"error": "Scoring service is temporarily unavailable"}),
            status_code=503,
            mimetype="application/json",
        )
