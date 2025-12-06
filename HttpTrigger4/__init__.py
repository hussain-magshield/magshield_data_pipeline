import json
import azure.functions as func
from modules.callable import final4

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        final4()
        body = json.dumps({"status": "success", "message": " Function executed successfully"})
        return func.HttpResponse(body, status_code=200, mimetype="application/json")
    except Exception as e:
        error_body = json.dumps({"status": "error", "message": str(e)})
        return func.HttpResponse(error_body, status_code=500, mimetype="application/json")
