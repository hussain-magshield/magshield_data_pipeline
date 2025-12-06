# import json
# import azure.functions as func
# from modules.callable import final3

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         final3()
#         body = json.dumps({"status": "success", "message": " Function executed successfully"})
#         return func.HttpResponse(body, status_code=200, mimetype="application/json")
#     except Exception as e:
#         error_body = json.dumps({"status": "error", "message": str(e)})
#         return func.HttpResponse(error_body, status_code=500, mimetype="application/json")
# import json
# import azure.functions as func
# from modules.callable import final2

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         final2()
#         body = json.dumps({"status": "success", "message": " Function executed successfully"})
#         return func.HttpResponse(body, status_code=200, mimetype="application/json")
#     except Exception as e:
#         error_body = json.dumps({"status": "error", "message": str(e)})
#         return func.HttpResponse(error_body, status_code=500, mimetype="application/json")
import json
import azure.functions as func
import threading
from modules.callable import final3

def run_background():
    try:
        final3()   # <-- long job runs here (20 min allowed)
    except Exception as e:
        # optionally log to Application Insights
        print(f"Background error: {e}")

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Start long-running job in background
    threading.Thread(target=run_background).start()

    # Return immediately (NON-BLOCKING)
    response = {
        "status": "started",
        "message": "Background export job started successfully"
    }

    return func.HttpResponse(
        json.dumps(response),
        status_code=202,
        mimetype="application/json"
    )
