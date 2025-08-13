# Valvoline PDS API

FastAPI service that reads Valvoline (and competitor) PDS PDFs and returns:
- Approvals & Specifications
- Typical properties (order preserved)
- Norwegian summary or side-by-side comparison

## Endpoints
- GET /health
- POST /upload (multipart/form-data file=PDF) -> { stored_as: "data/<id>.pdf" }
- POST /answer
  - Body (summary):
    {
      "product_a_file": "data/<id>.pdf",
      "expected_output": "summary",
      "locale": "no"
    }
  - Body (comparison):
    {
      "product_a_file": "data/<idA>.pdf",
      "product_b_file": "data/<idB>.pdf",
      "expected_output": "comparison",
      "locale": "no"
    }
  - You can also use product_a_url / product_b_url for direct .pdf links.
