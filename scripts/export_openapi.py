import json
from pathlib import Path

from newsvine_api.main import app


def main() -> None:
    output_path = Path("docs/openapi/openapi.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema = app.openapi()
    output_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
