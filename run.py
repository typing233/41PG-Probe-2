#!/usr/bin/env python3
import uvicorn
import yaml
import sys
from pathlib import Path


def main():
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("ERROR: config.yaml not found in current directory")
        sys.exit(1)

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    server = cfg.get("server", {})
    uvicorn.run(
        "app.main:app",
        host=server.get("host", "0.0.0.0"),
        port=server.get("port", 8080),
        reload=server.get("debug", False),
        log_level="info",
    )


if __name__ == "__main__":
    main()
