# CONN-KAFKA-MCP-CONNECTOR-CONTENT — Requirements

Governing spec: [spec.md](spec.md).

Each row is one requirement. A row names the file it touches and the test that proves it. The generator sets each row's state in `status.json`.

| ID | Requirement | Acceptance |
|---|---|---|
| `CONN-KAFKA-MCP-CONNECTOR-CONTENT-R001` | **Default content.** `pyproject.toml` declares `kafka-mcp` as the single `scion_sdk.connector` entry point. The server lists the packaged `skill://` and `ontology://` resources and the packaged prompts with no `content=` argument. Proving test: `tests/test_default_content_surfaces.py`. Caller chain: the kafka-mcp console script (kafka_mcp.mcp_server:mcp_server, kafka_mcp/mcp_server.py:94) calls get_mcp_instance (mcp_server.py:36), whose scion_sdk.connector.build_server loads kafka_mcp:connector (kafka_mcp/__init__.py:90) and serves the packaged skills, ontology and prompts. Shape: Connector in kafka_mcp/__init__.py passes no content= argument; default content comes from kafka_mcp/skills, ontology and prompts, verified by tests/test_default_content_surfaces.py which checks skill://, ontology://kafka-mcp/ and each prompt. | `resources/list` includes `skill://` and `ontology://`; `prompts/list` includes each packaged prompt. Test: `tests/test_default_content_surfaces.py::test_default_content_surfaces`. |
