from __future__ import annotations
import json
from importlib.resources import files
from typing import Any, Dict, List, Optional
from fastmcp import FastMCP
from fastmcp.prompts.prompt import PromptMessage, TextContent

PROMPT_PKG = "graph_service_mcp.assets.prompts"
PROMPT_MAP = {}

def _load_template(name: str) -> str:
    return files(PROMPT_PKG).joinpath(name).read_text(encoding="utf-8")


def _j(v):
    return json.dumps(v, ensure_ascii=False)


def get_prompt_map():
    return PROMPT_MAP


def register(mcp: FastMCP, *_deps) -> None:
    return;