"""Stand-in for the OpenAI API so the n8n integration test needs no API key or model.

Embeddings are deterministic and word based: each word hashes to a fixed direction and a text's
vector is the normalized sum, so texts sharing words land close together and a similarity
search for "cats" returns the cat document.

Chat completions are canned: when tools are offered and none has been called yet, the reply
calls the first tool with "cats" for every string argument; otherwise the reply repeats the
last tool result (or the last user message) so the test can see what flowed through.
"""

import base64
import hashlib
import json
import math
import struct
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

DIMENSIONS = 32


def word_vector(word):
    digest = hashlib.sha256(word.lower().encode()).digest()
    return [(b - 128) / 128 for b in digest[:DIMENSIONS]]


def embed(text):
    total = [0.0] * DIMENSIONS
    for word in text.split():
        for i, v in enumerate(word_vector(word)):
            total[i] += v
    norm = math.sqrt(sum(v * v for v in total)) or 1.0
    return [v / norm for v in total]


def embeddings(body):
    inputs = body.get("input", [])
    if isinstance(inputs, str):
        inputs = [inputs]
    # The OpenAI client asks for base64 unless told otherwise and decodes float32 little endian
    if body.get("encoding_format") == "base64":
        encode = lambda v: base64.b64encode(struct.pack("<%df" % len(v), *v)).decode()
    else:
        encode = lambda v: v
    data = [{"object": "embedding", "index": i, "embedding": encode(embed(t))} for i, t in enumerate(inputs)]
    return {"object": "list", "data": data, "model": body.get("model", "fake"), "usage": {"prompt_tokens": 0, "total_tokens": 0}}


def tool_call_arguments(tool):
    properties = tool.get("function", {}).get("parameters", {}).get("properties", {})
    args = {}
    for name, schema in properties.items():
        kind = schema.get("type", "string")
        args[name] = "cats" if kind == "string" else 1 if kind in ("number", "integer") else True if kind == "boolean" else {}
    return args


def chat_message(body):
    messages = body.get("messages", [])
    tools = body.get("tools") or []
    already_called = any(m.get("role") == "tool" for m in messages)
    if tools and not already_called:
        tool = tools[0]
        return {
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "id": "call_fake_1",
                "type": "function",
                "function": {"name": tool["function"]["name"], "arguments": json.dumps(tool_call_arguments(tool))},
            }],
        }, "tool_calls"
    last_tool = next((m for m in reversed(messages) if m.get("role") == "tool"), None)
    last_user = next((m for m in reversed(messages) if m.get("role") == "user"), None)
    source = last_tool or last_user or {"content": ""}
    content = source.get("content")
    if not isinstance(content, str):
        content = json.dumps(content)
    return {"role": "assistant", "content": "Tool result: " + content[:2000]}, "stop"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or b"{}"))
        if self.path.endswith("/embeddings"):
            return self.reply(embeddings(body))
        if self.path.endswith("/chat/completions"):
            message, finish_reason = chat_message(body)
            if body.get("stream"):
                return self.stream(body, message, finish_reason)
            return self.reply({
                "id": "chatcmpl-fake", "object": "chat.completion", "created": int(time.time()), "model": body.get("model", "fake"),
                "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            })
        self.reply({"error": {"message": "unknown endpoint " + self.path}}, 404)

    def do_GET(self):
        self.reply({"object": "list", "data": [{"id": "fake-model", "object": "model"}]})

    def stream(self, body, message, finish_reason):
        delta = dict(message)
        if delta.get("tool_calls"):
            delta["tool_calls"] = [dict(c, index=i) for i, c in enumerate(delta["tool_calls"])]
        chunks = [
            {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}]},
        ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for chunk in chunks:
            chunk.update({"id": "chatcmpl-fake", "object": "chat.completion.chunk", "created": int(time.time()), "model": body.get("model", "fake")})
            self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
        self.wfile.write(b"data: [DONE]\n\n")

    def reply(self, payload, status=200):
        out = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
