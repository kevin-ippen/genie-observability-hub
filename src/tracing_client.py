"""
Genie Traced Client — Pattern 4: Full Visibility Tracing

Bypasses the Genie SDK and calls the REST API directly, logging every
state transition to MLflow with full payloads, timing, generated SQL,
status codes, and errors.

Usage (in a notebook or app):
    from tracing_client import GenieTracedClient

    client = GenieTracedClient(space_id="your-space-id")
    result = client.ask("What were total sales last quarter?")
    # → Full conversation logged to MLflow experiment with per-state spans

Requires: databricks-sdk>=0.40.0, mlflow>=2.18.0
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional

import mlflow
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@dataclass
class GenieResponse:
    """Structured response from a Genie conversation."""
    message_id: str
    conversation_id: str
    space_id: str
    status: str
    content: Optional[str] = None
    generated_sql: Optional[str] = None
    state_transitions: list = field(default_factory=list)
    error: Optional[str] = None
    duration_ms: int = 0
    attachments: list = field(default_factory=list)


class GenieTracedClient:
    """
    MLflow-traced Genie REST API client.

    Each call to ask() creates a parent MLflow span with child spans for
    each observed state transition (asking_ai, fetching_metadata,
    executing_query, filtering_context, etc.).

    This captures what the standard SDK hides: the actual content flowing
    through each state, not just the state names.
    """

    TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "CANCELED"}
    POLL_INTERVAL_S = 1.0
    MAX_POLLS = 120  # 2 minutes max wait

    def __init__(
        self,
        space_id: str,
        experiment_name: Optional[str] = None,
        workspace_client: Optional[WorkspaceClient] = None,
    ):
        self.space_id = space_id
        self.w = workspace_client or WorkspaceClient()

        if experiment_name:
            mlflow.set_experiment(experiment_name)

    def ask(self, question: str, conversation_id: Optional[str] = None) -> GenieResponse:
        """
        Send a question to the Genie Space and trace the full lifecycle.

        Creates MLflow spans for:
        - send_message: initial API call
        - poll_completion: waiting loop
        - state:{name}: each state transition observed

        Returns GenieResponse with full content, SQL, states, timing.
        """
        with mlflow.start_span(name="genie_conversation") as parent_span:
            parent_span.set_inputs({"question": question, "space_id": self.space_id})
            start_time = time.time()

            # Step 1: Start or continue conversation
            with mlflow.start_span(name="send_message") as send_span:
                if conversation_id:
                    resp = self._send_message(conversation_id, question)
                else:
                    resp = self._start_conversation(question)

                conversation_id = resp.get("conversation_id")
                message_id = resp.get("message_id")
                send_span.set_attributes({
                    "conversation_id": conversation_id or "",
                    "message_id": message_id or "",
                })

            # Step 2: Poll for completion, logging each state transition
            states_seen = []
            final_message = {}

            with mlflow.start_span(name="poll_completion") as poll_span:
                previous_status = None
                for i in range(self.MAX_POLLS):
                    time.sleep(self.POLL_INTERVAL_S)

                    msg = self._get_message(conversation_id, message_id)
                    current_status = msg.get("status", "UNKNOWN")

                    # Track status field changes as state transitions
                    if current_status != previous_status:
                        states_seen.append(current_status)
                        with mlflow.start_span(name=f"state:{current_status.lower()}") as state_span:
                            state_span.set_attributes({
                                "state_name": current_status,
                                "poll_iteration": i,
                                "elapsed_ms": int((time.time() - start_time) * 1000),
                            })
                        previous_status = current_status

                    if current_status in self.TERMINAL_STATES:
                        final_message = msg
                        break
                else:
                    final_message = msg

                poll_span.set_attributes({
                    "final_status": current_status,
                    "polls_required": i + 1 if current_status in self.TERMINAL_STATES else self.MAX_POLLS,
                    "states_observed": states_seen,
                    "timeout": current_status not in self.TERMINAL_STATES,
                })

            # Step 3: Parse and return
            duration_ms = int((time.time() - start_time) * 1000)
            result = self._parse_response(final_message, conversation_id, states_seen, duration_ms)

            parent_span.set_outputs({
                "status": result.status,
                "content_preview": (result.content or "")[:200],
                "has_sql": result.generated_sql is not None,
                "duration_ms": duration_ms,
                "states": states_seen,
                "error": result.error,
            })

            return result

    # -- Private API methods --

    def _start_conversation(self, question: str) -> dict:
        """
        Start a new conversation. Response shape (from docs):
        {
            "conversation": {"id": "..."},
            "message": {"id": "..."}
        }
        We normalize to {"conversation_id": ..., "message_id": ...}
        """
        resp = self.w.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{self.space_id}/start-conversation",
            body={"content": question},
        )
        # Normalize nested response to flat keys
        return {
            "conversation_id": resp.get("conversation", {}).get("id") or resp.get("conversation_id"),
            "message_id": resp.get("message", {}).get("id") or resp.get("message_id"),
        }

    def _send_message(self, conversation_id: str, question: str) -> dict:
        """
        Send follow-up message. Response includes message_id directly.
        """
        resp = self.w.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages",
            body={"content": question},
        )
        return {
            "conversation_id": conversation_id,
            "message_id": resp.get("message", {}).get("id") or resp.get("message_id") or resp.get("id"),
        }

    def _get_message(self, conversation_id: str, message_id: str) -> dict:
        return self.w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}",
        )

    def _parse_response(self, msg: dict, conversation_id: str, states: list, duration_ms: int) -> GenieResponse:
        """
        Parse final message response. Actual API shape (list form):
        attachments: [
            {"query": {"query": "SQL...", "description": "...", "thoughts": [...]}, "attachment_id": "..."},
            {"suggested_questions": {"questions": [...]}},
            {"text": {"content": "The natural language answer..."}}
        ]
        """
        generated_sql = None
        response_text = None
        thoughts = []
        attachments_raw = msg.get("attachments") or []

        if isinstance(attachments_raw, dict):
            # Dict form (older shape): {"query_attachments": [...], "text_attachments": [...]}
            for qa in attachments_raw.get("query_attachments", []):
                if isinstance(qa, dict):
                    query_obj = qa.get("query", qa)
                    if isinstance(query_obj, dict):
                        generated_sql = generated_sql or query_obj.get("query")
                        thoughts = query_obj.get("thoughts", [])
                    elif isinstance(query_obj, str):
                        generated_sql = query_obj
            for ta in attachments_raw.get("text_attachments", []):
                if isinstance(ta, dict):
                    response_text = ta.get("content") or ta.get("text")
        elif isinstance(attachments_raw, list):
            # Current API shape: list of attachment objects
            for att in attachments_raw:
                if not isinstance(att, dict):
                    continue
                # SQL attachment
                if "query" in att and isinstance(att["query"], dict):
                    generated_sql = generated_sql or att["query"].get("query")
                    thoughts = att["query"].get("thoughts", [])
                # Text/response attachment
                if "text" in att and isinstance(att["text"], dict):
                    response_text = att["text"].get("content")

        error_msg = None
        if msg.get("error"):
            error_msg = msg["error"].get("message", str(msg["error"]))

        return GenieResponse(
            message_id=msg.get("id") or msg.get("message_id") or "unknown",
            conversation_id=conversation_id or "unknown",
            space_id=self.space_id,
            status=msg.get("status", "UNKNOWN"),
            content=response_text or msg.get("content"),
            generated_sql=generated_sql,
            state_transitions=states,
            error=error_msg,
            duration_ms=duration_ms,
            attachments=attachments_raw,
        )
