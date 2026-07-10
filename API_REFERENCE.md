# Genie Conversation API Reference

Source: Databricks REST API docs (`/api/2.0/genie/spaces`)

## Core Flow

### 1. Start a Conversation

```
POST /api/2.0/genie/spaces/{space_id}/start-conversation
Body: {"content": "your question"}
```

**Response:**
```json
{
  "conversation": {"id": "conv-uuid"},
  "message": {"id": "msg-uuid"}
}
```

### 2. Poll Message Status

```
GET /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}
```

**Response (progressively populated):**
```json
{
  "id": "msg-uuid",
  "status": "COMPLETED",
  "content": "Here are the top sales...",
  "created_by": "user@example.com",
  "created_at": "2026-07-09T10:00:00Z",
  "completed_at": "2026-07-09T10:00:05Z",
  "attachments": {
    "text_attachments": [...],
    "query_attachments": [
      {
        "attachment_id": "att-uuid",
        "query": "SELECT ... FROM ...",
        "title": "Top Sales",
        "description": "reasoning trace / step-by-step explanation"
      }
    ],
    "viz_attachments": [...]
  }
}
```

**Key insight:** `attachments.query_attachments` contains the reasoning trace
(Genie's step-by-step thinking) when present. This is the primary source
for observability of HOW Genie answered, not just WHAT it answered.

**Status values:** `ASKING_AI`, `FETCHING_METADATA`, `FILTERING_CONTEXT`,
`EXECUTING_QUERY`, `COMPLETED`, `FAILED`, `CANCELLED`

### 3. Fetch Query Results

```
GET /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result
```

Returns the actual tabular result set from the generated SQL.

### 4. Download Full Result Set (optional)

```
POST /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/downloads
```

Creates a downloadable result. Response includes `download_id` and `download_id_signature`.

## Send Follow-up Message

```
POST /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages
Body: {"content": "follow-up question"}
```

## List Conversations

```
GET /api/2.0/genie/spaces/{space_id}/conversations
```

## Get Space Metadata

```
GET /api/2.0/genie/spaces/{space_id}
```

## Notes

- **Attachments populate progressively** during processing — poll until `status` is terminal
- **Two attachment shapes exist** depending on API version:
  - Dict form: `{"query_attachments": [...], "text_attachments": [...]}`
  - List form: `[{"type": "QUERY", "attachment_id": "...", ...}]`
- **Reasoning traces** live in `query_attachments` — this is the "thinking" data
- **`enable_visualization: true`** in the request body enables viz attachment generation
- **Permissions**: Caller needs `CAN MANAGE` on the space to access other users' conversations
