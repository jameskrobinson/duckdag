# AI-Assisted Transform Generation ("Describe & Generate")

The pipeline builder includes an AI code generation workflow that lets you describe a transformation in plain English and have it written automatically as a `pandas_transform` function, grounded in the real column names and dtypes from your upstream nodes.

---

## Prerequisites

The feature requires an Anthropic API key. Set the environment variable before starting the service:

```
ANTHROPIC_API_KEY=sk-ant-...
```

If the key is absent the service returns a `503` with a clear error message when generation is attempted. All other builder functionality works normally without the key.

---

## Workflow

### 1. Drag a blank Python node onto the canvas

In the palette, open the **AI** section and drag **"New Python transform"** onto the canvas. The node renders with a purple dashed border to distinguish it from configured nodes.

### 2. Wire upstream inputs (recommended)

Connect any upstream nodes before describing the transform. The generation prompt is grounded in the exact column names and dtypes from those inputs, which produces more accurate code. The DescribeTransform modal shows a read-only summary of available columns per input node.

You can connect inputs after generating too, but the AI will have less context to work with.

### 3. Click "✦ Describe" in the config panel

Select the stub node to open its config panel. Click the **✦ Describe** button in the footer. The Describe Transform modal opens.

### 4. Fill in the modal

| Field | Required | Description |
|-------|----------|-------------|
| Description | Yes | Plain-English description of what the transform should do. Can reference column names directly. |
| Output columns | No | Comma-separated list of expected output column names. Helps the AI produce the correct `return` statement and populates the node's contract validation. |

Press **Ctrl+Enter** or click **Generate** to submit.

### 5. Review the result

The AI returns one of two responses:

#### "Configure" path — an existing transform can do the job

If a transform already in your workspace matches the requirement, the modal closes automatically and the node is updated to `pandas_transform` with that transform and the appropriate params pre-filled. A brief explanation is shown in the modal before it closes.

#### "New code" path — new Python function required

The modal expands to show a **CodeMirror Python editor** pre-filled with the generated function. Review and edit the code directly in the editor before saving.

Click **Save & apply**:
- You are prompted for a filename (pre-filled with a suggestion such as `transforms/generated_regression.py`).
- The file is written to the pipeline-local transforms directory: `pipelines/{name}/transforms/`.
- The stub node is converted to a `pandas_transform` node pointing to the new function.
- The node label updates to the generated function name.
- The new transform appears immediately in the palette under **Pipeline Transforms**.

### 6. Run or preview

Once applied, use **⊞ Preview** in the config panel or **▶ Run** in the session panel to execute the transform and inspect the output. The node behaves identically to any hand-written `pandas_transform`.

---

## Iterative refinement ("✦ Re-describe")

Any `pandas_transform` node that was created via AI generation shows a **✦ Re-describe** button in its config panel footer (instead of ✦ Describe). Clicking it re-opens the modal with:

- The previous description pre-filled.
- The previously generated code visible as context (sent to the AI on regeneration so it can iterate rather than start from scratch).

Refine the description or output columns hint and click Generate again. The updated code replaces the previous version.

---

## Validation

If any `python_stub` nodes remain on the canvas when you attempt to run a pipeline, a warning appears in the validation banner:

> Node "{id}" is an unresolved AI stub — open config panel and click ✦ Describe to generate it

The pipeline will still run but stub nodes will fail at execution time with a clear error directing you back to the Describe flow.

---

## Generated file location

New transform files are always written to the **pipeline-local transforms directory**:

```
workspace/
  pipelines/
    my_pipeline/
      pipeline.yaml
      transforms/
        generated_price_normaliser.py   ← written here
```

This keeps generated code close to the pipeline that uses it and out of the shared workspace library until you are confident in it.

Once you are happy with the generated function, use the **↑ Promote to workspace** button in the Transform Editor panel to move it to `workspace/transforms/` where it becomes available to all pipelines.

---

## What the AI can and cannot do

| The AI is good at | The AI needs help with |
|-------------------|----------------------|
| Filtering, joining, reshaping DataFrames | Transforms with complex business rules not expressible in a short description |
| Selecting, renaming, casting columns | Transforms that require external data or side effects |
| Computing derived columns (arithmetic, string manipulation, date math) | Exact numeric precision requirements |
| Aggregations and pivot operations | Organisation-specific domain logic |
| Reusing existing transforms when a match exists | Knowing which columns are semantically meaningful without a description |

Connecting upstream nodes before describing gives the AI the actual column names to reference — vague descriptions like "join the two tables" become precise when the AI can see `customer_id` on both sides.

---

## Technical notes

- Model: `claude-sonnet-4-6`, temperature 0 (deterministic output).
- The prompt includes the full input schema (all upstream column names and dtypes), existing workspace transform paths (so the AI can prefer reuse over writing new code), and any previous generated code on re-describe.
- File writes are **user-confirmed client-side** — the service never writes transform files directly; it returns the code and the frontend prompts for a filename before saving.
- Generated nodes carry `_generated: true` in their params, which is what triggers the ✦ Re-describe button. Manually created `pandas_transform` nodes are not affected.
- The `_description` and `_generated_code` params are persisted in `pipeline.yaml` so the re-describe context survives a page reload.
