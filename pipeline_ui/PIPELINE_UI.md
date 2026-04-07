# pipeline_ui

React-based visual DAG builder for constructing and configuring data pipelines. Built with TypeScript, Vite, and `@xyflow/react`. Communicates exclusively with `pipeline_service` over HTTP.

## Purpose

`pipeline_ui` provides a drag-and-drop interface for domain experts (statisticians, analysts) to build pipelines without writing YAML by hand. Users assemble a DAG from a palette of typed nodes, configure each node through a generated form, infer data contracts at design time, and request AI-generated node configuration based on the shapes of incoming data.

## Features

### Transform palette
- Populated dynamically from `GET /node-types` on load — always in sync with the server
- Grouped by category: Load, Transform, SQL, Export
- Each item shows the node label; tooltip shows the full description
- Drag a node type from the palette onto the canvas to add it

### DAG canvas
- Rendered by `@xyflow/react` with a dark Catppuccin-inspired theme
- Nodes can be freely repositioned; edges are drawn by dragging from a source handle to a target handle
- MiniMap and zoom/pan controls included
- Drag-and-drop from palette places a new node at the cursor position

### Custom node rendering (`PipelineNode`)
- Displays node type as a colour-coded tag (load=cyan, transform=green, sql=red, export=orange)
- Shows node ID as the primary label
- If an output schema has been inferred, shows the first four columns (name + dtype) with a "+N more" overflow indicator

### Node config panel (`NodeConfigPanel`)
Opens as a right-hand side panel when a node is selected. Contains:

**Typed param forms**
- Form fields are generated from the node type's `fixed_params` (from `GET /node-types`)
- Each field is rendered as the appropriate control: text input, number input, checkbox (bool), or JSON textarea (list/dict)
- Required fields are marked; defaults are shown as placeholder text

**Template param editor**
- For node types with `accepts_template_params: true` (e.g. `sql_exec`, `sql_transform`), a free key/value editor is provided for arbitrary Jinja2 template parameters
- Existing extra params are shown; new pairs can be added inline

**Input schema display**
- Shows the inferred output schemas of all upstream (connected) nodes
- Column names and dtypes listed per input — gives the user context when writing SQL or configuring transforms

**Output schema display**
- Once schema inference has been run for this node, its inferred output schema is shown

**AI suggest button (✦ AI suggest)**
- Calls `POST /pipelines/suggest-config` with the current node type, input schemas, and already-set params
- Merges the suggested params into the form
- Displays Claude's one-sentence explanation of the suggestion below the form
- Useful for: auto-writing SQL JOINs for `sql_transform` nodes, suggesting transform paths and sub-params for `pandas_transform` nodes

**Infer schema button (▶ Infer schema)**
- Available on nodes that produce output
- Serialises the current canvas state to a pipeline YAML fragment and calls `POST /pipelines/execute-node`
- On success: updates the node's output schema display and propagates the schema to all outgoing edges
- Runs the subgraph in a temporary DuckDB — does not touch any real data source

### Pipeline serialisation
The canvas is continuously serialisable to a pipeline YAML fragment (currently JSON-encoded, valid YAML superset):
- Node IDs are used as output names and input references — edges determine the `inputs` list for each node
- Params from the config panel are embedded as `params` on each node spec
- The serialised form is what gets sent to `execute-node` and `suggest-config`

### API client
Typed fetch wrappers in `src/api/client.ts` for all `pipeline_service` endpoints:
- `fetchNodeTypes()` — palette data
- `inspectTransform(path)` — docstring-driven param schema
- `validatePipeline(yaml, env?)` — validation feedback
- `fetchDag(yaml, env?)` — load existing pipeline as DAG
- `executeNode(yaml, nodeId, env?)` — design-time schema inference
- `suggestConfig(type, id, inputSchemas, currentParams)` — AI config

All requests proxy through Vite's dev server (`/api` → `http://localhost:8000`).

## Project layout

```
pipeline_ui/
  src/
    api/
      client.ts         # Typed fetch wrappers for all pipeline_service endpoints
    components/
      Palette.tsx       # Drag-and-drop node type palette
      PipelineNode.tsx  # Custom ReactFlow node component
      NodeConfigPanel.tsx  # Side panel: param forms, AI suggest, infer schema
    hooks/
      useNodeTypes.ts   # Fetches and memoises node type schemas on mount
    types/
      index.ts          # TypeScript interfaces mirroring pipeline_service models
    App.tsx             # Main layout: palette + canvas + config panel; state management
    main.tsx            # React entry point
  index.html
  vite.config.ts        # Dev server with /api proxy to port 8000
  package.json
  tsconfig.app.json
```

## Running

```bash
npm install      # first time only
npm run dev      # starts Vite dev server at http://localhost:5173
```

Requires `pipeline_service` to be running at `http://localhost:8000`.

## Build

```bash
npm run build    # TypeScript compile + Vite production bundle → dist/
```
