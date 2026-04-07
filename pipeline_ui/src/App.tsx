import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import * as yaml from 'js-yaml'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type OnConnect,
  type NodeTypes,
} from '@xyflow/react'
import '@xyflow/react/dist/base.css'

import Palette from './components/Palette'
import PipelineNode from './components/PipelineNode'
import ContractEdge from './components/ContractEdge'
import NodeConfigPanel from './components/NodeConfigPanel'
import WorkspaceBar from './components/WorkspaceBar'
import LoadPipelineModal from './components/LoadPipelineModal'
import NewPipelineModal from './components/NewPipelineModal'
import YamlPreviewPanel from './components/YamlPreviewPanel'
import VariablesPanel from './components/VariablesPanel'
import RunHistoryPanel from './components/RunHistoryPanel'
import RunVariablesModal from './components/RunVariablesModal'
import RunPanel from './components/RunPanel'
import SessionPanel from './components/SessionPanel'
import TransformEditorPanel from './components/TransformEditorPanel'
import { useNodeTypes } from './hooks/useNodeTypes'
import { useValidation } from './hooks/useValidation'
import { createRun, createSession, executeNode, fetchActiveSession, fetchDag, fetchGitStatus, fetchNodeLineage, fetchTemplates, fetchVariableDeclarations, fetchWorkspaceVariables, getSession, invalidateSessionNode, pollRun, pollRunNodes, pollSessionNodes, previewNode, readWorkspacePipeline, writeSchemaFile, writeWorkspaceFile } from './api/client'
import type { BuilderNodeData, ColumnSchema, NodePreviewResponse, NodeRunResponse, NodeTemplate, NodeTypeSchema, PandasTransformEntry, RunResponse, SessionNodeResponse, SessionResponse, VariableDeclaration } from './types'

const nodeTypes: NodeTypes = {
  pipelineNode: PipelineNode as NodeTypes[string],
}

const edgeTypes = {
  contractEdge: ContractEdge,
}

const WORKSPACE_KEY = 'pipeline_workspace'

let _idCounter = 0
function nextId() { return `node_${++_idCounter}` }

type PipelineNode = Node<BuilderNodeData>

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<PipelineNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const reactFlowWrapper = useRef<HTMLDivElement>(null)

  // Keep a ref to current edges so param-update handler can traverse without stale closure
  const edgesRef = useRef(edges)
  useEffect(() => { edgesRef.current = edges }, [edges])

  // ---------------------------------------------------------------------------
  // Undo / Redo — history stack of { nodes, edges } snapshots
  // ---------------------------------------------------------------------------
  type HistoryEntry = { nodes: PipelineNode[]; edges: Edge[] }
  const undoStack = useRef<HistoryEntry[]>([])
  const redoStack = useRef<HistoryEntry[]>([])
  const [canUndo, setCanUndo] = useState(false)
  const [canRedo, setCanRedo] = useState(false)

  /** Call this *before* any mutating setNodes/setEdges call to record the current state. */
  const nodesRef = useRef(nodes)
  useEffect(() => { nodesRef.current = nodes }, [nodes])

  function pushHistory() {
    const entry: HistoryEntry = { nodes: nodesRef.current, edges: edgesRef.current }
    undoStack.current = [...undoStack.current.slice(-49), entry]
    redoStack.current = []
    setCanUndo(true)
    setCanRedo(false)
  }

  function handleUndo() {
    if (!undoStack.current.length) return
    // Cancel any pending debounced param-edit history so it doesn't fire after undo
    if (paramEditTimer.current) { clearTimeout(paramEditTimer.current); paramEditTimer.current = null; paramEditPreSnap.current = null }
    const prev = undoStack.current[undoStack.current.length - 1]
    undoStack.current = undoStack.current.slice(0, -1)
    redoStack.current = [{ nodes: nodesRef.current, edges: edgesRef.current }, ...redoStack.current]
    setNodes(prev.nodes)
    setEdges(prev.edges)
    setCanUndo(undoStack.current.length > 0)
    setCanRedo(true)
  }

  function handleRedo() {
    if (!redoStack.current.length) return
    // Cancel any pending debounced param-edit history
    if (paramEditTimer.current) { clearTimeout(paramEditTimer.current); paramEditTimer.current = null; paramEditPreSnap.current = null }
    const next = redoStack.current[0]
    redoStack.current = redoStack.current.slice(1)
    undoStack.current = [...undoStack.current, { nodes: nodesRef.current, edges: edgesRef.current }]
    setNodes(next.nodes)
    setEdges(next.edges)
    setCanUndo(true)
    setCanRedo(redoStack.current.length > 0)
  }

  // Keyboard shortcut: Ctrl+Z / Ctrl+Y / Ctrl+Shift+Z
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey) {
        e.preventDefault()
        handleUndo()
      }
      if ((e.ctrlKey || e.metaKey) && (e.key === 'y' || (e.key === 'z' && e.shiftKey))) {
        e.preventDefault()
        handleRedo()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  })

  // Wrap onNodesChange to capture history on drag-end and keyboard-delete
  const onNodesChangeWrapped: typeof onNodesChange = (changes) => {
    const hasDragEnd = changes.some(
      (c) => c.type === 'position' && !(c as { dragging?: boolean }).dragging
        && nodesRef.current.some((n) => n.id === c.id && n.dragging)
    )
    const hasRemove = changes.some((c) => c.type === 'remove')
    if (hasDragEnd || hasRemove) pushHistory()
    onNodesChange(changes)
  }

  // Wrap onEdgesChange to capture history on edge removal
  const onEdgesChangeWrapped: typeof onEdgesChange = (changes) => {
    if (changes.some((c) => c.type === 'remove')) pushHistory()
    onEdgesChange(changes)
  }

  // Snapshot of params at the last successful run completion — used for stale detection
  const paramsAtRun = useRef<Record<string, Record<string, unknown>>>({})

  // Debounce param-change history: capture the pre-edit snapshot once on first keystroke,
  // then commit it to the undo stack after 600 ms of inactivity (so a whole field edit
  // is one Ctrl+Z step rather than character-by-character).
  const paramEditTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const paramEditPreSnap = useRef<{ nodes: PipelineNode[]; edges: Edge[] } | null>(null)

  // Context menu for node right-click
  const [contextMenu, setContextMenu] = useState<{ nodeId: string; x: number; y: number } | null>(null)

  // Run panel (non-workspace ad-hoc runs)
  const [activeRun, setActiveRun] = useState<RunResponse | null>(null)
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, NodeRunResponse>>({})
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Session panel (workspace-backed sessions)
  const [activeSession, setActiveSession] = useState<SessionResponse | null>(null)
  const [sessionNodeStatuses, setSessionNodeStatuses] = useState<Record<string, SessionNodeResponse>>({})
  const sessionPollRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const sessionWsRef = useRef<WebSocket | null>(null)

  // Git status for the loaded pipeline — shown as a warning when running with uncommitted changes
  const [hasUncommittedChanges, setHasUncommittedChanges] = useState(false)

  /** Absolute path to the pipeline YAML file loaded from workspace */
  const [pipelineFilePath, setPipelineFilePath] = useState<string | null>(null)

  /** Human-readable name derived from pipelineFilePath, e.g. "market_summary" */
  const pipelineName = useMemo<string>(() => {
    if (!pipelineFilePath) return 'Untitled'
    const parts = pipelineFilePath.replace(/\\/g, '/').split('/')
    // New layout: pipelines/{name}/pipeline.yaml → use {name}
    const pipelinesIdx = parts.findLastIndex((p) => p.toLowerCase() === 'pipelines')
    if (pipelinesIdx >= 0 && parts[pipelinesIdx + 1]) return parts[pipelinesIdx + 1]
    // Fallback: parent directory of the yaml file
    if (parts.length >= 2) return parts[parts.length - 2]
    return parts[parts.length - 1].replace(/\.ya?ml$/i, '') || 'Untitled'
  }, [pipelineFilePath])

  // Workspace
  const [workspace, setWorkspace] = useState<string>(
    () => localStorage.getItem(WORKSPACE_KEY) ?? ''
  )
  const { nodeTypes: paletteTypes, nodeTypeMap, pandasCategories, refreshTransforms } = useNodeTypes(workspace || undefined)

  // Derive pandas transform entries directly from node data — no separate state
  // that can drift out of sync when nodes are updated (e.g. after schema inference).
  const nodeTransformMap = useMemo<Record<string, PandasTransformEntry>>(() => {
    if (pandasCategories.length === 0) return {}
    const result: Record<string, PandasTransformEntry> = {}
    for (const n of nodes) {
      const transformPath = n.data.params?.transform as string | undefined
      if (n.data.node_type === 'pandas_transform' && transformPath) {
        for (const cat of pandasCategories) {
          const entry = cat.transforms.find((t) => t.full_path === transformPath)
          if (entry) { result[n.id] = entry; break }
        }
      }
    }
    return result
  }, [nodes, pandasCategories])
  const [showLoadModal, setShowLoadModal] = useState(false)
  const [showNewPipelineModal, setShowNewPipelineModal] = useState(false)
  const [showVariablesPanel, setShowVariablesPanel] = useState(false)
  const [showRunHistory, setShowRunHistory] = useState(false)
  const [showTransformEditor, setShowTransformEditor] = useState(false)
  const [showRunVarsModal, setShowRunVarsModal] = useState(false)
  const [yamlPreviewOpen, setYamlPreviewOpen] = useState(false)
  /** Absolute directory of the last pipeline file loaded from workspace */
  const [pipelineDir, setPipelineDir] = useState<string | null>(null)
  /** Raw YAML string from variables.yaml — passed to all service calls */
  const [variablesYaml, setVariablesYaml] = useState<string | null>(null)
  /** Variable declarations from the loaded pipeline.yaml (variable_declarations block) */
  const [variableDeclarations, setVariableDeclarations] = useState<VariableDeclaration[]>([])

  // Templates — fetched whenever workspace changes
  const [remoteTemplates, setRemoteTemplates] = useState<NodeTemplate[]>([])
  useEffect(() => {
    fetchTemplates(workspace || undefined).then(setRemoteTemplates).catch(() => setRemoteTemplates([]))
  }, [workspace])

  // Variables — load variables.yaml content whenever workspace changes
  useEffect(() => {
    if (!workspace) { setVariablesYaml(null); return }
    fetchWorkspaceVariables(workspace)
      .then((data) => {
        if (Object.keys(data.variables).length > 0) {
          // Re-serialise to YAML for the service
          import('js-yaml').then((yamlLib) => {
            setVariablesYaml(yamlLib.dump(data.variables))
          })
        } else {
          setVariablesYaml(null)
        }
      })
      .catch(() => setVariablesYaml(null))
  }, [workspace])

  // Pipeline-local templates — derived from current canvas nodes so every
  // existing node is instantly reusable as a drag-and-drop starting point.
  const pipelineTemplates = useMemo<NodeTemplate[]>(() =>
    nodes
      .filter((n) => Object.keys(n.data.params ?? {}).length > 0 || n.data.template_file)
      .map((n) => ({
        id: `pipeline/${n.id}`,
        node_type: n.data.node_type,
        label: n.data.label || n.id,
        description: n.data.description || `From pipeline: ${n.id}`,
        scope: 'pipeline' as const,
        params: n.data.params ?? {},
        template_file: n.data.template_file,
        template_path: n.data.template_path,
      }))
  , [nodes])

  const templates = useMemo(
    () => [...remoteTemplates, ...pipelineTemplates],
    [remoteTemplates, pipelineTemplates],
  )

  function handleWorkspaceChange(path: string) {
    setWorkspace(path)
    localStorage.setItem(WORKSPACE_KEY, path)
  }

  // ---------------------------------------------------------------------------
  // Connect nodes
  // ---------------------------------------------------------------------------

  const onConnect: OnConnect = useCallback(
    (connection) => {
      pushHistory()
      setEdges((eds) => addEdge({ ...connection, type: 'contractEdge', animated: true }, eds))
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [setEdges],
  )

  // ---------------------------------------------------------------------------
  // Drag-and-drop from palette
  // ---------------------------------------------------------------------------

  function onDragOver(e: React.DragEvent) {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }

  function onDrop(e: React.DragEvent) {
    e.preventDefault()
    const raw = e.dataTransfer.getData('application/pipeline-node-type')
    if (!raw) return

    const payload = JSON.parse(raw) as NodeTypeSchema & {
      _defaultParams?: Record<string, unknown>
      _templateFile?: string
      _templatePath?: string
    }
    const rect = reactFlowWrapper.current?.getBoundingClientRect()
    if (!rect) return

    const position = { x: e.clientX - rect.left - 90, y: e.clientY - rect.top - 40 }
    const id = nextId()
    const defaultParams = payload._defaultParams ?? {}

    const newNode: Node<BuilderNodeData> = {
      id,
      type: 'pipelineNode',
      position,
      data: {
        label: payload.label ?? id,
        node_type: payload.type,
        description: null,
        output_schema: null,
        params: defaultParams,
        template_file: payload._templateFile,
        template_path: payload._templatePath,
      },
    }
    pushHistory()
    setNodes((nds) => [...nds, newNode])
  }

  // ---------------------------------------------------------------------------
  // Load pipeline from workspace
  // ---------------------------------------------------------------------------

  async function handleLoadPipeline(fullPath: string) {
    setShowLoadModal(false)
    try {
      const { yaml: yamlText } = await readWorkspacePipeline(fullPath)

      // Parse YAML to extract params and template per node
      const parsed = yaml.load(yamlText) as Record<string, unknown>
      const rawNodes = (parsed?.nodes as Array<Record<string, unknown>>) ?? []

      // Resolve templates dir: {pipeline_dir}/{templates.dir} (default: "templates")
      const pipelineDir = fullPath.replace(/[\\/][^\\/]+$/, '')
      setPipelineDir(pipelineDir)
      setPipelineFilePath(fullPath)
      fetchVariableDeclarations(fullPath).then(setVariableDeclarations).catch(() => setVariableDeclarations([]))
      fetchGitStatus(fullPath).then((s) => setHasUncommittedChanges(s.has_uncommitted_changes)).catch(() => setHasUncommittedChanges(false))

      // Reconnect to any active session for this pipeline
      fetchActiveSession(fullPath).then((session) => {
        if (session) {
          setActiveSession(session)
          setActiveRun(null)
          startSessionWebSocket(session.session_id)
        }
      }).catch(() => {})
      const templatesRelDir = (parsed?.templates as Record<string, unknown>)?.dir as string ?? 'templates'
      const templatesDir = `${pipelineDir}/${templatesRelDir}`.replace(/\\/g, '/')

      const paramsByNodeId: Record<string, Record<string, unknown>> = {}
      const templatePathByNodeId: Record<string, string> = {}
      const templateFileByNodeId: Record<string, string> = {}
      const dqChecksByNodeId: Record<string, import('./types').DQCheck[]> = {}
      for (const rn of rawNodes) {
        const id = rn.id as string
        paramsByNodeId[id] = (rn.params as Record<string, unknown>) ?? {}
        if (rn.template) {
          templateFileByNodeId[id] = rn.template as string
          templatePathByNodeId[id] = `${templatesDir}/${rn.template}`
        }
        if (Array.isArray(rn.dq_checks) && rn.dq_checks.length > 0) {
          dqChecksByNodeId[id] = rn.dq_checks as import('./types').DQCheck[]
        }
      }

      // Get DAG layout from service
      const dag = await fetchDag(yamlText)

      // Clear canvas and rebuild from DAG
      setSelectedNodeId(null)

      const newNodes: Node<BuilderNodeData>[] = dag.nodes.map((sn) => ({
        id: sn.id,
        type: 'pipelineNode',
        position: sn.position,
        data: {
          label: sn.id,
          node_type: sn.data.node_type,
          description: sn.data.description,
          output_schema: sn.data.output_schema ?? null,
          params: paramsByNodeId[sn.id] ?? {},
          template_file: templateFileByNodeId[sn.id],
          template_path: templatePathByNodeId[sn.id],
          dq_checks: dqChecksByNodeId[sn.id],
        },
      }))

      const newEdges: Edge[] = dag.edges.map((se) => ({
        id: se.id,
        source: se.source,
        target: se.target,
        type: 'contractEdge',
        animated: true,
        data: { contract: se.contract },
      }))

      pushHistory()
      setNodes(newNodes)
      setEdges(newEdges)
    } catch (e) {
      console.error('Load pipeline failed:', e)
      alert(`Failed to load pipeline: ${e}`)
    }
  }

  // ---------------------------------------------------------------------------
  // Node selection
  // ---------------------------------------------------------------------------

  function onNodeClick(_: React.MouseEvent, node: Node) { setSelectedNodeId(node.id) }
  function onPaneClick() { setSelectedNodeId(null); setContextMenu(null) }

  // ---------------------------------------------------------------------------
  // Update params from config panel
  // ---------------------------------------------------------------------------

  function handleParamUpdate(nodeId: string, params: Record<string, unknown>) {
    // Debounced history: capture pre-edit snapshot on the FIRST keystroke,
    // then push it to the undo stack after 600 ms of inactivity. This makes
    // the whole field edit a single Ctrl+Z step instead of character-by-character.
    if (!paramEditTimer.current) {
      // First change in this edit — capture current state as the undo checkpoint
      paramEditPreSnap.current = { nodes: nodesRef.current, edges: edgesRef.current }
    }
    if (paramEditTimer.current) clearTimeout(paramEditTimer.current)
    paramEditTimer.current = setTimeout(() => {
      if (paramEditPreSnap.current) {
        undoStack.current = [...undoStack.current.slice(-49), paramEditPreSnap.current]
        redoStack.current = []
        setCanUndo(true)
        setCanRedo(false)
        paramEditPreSnap.current = null
      }
      paramEditTimer.current = null
    }, 600)

    const snapshotParams = paramsAtRun.current[nodeId]
    const isStale = snapshotParams != null &&
      JSON.stringify(params) !== JSON.stringify(snapshotParams)

    setNodes((nds) => {
      if (!isStale) {
        return nds.map((n) => n.id === nodeId ? { ...n, data: { ...n.data, params } } : n)
      }
      // Mark this node and all transitive downstream nodes as stale
      const staleIds = new Set(getDownstreamIds(nodeId, edgesRef.current))
      staleIds.add(nodeId)
      return nds.map((n) => {
        if (n.id === nodeId) return { ...n, data: { ...n.data, params, stale: true } }
        if (staleIds.has(n.id)) return { ...n, data: { ...n.data, stale: true } }
        return n
      })
    })
  }

  function handleDqChecksUpdate(nodeId: string, dq_checks: import('./types').DQCheck[]) {
    pushHistory()
    // Always update the dq_checks on the node data first
    setNodes((nds) => nds.map((n) =>
      n.id === nodeId ? { ...n, data: { ...n.data, dq_checks } } : n
    ))
    if (activeSession && activeSession.status === 'active') {
      // Session is live — call the invalidate endpoint so the backend resets the node
      // (and downstream) to pending in session.duckdb, and the SessionPanel updates.
      handleInvalidateNode(nodeId).catch(() => {/* silent — best-effort */})
    } else if (paramsAtRun.current[nodeId] != null) {
      // No active session but this node ran in a previous one — set the canvas stale flag
      // so it's included in staleNodeIds on the next Re-execute call.
      const staleIds = new Set([nodeId, ...getDownstreamIds(nodeId, edgesRef.current)])
      setNodes((nds) => nds.map((n) =>
        staleIds.has(n.id) ? { ...n, data: { ...n.data, stale: true } } : n
      ))
    }
  }

  function handleSetTemplate(nodeId: string, templatePath: string, templateFile: string) {
    setNodes((nds) => nds.map((n) =>
      n.id === nodeId
        ? { ...n, data: { ...n.data, template_path: templatePath, template_file: templateFile } }
        : n
    ))
  }

  // ---------------------------------------------------------------------------
  // Design-time schema inference
  // ---------------------------------------------------------------------------

  async function handlePreviewNode(nodeId: string, limit?: number, whereClause?: string): Promise<NodePreviewResponse> {
    return previewNode(currentPipelineJson, nodeId, undefined, pipelineDir ?? undefined, limit ?? 1000, variablesYaml ?? undefined, workspace || undefined, activeSession?.bundle_path ?? undefined, undefined, whereClause)
  }

  async function handleRunSqlDraft(nodeId: string, sqlOverride: string): Promise<NodePreviewResponse> {
    // Source nodes (no incoming edges) have no upstream dependencies and can run
    // stateless against their own external connection — no session required.
    const hasInputs = edges.some((e) => e.target === nodeId)
    if (hasInputs && !activeSession?.bundle_path) {
      throw new Error('SQL Run requires an active session with completed upstream nodes. Start a session first (▶ Run), then use Run here.')
    }
    const bundlePath = hasInputs ? activeSession?.bundle_path : undefined
    return previewNode(currentPipelineJson, nodeId, undefined, pipelineDir ?? undefined, 200, variablesYaml ?? undefined, workspace || undefined, bundlePath, sqlOverride)
  }

  async function handleFetchLineage(nodeId: string) {
    if (!activeSession) return []
    return fetchNodeLineage(activeSession.session_id, nodeId)
  }

  function handleDeleteNode(nodeId: string) {
    pushHistory()
    setNodes((nds) => nds.filter((n) => n.id !== nodeId))
    setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId))
    setSelectedNodeId(null)
  }

  function handleCloneNode(nodeId: string) {
    pushHistory()
    const source = nodes.find((n) => n.id === nodeId)
    if (!source) return
    const newId = nextId()
    setNodes((nds) => [...nds, {
      ...source,
      id: newId,
      position: { x: source.position.x + 40, y: source.position.y + 40 },
      selected: false,
      data: { ...source.data, output_schema: null, run_status: undefined },
    }])
    setSelectedNodeId(newId)
  }

  async function handleExecuteNode(nodeId: string) {
    const result = await executeNode(currentPipelineJson, nodeId, undefined, pipelineDir ?? undefined, variablesYaml ?? undefined, workspace || undefined, activeSession?.bundle_path ?? undefined)

    setNodes((nds) =>
      nds.map((n) => n.id === nodeId ? { ...n, data: { ...n.data, output_schema: result.columns } } : n),
    )
    setEdges((eds) =>
      eds.map((e) => e.source === nodeId ? { ...e, data: { ...e.data, contract: result.columns } } : e),
    )

    // Write schema file to disk if we know where the pipeline lives
    if (pipelineDir) {
      const schemaPath = `${pipelineDir}/pipeline.schema.json`
      // Build schema from all nodes including the newly inferred one
      setNodes((nds) => {
        const schema: Record<string, unknown> = {}
        for (const n of nds) {
          if (n.data.output_schema?.length) schema[n.id] = { columns: n.data.output_schema }
        }
        writeSchemaFile(schemaPath, schema).catch(() => {/* silent — schema write is best-effort */})
        return nds
      })
    }
  }

  // ---------------------------------------------------------------------------
  // Input schemas for config panel
  // ---------------------------------------------------------------------------

  function getInputSchemas(nodeId: string): Record<string, ColumnSchema[]> {
    const result: Record<string, ColumnSchema[]> = {}
    for (const edge of edges) {
      if (edge.target !== nodeId) continue
      const sourceNode = nodes.find((n) => n.id === edge.source)
      if (sourceNode?.data.output_schema) result[edge.source] = sourceNode.data.output_schema
    }
    return result
  }

  // ---------------------------------------------------------------------------
  // Save pipeline
  // ---------------------------------------------------------------------------

  function savePipeline() {
    const pipelineObj = buildPipelineObject(nodes, edges)
    downloadText('pipeline.yaml', yaml.dump(pipelineObj, { lineWidth: 120 }))

    const schema: Record<string, unknown> = {}
    for (const n of nodes) {
      if (n.data.output_schema?.length) schema[n.id] = n.data.output_schema
    }
    if (Object.keys(schema).length > 0) {
      downloadText('pipeline.schema.json', JSON.stringify(schema, null, 2))
    }
  }

  async function saveToWorkspace() {
    if (!pipelineFilePath) return
    const pipelineObj = buildPipelineObject(nodes, edges)
    const yamlText = yaml.dump(pipelineObj, { lineWidth: 120 })
    try {
      await writeWorkspaceFile(pipelineFilePath, yamlText)
    } catch (e) {
      alert(`Failed to save pipeline: ${e}`)
    }
  }

  async function handleNewPipeline(name: string) {
    if (!workspace) throw new Error('No workspace configured.')
    const filePath = `${workspace}/pipelines/${name}/pipeline.yaml`
    const initialYaml = yaml.dump({
      duckdb: { path: 'pipeline.duckdb' },
      templates: { dir: 'templates' },
      nodes: [],
    }, { lineWidth: 120 })
    await writeWorkspaceFile(filePath, initialYaml)
    setShowNewPipelineModal(false)
    // Load the newly created pipeline onto the canvas
    await handleLoadPipeline(filePath)
  }

  function downloadText(filename: string, content: string) {
    const blob = new Blob([content], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
  }

  // ---------------------------------------------------------------------------
  // Submit run + poll for status
  // ---------------------------------------------------------------------------

  function handleSubmitRun() {
    // Always show the variable override modal before running
    setShowRunVarsModal(true)
  }

  async function handleConfirmRun(overrideVariablesYaml: string | undefined) {
    setShowRunVarsModal(false)
    const varsYaml = overrideVariablesYaml ?? variablesYaml ?? undefined

    // When a workspace is set, use the session model
    if (workspace) {
      try {
        const session = await createSession(currentPipelineJson, workspace, {
          pipeline_path: pipelineFilePath || undefined,
          variables_yaml: varsYaml,
        })
        setActiveSession(session)
        setActiveRun(null)
        setSessionNodeStatuses({})
        applySessionNodeStatuses({})
        startSessionWebSocket(session.session_id)
      } catch (e) {
        alert(`Failed to create session: ${e}`)
      }
      return
    }

    // No workspace — fall back to ad-hoc run
    try {
      const run = await createRun(currentPipelineJson, {
        variables_yaml: varsYaml,
      })
      setActiveRun(run)
      setNodeStatuses({})
      applyNodeStatuses({})
      startPolling(run.run_id)
    } catch (e) {
      alert(`Failed to submit run: ${e}`)
    }
  }

  function _applySessionUpdate(session: SessionResponse, nodeList: SessionNodeResponse[]) {
    setActiveSession(session)
    const statusMap = Object.fromEntries(nodeList.map((n) => [n.node_id, n]))
    setSessionNodeStatuses(statusMap)
    applySessionNodeStatuses(statusMap)
    if (session.status !== 'running') {
      if (session.status === 'active' && nodeList.every(n => n.status !== 'failed')) {
        setNodes((nds) => {
          const snapshot: Record<string, Record<string, unknown>> = {}
          for (const n of nds) snapshot[n.id] = { ...(n.data.params ?? {}) }
          paramsAtRun.current = snapshot
          return nds.map((n) => ({ ...n, data: { ...n.data, stale: false } }))
        })
      }
    }
  }

  function startSessionWebSocket(sessionId: string) {
    // Clean up any existing WS or HTTP poll
    if (sessionWsRef.current) { sessionWsRef.current.close(); sessionWsRef.current = null }
    if (sessionPollRef.current) { clearInterval(sessionPollRef.current); sessionPollRef.current = null }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const wsUrl = `${protocol}//${window.location.host}/api/sessions/${sessionId}/live`
    let ws: WebSocket
    try {
      ws = new WebSocket(wsUrl)
    } catch {
      _startSessionPollingFallback(sessionId)
      return
    }

    let wsConnected = false
    ws.onopen = () => { wsConnected = true }
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data as string)
        _applySessionUpdate(data.session as SessionResponse, data.nodes as SessionNodeResponse[])
      } catch { /* ignore */ }
    }
    ws.onerror = () => {
      if (!wsConnected) {
        // WS failed before connecting — fall back to HTTP polling
        sessionWsRef.current = null
        _startSessionPollingFallback(sessionId)
      }
    }
    ws.onclose = () => {
      sessionWsRef.current = null
    }
    sessionWsRef.current = ws
  }

  function _startSessionPollingFallback(sessionId: string) {
    if (sessionPollRef.current) clearInterval(sessionPollRef.current)
    sessionPollRef.current = setInterval(async () => {
      try {
        const [session, nodeList] = await Promise.all([
          getSession(sessionId),
          pollSessionNodes(sessionId),
        ])
        _applySessionUpdate(session, nodeList)
        if (session.status !== 'running') {
          clearInterval(sessionPollRef.current!)
          sessionPollRef.current = null
        }
      } catch { /* ignore transient errors */ }
    }, 2000)
  }

  function applySessionNodeStatuses(statusMap: Record<string, SessionNodeResponse>) {
    setNodes((nds) =>
      nds.map((n) => ({
        ...n,
        data: { ...n.data, run_status: statusMap[n.id]?.status ?? 'idle' },
      }))
    )
  }

  function handleDismissSession() {
    if (sessionWsRef.current) { sessionWsRef.current.close(); sessionWsRef.current = null }
    if (sessionPollRef.current) { clearInterval(sessionPollRef.current); sessionPollRef.current = null }
    setActiveSession(null)
    setSessionNodeStatuses({})
    setNodes((nds) => nds.map((n) => ({ ...n, data: { ...n.data, run_status: undefined } })))
  }

  function handleSessionUpdate(updated: SessionResponse) {
    setActiveSession(updated)
    if (updated.status === 'finalized' || updated.status === 'abandoned') {
      if (sessionWsRef.current) { sessionWsRef.current.close(); sessionWsRef.current = null }
      if (sessionPollRef.current) { clearInterval(sessionPollRef.current); sessionPollRef.current = null }
    }
  }

  function startPolling(runId: string) {
    if (pollRef.current) clearInterval(pollRef.current)
    pollRef.current = setInterval(async () => {
      try {
        const [run, nodeList] = await Promise.all([pollRun(runId), pollRunNodes(runId)])
        setActiveRun(run)
        const statusMap = Object.fromEntries(nodeList.map((n) => [n.node_id, n]))
        setNodeStatuses(statusMap)
        applyNodeStatuses(statusMap)
        if (run.status === 'completed' || run.status === 'failed') {
          clearInterval(pollRef.current!)
          pollRef.current = null
          if (run.status === 'completed') {
            // Snapshot params for stale detection and clear stale flags
            setNodes((nds) => {
              const snapshot: Record<string, Record<string, unknown>> = {}
              for (const n of nds) snapshot[n.id] = { ...(n.data.params ?? {}) }
              paramsAtRun.current = snapshot
              return nds.map((n) => ({ ...n, data: { ...n.data, stale: false } }))
            })
          }
        }
      } catch { /* ignore transient errors */ }
    }, 2000)
  }

  function applyNodeStatuses(statusMap: Record<string, NodeRunResponse>) {
    setNodes((nds) =>
      nds.map((n) => ({
        ...n,
        data: { ...n.data, run_status: statusMap[n.id]?.status ?? 'idle' },
      }))
    )
  }

  function handleDismissRun() {
    if (pollRef.current) clearInterval(pollRef.current)
    pollRef.current = null
    setActiveRun(null)
    setNodeStatuses({})
    // Clear run_status from nodes
    setNodes((nds) => nds.map((n) => ({ ...n, data: { ...n.data, run_status: undefined } })))
  }

  async function handleInvalidateNode(nodeId: string) {
    setContextMenu(null)
    if (!activeSession) return
    try {
      const resetIds = await invalidateSessionNode(activeSession.session_id, nodeId)
      // Reflect the reset in the canvas — mark those nodes as stale
      setNodes((nds) => nds.map((n) =>
        resetIds.includes(n.id) ? { ...n, data: { ...n.data, stale: true } } : n
      ))
      // Update the session node status panel — reset to pending
      setSessionNodeStatuses((prev) => {
        const next = { ...prev }
        for (const id of resetIds) {
          if (next[id]) next[id] = { ...next[id], status: 'pending', error: null }
        }
        return next
      })
    } catch (e) {
      alert(`Failed to invalidate node: ${e}`)
    }
  }

  async function handleRerunFromNode(nodeId: string) {
    setContextMenu(null)
    const upstreamIds = getUpstreamIds(nodeId, edgesRef.current)
    // Sessions don't yet support partial rerun — use ad-hoc run regardless
    try {
      const run = await createRun(currentPipelineJson, {
        workspace: workspace || undefined,
        pipeline_path: pipelineFilePath || pipelineDir || undefined,
        variables_yaml: variablesYaml ?? undefined,
        completed_nodes: upstreamIds,
      })
      setActiveRun(run)
      setActiveSession(null)
      setNodeStatuses({})
      applyNodeStatuses({})
      startPolling(run.run_id)
    } catch (e) {
      alert(`Failed to submit run: ${e}`)
    }
  }

  function onNodeContextMenu(e: React.MouseEvent, node: Node) {
    e.preventDefault()
    setContextMenu({ nodeId: node.id, x: e.clientX, y: e.clientY })
  }

  // ---------------------------------------------------------------------------
  // Live YAML preview
  // ---------------------------------------------------------------------------

  const previewYaml = useMemo(() => {
    if (!yamlPreviewOpen) return ''
    return yaml.dump(buildPipelineObject(nodes, edges), { lineWidth: 120 })
  }, [yamlPreviewOpen, nodes, edges])

  // ---------------------------------------------------------------------------
  // Validation (debounced, runs on every canvas change)
  // ---------------------------------------------------------------------------

  const currentPipelineJson = useMemo(() => buildPipelineJson(nodes, edges), [nodes, edges])
  const { errors: validationErrors, warnings: validationWarnings } = useValidation(currentPipelineJson, variablesYaml ?? undefined, 800, pipelineDir, workspace)

  /** Node IDs currently flagged stale on the canvas — sent to the service on re-execute */
  const staleNodeIds = useMemo(() => nodes.filter((n) => n.data.stale).map((n) => n.id), [nodes])

  /** Known variable names — derived from variablesYaml for autocomplete + highlighting */
  const variableNames = useMemo<string[]>(() => {
    if (!variablesYaml) return []
    try {
      const parsed = yaml.load(variablesYaml) as Record<string, unknown> | null
      return parsed ? Object.keys(parsed) : []
    } catch { return [] }
  }, [variablesYaml])

  /** Nodes augmented with var_error flag for missing ${variables.*} references */
  const nodesForCanvas = useMemo(() => {
    const varSet = new Set(variableNames)
    const VAR_REF = /\$\{variables\.([^}]+)\}/g
    return nodes.map((n) => {
      let hasError = false
      for (const v of Object.values(n.data.params ?? {})) {
        if (typeof v !== 'string') continue
        for (const m of v.matchAll(VAR_REF)) {
          if (!varSet.has(m[1])) { hasError = true; break }
        }
        if (hasError) break
      }
      if (hasError === !!n.data.var_error) return n
      return { ...n, data: { ...n.data, var_error: hasError || undefined } }
    })
  }, [nodes, variableNames])

  // ---------------------------------------------------------------------------
  // Selected node helpers
  // ---------------------------------------------------------------------------

  const selectedNode = selectedNodeId ? nodesForCanvas.find((n) => n.id === selectedNodeId) : null
  const selectedNodeTypeSchema = selectedNode ? (nodeTypeMap[selectedNode.data.node_type] ?? null) : null

  return (
    <div style={styles.root}>
      <Palette nodeTypes={paletteTypes} pandasCategories={pandasCategories} templates={templates} />

      <div style={styles.canvasWrapper}>
        <WorkspaceBar
          workspace={workspace}
          onWorkspaceChange={handleWorkspaceChange}
          onNewPipeline={workspace ? () => setShowNewPipelineModal(true) : undefined}
          onLoad={() => setShowLoadModal(true)}
          onSave={savePipeline}
          onSaveToWorkspace={pipelineFilePath ? saveToWorkspace : undefined}
          onRun={handleSubmitRun}
          onOpenVariables={() => setShowVariablesPanel(true)}
          onOpenHistory={() => setShowRunHistory(true)}
          onOpenTransforms={() => setShowTransformEditor(true)}
          yamlPreviewOpen={yamlPreviewOpen}
          onToggleYamlPreview={() => setYamlPreviewOpen((v) => !v)}
          nodeCount={nodes.length}
          runActive={activeRun !== null || activeSession?.status === 'running'}
          hasUncommittedChanges={hasUncommittedChanges}
          canUndo={canUndo}
          canRedo={canRedo}
          onUndo={handleUndo}
          onRedo={handleRedo}
          pipelineName={pipelineName}
        />

        {(validationErrors.length > 0 || validationWarnings.length > 0) && (
          <div style={styles.validationBanner}>
            {validationErrors.map((e, i) => (
              <span key={`e${i}`} style={styles.validationError}>⚠ {e}</span>
            ))}
            {validationWarnings.map((w, i) => (
              <span key={`w${i}`} style={styles.validationWarning}>⚠ {w}</span>
            ))}
          </div>
        )}

        <div style={styles.canvasRow}>
          <div ref={reactFlowWrapper} style={styles.canvas}>
            <ReactFlow
              nodes={nodesForCanvas}
              edges={edges}
              nodeTypes={nodeTypes}
              edgeTypes={edgeTypes}
              onNodesChange={onNodesChangeWrapped}
              onEdgesChange={onEdgesChangeWrapped}
              onConnect={onConnect}
              onNodeClick={onNodeClick}
              onPaneClick={onPaneClick}
              onNodeContextMenu={onNodeContextMenu}
              onDrop={onDrop}
              onDragOver={onDragOver}
              deleteKeyCode={['Delete', 'Backspace']}
              fitView
              colorMode="dark"
            >
              <Background />
              <Controls />
              <MiniMap nodeColor="#313244" maskColor="#11111b88" />
            </ReactFlow>
          </div>

          {yamlPreviewOpen && <YamlPreviewPanel yaml={previewYaml} />}
        </div>
      </div>

      {selectedNode && (
        <NodeConfigPanel
          key={selectedNode.id}
          nodeId={selectedNode.id}
          data={selectedNode.data}
          nodeTypeSchema={selectedNodeTypeSchema}
          pandasTransformEntry={selectedNodeId ? nodeTransformMap[selectedNodeId] : undefined}
          inputSchemas={getInputSchemas(selectedNode.id)}
          variableNames={variableNames}
          workspace={workspace || undefined}
          onUpdate={handleParamUpdate}
          onUpdateDqChecks={handleDqChecksUpdate}
          onExecute={handleExecuteNode}
          onPreview={handlePreviewNode}
          onRunSqlDraft={handleRunSqlDraft}
          onFetchLineage={activeSession ? handleFetchLineage : undefined}
          onDelete={handleDeleteNode}
          onClone={handleCloneNode}
          onClose={() => setSelectedNodeId(null)}
          onTemplateSaved={() => fetchTemplates(workspace || undefined).then(setRemoteTemplates).catch(() => {})}
          pipelineDir={pipelineDir ?? undefined}
          onSetTemplate={handleSetTemplate}
          bottomOffset={activeSession ? 224 : activeRun ? 44 : 0}
        />
      )}

      {activeSession && (
        <SessionPanel
          session={activeSession}
          nodeStatuses={sessionNodeStatuses}
          onDismiss={handleDismissSession}
          onSessionUpdate={handleSessionUpdate}
          currentPipelineYaml={currentPipelineJson}
          currentVariablesYaml={variablesYaml}
          staleNodeIds={staleNodeIds}
          onReexecute={() => startSessionWebSocket(activeSession.session_id)}
        />
      )}

      {activeRun && !activeSession && (
        <RunPanel
          run={activeRun}
          nodeStatuses={nodeStatuses}
          onDismiss={handleDismissRun}
        />
      )}

      {showLoadModal && (
        <LoadPipelineModal
          workspace={workspace}
          onLoad={handleLoadPipeline}
          onClose={() => setShowLoadModal(false)}
        />
      )}

      {showNewPipelineModal && workspace && (
        <NewPipelineModal
          workspace={workspace}
          onConfirm={handleNewPipeline}
          onClose={() => setShowNewPipelineModal(false)}
        />
      )}

      {showTransformEditor && workspace && (
        <TransformEditorPanel
          workspace={workspace}
          pipelineDir={pipelineDir}
          onClose={() => setShowTransformEditor(false)}
          onTransformsSaved={refreshTransforms}
        />
      )}

      {showRunHistory && (
        <RunHistoryPanel
          onClose={() => setShowRunHistory(false)}
          currentPipelineName={pipelineName !== 'Untitled' ? pipelineName : undefined}
          onBranch={(session) => {
            setActiveSession(session)
            setSessionNodeStatuses({})
            setActiveRun(null)
            setShowRunHistory(false)
            startSessionWebSocket(session.session_id)
          }}
        />
      )}

      {showRunVarsModal && (
        <RunVariablesModal
          baseVariables={variablesYaml ? (yaml.load(variablesYaml) as Record<string, unknown> ?? {}) : {}}
          declarations={variableDeclarations}
          onRun={handleConfirmRun}
          onCancel={() => setShowRunVarsModal(false)}
        />
      )}

      {contextMenu && (
        <div
          style={{ ...styles.contextMenu, left: contextMenu.x, top: contextMenu.y }}
          onMouseLeave={() => setContextMenu(null)}
        >
          <button style={styles.contextMenuItem} onClick={() => { setContextMenu(null); handleCloneNode(contextMenu.nodeId) }}>
            ⧉ Clone
          </button>
          <button style={{ ...styles.contextMenuItem, color: '#f38ba8' }} onClick={() => { setContextMenu(null); handleDeleteNode(contextMenu.nodeId) }}>
            ⌫ Delete
          </button>
          <div style={styles.contextMenuSeparator} />
          {activeSession && activeSession.status === 'active' && (
            <button style={styles.contextMenuItem} onClick={() => handleInvalidateNode(contextMenu.nodeId)}>
              ⟳ Mark stale (force re-run)
            </button>
          )}
          <button style={styles.contextMenuItem} onClick={() => handleRerunFromNode(contextMenu.nodeId)}>
            ▶ Rerun from here
          </button>
        </div>
      )}

      {showVariablesPanel && (
        <VariablesPanel
          workspace={workspace}
          declarations={variableDeclarations}
          onClose={() => {
            setShowVariablesPanel(false)
            // Reload variables in case the user saved changes
            if (workspace) {
              fetchWorkspaceVariables(workspace)
                .then((data) => {
                  if (Object.keys(data.variables).length > 0) {
                    import('js-yaml').then((yamlLib) => setVariablesYaml(yamlLib.dump(data.variables)))
                  } else {
                    setVariablesYaml(null)
                  }
                })
                .catch(() => {})
            }
          }}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pipeline serialisation helpers
// ---------------------------------------------------------------------------

function buildPipelineObject(nodes: Node<BuilderNodeData>[], edges: Edge[]) {
  const inputMap: Record<string, string[]> = {}
  for (const edge of edges) {
    if (!inputMap[edge.target]) inputMap[edge.target] = []
    inputMap[edge.target].push(edge.source)
  }
  const nodeSpecs = nodes.map((n) => {
    const spec: Record<string, unknown> = {
      id: n.id,
      type: n.data.node_type,
      inputs: inputMap[n.id] ?? [],
      output: !['sql_exec', 'export_dta', 'push_odbc'].includes(n.data.node_type) ? n.id : null,
      params: n.data.params ?? {},
    }
    if (n.data.template_file) spec.template = n.data.template_file
    if (n.data.description) spec.description = n.data.description
    if (n.data.dq_checks && (n.data.dq_checks as unknown[]).length > 0)
      spec.dq_checks = n.data.dq_checks
    return spec
  })
  // Include templates section whenever at least one node uses a template file
  const hasTemplates = nodes.some((n) => n.data.template_file)
  const templatesSection = hasTemplates ? { templates: { dir: 'templates' } } : {}
  return { duckdb: { path: 'pipeline.duckdb' }, ...templatesSection, nodes: nodeSpecs }
}

/** JSON-encoded pipeline for service calls (JSON is valid YAML for the service). */
function buildPipelineJson(nodes: Node<BuilderNodeData>[], edges: Edge[]): string {
  const obj = buildPipelineObject(nodes, edges)
  return JSON.stringify({ ...obj, duckdb: { path: ':memory:' } })
}

// ---------------------------------------------------------------------------
// Graph traversal helpers
// ---------------------------------------------------------------------------

function getDownstreamIds(nodeId: string, edges: Edge[]): string[] {
  const seen = new Set<string>([nodeId])
  const queue = edges.filter((e) => e.source === nodeId).map((e) => e.target)
  const out: string[] = []
  while (queue.length) {
    const id = queue.shift()!
    if (seen.has(id)) continue
    seen.add(id)
    out.push(id)
    for (const e of edges) {
      if (e.source === id && !seen.has(e.target)) queue.push(e.target)
    }
  }
  return out
}

function getUpstreamIds(nodeId: string, edges: Edge[]): string[] {
  const seen = new Set<string>([nodeId])
  const queue = edges.filter((e) => e.target === nodeId).map((e) => e.source)
  const out: string[] = []
  while (queue.length) {
    const id = queue.shift()!
    if (seen.has(id)) continue
    seen.add(id)
    out.push(id)
    for (const e of edges) {
      if (e.target === id && !seen.has(e.source)) queue.push(e.source)
    }
  }
  return out
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: 'flex',
    height: '100vh',
    background: '#11111b',
    color: '#cdd6f4',
    fontFamily: "'Inter', 'Segoe UI', sans-serif",
    overflow: 'hidden',
  },
  canvasWrapper: { flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', minWidth: 0 },
  validationBanner: {
    background: '#f38ba811', borderBottom: '1px solid #f38ba844',
    padding: '4px 14px', display: 'flex', flexWrap: 'wrap', gap: 12, flexShrink: 0,
  },
  validationError: { fontSize: 11, color: '#f38ba8' },
  validationWarning: { fontSize: 11, color: '#f9e2af' },
  canvasRow: { flex: 1, display: 'flex', overflow: 'hidden' },
  canvas: { flex: 1, position: 'relative' },
  contextMenu: {
    position: 'fixed',
    zIndex: 9999,
    background: '#1e1e2e',
    border: '1px solid #45475a',
    borderRadius: 6,
    boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
    minWidth: 160,
    padding: '4px 0',
  },
  contextMenuItem: {
    display: 'block',
    width: '100%',
    padding: '8px 14px',
    background: 'none',
    border: 'none',
    color: '#cdd6f4',
    fontSize: 13,
    textAlign: 'left',
    cursor: 'pointer',
  },
  contextMenuSeparator: {
    borderTop: '1px solid #45475a',
    margin: '4px 0',
  },
}
