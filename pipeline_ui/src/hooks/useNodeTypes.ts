import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchNodeTypes, fetchPalette, fetchPandasTransforms, fetchTransformMtimes } from '../api/client'
import type { NodeTypeSchema, PaletteResponse, PandasTransformCategory } from '../types'

const WATCHER_INTERVAL_MS = 3000

export function useNodeTypes(workspace?: string) {
  const [nodeTypes, setNodeTypes] = useState<NodeTypeSchema[]>([])
  const [pandasCategories, setPandasCategories] = useState<PandasTransformCategory[]>([])
  const [paletteData, setPaletteData] = useState<PaletteResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  // Mtime watcher — polls for transform file changes and bumps refreshKey
  const lastMtimesRef = useRef<Record<string, number>>({})
  const watcherRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      fetchNodeTypes(),
      fetchPandasTransforms(workspace || undefined),
      fetchPalette(workspace || undefined),
    ])
      .then(([types, pandas, palette]) => {
        setNodeTypes(types)
        setPandasCategories(pandas)
        setPaletteData(palette)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [workspace, refreshKey])

  // Start/stop the mtime watcher when workspace changes
  useEffect(() => {
    if (watcherRef.current) {
      clearInterval(watcherRef.current)
      watcherRef.current = null
    }
    if (!workspace) return

    watcherRef.current = setInterval(async () => {
      try {
        const mtimes = await fetchTransformMtimes(workspace)
        const prev = lastMtimesRef.current
        const changed =
          Object.keys(mtimes).some((k) => mtimes[k] !== prev[k]) ||
          Object.keys(prev).some((k) => !(k in mtimes))
        lastMtimesRef.current = mtimes
        if (changed) {
          setRefreshKey((k) => k + 1)
        }
      } catch { /* workspace may not have a transforms dir — ignore */ }
    }, WATCHER_INTERVAL_MS)

    return () => {
      if (watcherRef.current) clearInterval(watcherRef.current)
    }
  }, [workspace])

  const refreshTransforms = useCallback(() => setRefreshKey(k => k + 1), [])

  return {
    nodeTypes,
    nodeTypeMap: Object.fromEntries(nodeTypes.map((nt) => [nt.type, nt])),
    pandasCategories,
    paletteData,
    loading,
    error,
    refreshTransforms,
  }
}
