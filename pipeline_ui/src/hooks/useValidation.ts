import { useEffect, useRef, useState } from 'react'
import { validatePipeline } from '../api/client'

/**
 * Debounced pipeline validation hook.
 *
 * Calls POST /pipelines/validate after the pipeline YAML stops changing for
 * `debounceMs` milliseconds. Returns validation errors and warnings separately.
 * Errors block execution; warnings are surfaced as amber advisories.
 */
export function useValidation(
  pipelineJson: string,
  variablesYaml?: string,
  debounceMs = 800,
  pipelineDir?: string | null,
  workspace?: string | null,
) {
  const [errors, setErrors] = useState<string[]>([])
  const [warnings, setWarnings] = useState<string[]>([])
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current)

    timerRef.current = setTimeout(async () => {
      if (!pipelineJson || pipelineJson === '{}') return
      try {
        const result = await validatePipeline(
          pipelineJson,
          undefined,
          variablesYaml,
          pipelineDir ?? undefined,
          workspace ?? undefined,
        )
        setErrors(result.valid ? [] : result.errors)
        setWarnings(result.warnings ?? [])
      } catch {
        // Don't surface network errors as validation errors
      }
    }, debounceMs)

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [pipelineJson, variablesYaml, debounceMs, pipelineDir, workspace])

  return { errors, warnings }
}
