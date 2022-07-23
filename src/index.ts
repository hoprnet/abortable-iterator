import { AbortError } from './abort-error.js'
import { getIterator } from 'get-iterator'
import type { Duplex, Source, Sink } from 'it-stream-types'

export interface Options<T> {
  onReturnError?: (err: Error) => void
  onAbort?: (source: Source<T>) => void
  abortMessage?: string
  abortCode?: string
  returnOnAbort?: boolean
}

enum Result {
  ABORTED,
  PAYLOAD
}

type AbortedEvent = {
  type: Result.ABORTED
  err: any
}

type PayloadEvent<T> = {
  type: Result.PAYLOAD
  value: IteratorResult<T>
}

type SourceEvent<T> = AbortedEvent | PayloadEvent<T>

// Wrap an iterator to make it abortable, allow cleanup when aborted via onAbort
export function abortableSource <T> (source: Source<T>, signal: AbortSignal, options?: Options<T>) {
  const opts: Options<T> = options ?? {}
  const iterator = getIterator<T>(source)

  async function* abortable () {
    // Catch abort that has happened before
    if (signal.aborted) {
      const { abortMessage, abortCode } = opts
      throw new AbortError(abortMessage, abortCode)
    }

    let done = false
    let toYield: T | undefined

    let abortHandler: ((event: any) => void) | undefined
    const abortPromise = new Promise<AbortedEvent>((resolve) => {
      abortHandler = (event: any) => {
        const { abortMessage, abortCode } = opts
        const err = new AbortError(abortMessage, abortCode)
        resolve({ type: Result.ABORTED, err})
      } 
      signal.addEventListener('abort', abortHandler)
    })

    const nextPayload = async (): Promise<PayloadEvent<T>> => {
      let payload = await  iterator.next()

      return {
        type: Result.PAYLOAD,
        value: payload
      }
    }

    const cleanup = () => {
      done = true
      if (abortHandler != undefined) {
        signal.removeEventListener('abort', abortHandler)
      }
    }
    while (!done) {
      const result: SourceEvent<T> = await Promise.race([abortPromise, nextPayload()]) 

      switch (result.type) {
        case Result.ABORTED:
          cleanup()

          if (opts.onAbort != null) {
            await opts.onAbort(source)
          }

          if (typeof iterator.return === 'function') {
            try {
              const p = iterator.return()

              if (p instanceof Promise) { // eslint-disable-line max-depth
                p.catch(err => {
                  if (opts.onReturnError != null) {
                    opts.onReturnError(err)
                  }
                })
              }
            } catch (err: any) {
              if (opts.onReturnError != null) { // eslint-disable-line max-depth
                opts.onReturnError(err)
              }
            }
          }

          if (opts.returnOnAbort) {
            break
          }


          throw result.err
        case Result.PAYLOAD:
          if (result.value.done) {
            cleanup()

            break
          }

          toYield = result.value.value
          break
        default:
          throw Error(`Invalid result. Received ${JSON.stringify(result)}`)
      }

      if (toYield) {
        yield toYield
      }
    }
  }

  return abortable()
}

export function abortableSink <T, R> (sink: Sink<T, R>, signal: AbortSignal, options?: Options<T>): Sink<T, R> {
  return (source: Source<T>) => sink(abortableSource(source, signal, options))
}

export function abortableDuplex <TSource, TSink = TSource, RSink = Promise<void>> (duplex: Duplex<TSource, TSink, RSink>, signal: AbortSignal, options?: Options<TSource>) {
  return {
    sink: abortableSink(duplex.sink, signal, {
      ...options,
      onAbort: undefined
    }),
    source: abortableSource(duplex.source, signal, options)
  }
}

export { AbortError }
export { abortableSink as abortableTransform }
