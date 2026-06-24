/**
 * Connect/idle timeout helpers shared by the streaming providers. Kept here so Bedrock,
 * Anthropic (and any future provider) wire the same logic rather than re-implementing it.
 */

/**
 * Resolve a timeout option (ms): `undefined`/`null` uses `defaultMs`; a non-positive or
 * non-finite value disables the timeout (returns `0`).
 */
export function resolveTimeoutMs(value: number | null | undefined, defaultMs: number): number {
	if (value === undefined || value === null) return defaultMs;
	if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return 0;
	return Math.floor(value);
}

/**
 * Idle-timeout watchdog for a streaming read loop. A half-open socket (open, but no further
 * events and no FIN/RST) otherwise blocks the underlying `next()` forever. The timer resets on
 * every event; on prolonged silence the generator throws `makeError()`, which callers classify
 * as retryable. The race resolves a sentinel on timeout rather than rejecting, so the throw
 * happens once, at the top of the loop.
 */
export async function* withStreamIdleTimeout<T>(
	source: AsyncIterable<T>,
	idleMs: number,
	makeError: () => Error,
): AsyncGenerator<T> {
	const iterator = source[Symbol.asyncIterator]();
	const timedOut = Symbol("idle-timeout");
	try {
		while (true) {
			let timer: ReturnType<typeof setTimeout> | undefined;
			const idle = new Promise<typeof timedOut>((resolve) => {
				timer = setTimeout(() => resolve(timedOut), idleMs);
			});
			let result: IteratorResult<T> | typeof timedOut;
			try {
				result = await Promise.race([iterator.next(), idle]);
			} finally {
				clearTimeout(timer);
			}
			if (result === timedOut) throw makeError();
			if (result.done) return;
			yield result.value;
		}
	} finally {
		// Release the underlying stream on early exit (timeout / abort / break).
		try {
			await iterator.return?.();
		} catch {
			/* ignore */
		}
	}
}

export interface ConnectTimeout {
	/** Abort signal to combine into the request; aborts when the timer fires. Undefined when disabled. */
	signal: AbortSignal | undefined;
	/** The timeout error, set only once the timer has fired. */
	error: () => Error | undefined;
	/** Stop the timer; call once response headers arrive or the request settles. */
	clear: () => void;
}

/**
 * Pre-stream connect/first-byte timeout, mirroring the codex provider's header-timeout pattern:
 * the timer aborts a signal and records an error rather than throwing for control flow. Combine
 * the returned `signal` into the request, and in the request's `catch` rethrow `error()` when it
 * is set. A pre-stream timeout has produced no tokens, so retrying is safe. A non-positive
 * `connectMs` disables it (returns an undefined signal).
 */
export function createConnectTimeout(connectMs: number, makeError: () => Error): ConnectTimeout {
	if (!(connectMs > 0)) {
		return { signal: undefined, error: () => undefined, clear: () => {} };
	}
	const controller = new AbortController();
	let timedOut: Error | undefined;
	const timer = setTimeout(() => {
		timedOut = makeError();
		controller.abort(timedOut);
	}, connectMs);
	return {
		signal: controller.signal,
		error: () => timedOut,
		clear: () => clearTimeout(timer),
	};
}
