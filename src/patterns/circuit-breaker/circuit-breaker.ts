import { logger } from '../../config/logger';

export enum CircuitState {
  CLOSED = 'CLOSED', // Normal operation: requests pass through
  OPEN = 'OPEN', // Tripped: fail-fast immediately without invoking external resource
  HALF_OPEN = 'HALF_OPEN', // Probe: testing if external service has recovered
}

export interface CircuitBreakerOptions {
  name: string;
  failureThreshold?: number; // Consecutive failures before tripping (default: 5)
  recoveryTimeoutMs?: number; // Time to wait in OPEN state before trying HALF_OPEN (default: 30000ms)
  successThreshold?: number; // Consecutive successes in HALF_OPEN to close circuit (default: 2)
}

export class CircuitBreakerOpenException extends Error {
  constructor(public readonly circuitName: string, public readonly retryAfterMs: number) {
    super(`Circuit breaker '${circuitName}' is OPEN. Requests are temporarily blocked to prevent downstream overload.`);
    this.name = 'CircuitBreakerOpenException';
  }
}

export class CircuitBreaker {
  public readonly name: string;
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount = 0;
  private successCount = 0;
  private nextAttemptTime = 0;
  private readonly failureThreshold: number;
  private readonly recoveryTimeoutMs: number;
  private readonly successThreshold: number;

  constructor(options: CircuitBreakerOptions) {
    this.name = options.name;
    this.failureThreshold = options.failureThreshold ?? 5;
    this.recoveryTimeoutMs = options.recoveryTimeoutMs ?? 30000;
    this.successThreshold = options.successThreshold ?? 2;
  }

  public getState(): CircuitState {
    // If OPEN and recovery timeout has passed, transition to HALF_OPEN
    if (this.state === CircuitState.OPEN && Date.now() >= this.nextAttemptTime) {
      this.transitionTo(CircuitState.HALF_OPEN);
    }
    return this.state;
  }

  public isOpen(): boolean {
    return this.getState() === CircuitState.OPEN;
  }

  /**
   * Executes a protected action through the circuit breaker.
   */
  async execute<T>(action: () => Promise<T>): Promise<T> {
    const currentState = this.getState();

    if (currentState === CircuitState.OPEN) {
      const remainingMs = Math.max(0, this.nextAttemptTime - Date.now());
      logger.warn(
        { circuit: this.name, state: this.state, retryAfterMs: remainingMs },
        'Circuit is OPEN. Fast-failing request.'
      );
      throw new CircuitBreakerOpenException(this.name, remainingMs);
    }

    try {
      const result = await action();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure(error);
      throw error;
    }
  }

  private onSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      logger.info(
        { circuit: this.name, successCount: this.successCount, target: this.successThreshold },
        'Circuit breaker probe succeeded in HALF_OPEN'
      );
      if (this.successCount >= this.successThreshold) {
        this.transitionTo(CircuitState.CLOSED);
      }
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount = 0;
    }
  }

  private onFailure(error: unknown): void {
    const errorMsg = error instanceof Error ? error.message : String(error);

    if (this.state === CircuitState.HALF_OPEN) {
      logger.warn(
        { circuit: this.name, error: errorMsg },
        'Probe request failed in HALF_OPEN. Re-opening circuit.'
      );
      this.transitionTo(CircuitState.OPEN);
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount++;
      logger.warn(
        { circuit: this.name, failureCount: this.failureCount, threshold: this.failureThreshold, error: errorMsg },
        'Failure recorded in circuit breaker'
      );
      if (this.failureCount >= this.failureThreshold) {
        this.transitionTo(CircuitState.OPEN);
      }
    }
  }

  private transitionTo(newState: CircuitState): void {
    const oldState = this.state;
    this.state = newState;

    if (newState === CircuitState.OPEN) {
      this.nextAttemptTime = Date.now() + this.recoveryTimeoutMs;
      logger.error(
        {
          circuit: this.name,
          from: oldState,
          to: newState,
          recoveryTimeoutMs: this.recoveryTimeoutMs,
          nextAttemptAt: new Date(this.nextAttemptTime).toISOString(),
        },
        '🚨 Circuit breaker tripped to OPEN! Fast-failing downstream requests.'
      );
    } else if (newState === CircuitState.HALF_OPEN) {
      this.successCount = 0;
      logger.info(
        { circuit: this.name, from: oldState, to: newState },
        '🟡 Circuit breaker transitioned to HALF_OPEN (probing downstream recovery)'
      );
    } else if (newState === CircuitState.CLOSED) {
      this.failureCount = 0;
      this.successCount = 0;
      logger.info(
        { circuit: this.name, from: oldState, to: newState },
        '✅ Circuit breaker recovered and closed. Normal traffic resumed.'
      );
    }
  }

  /**
   * Diagnostic details
   */
  getDiagnostics() {
    return {
      name: this.name,
      state: this.getState(),
      failureCount: this.failureCount,
      successCount: this.successCount,
      failureThreshold: this.failureThreshold,
      recoveryTimeoutMs: this.recoveryTimeoutMs,
      nextAttemptAt: this.state === CircuitState.OPEN ? new Date(this.nextAttemptTime).toISOString() : null,
    };
  }
}
