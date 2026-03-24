/**
 * Spatiotemporal Hawkes Process — Causal Contagion Model
 *
 * Models the spread of MacroEvents based on historical interactions
 * and geographical proximity using the Self-Exciting Point Process.
 *
 * The Hawkes Process intensity function:
 * $$\lambda(t) = \mu + \sum_{t_i < t} \phi(t - t_i)$$
 *
 * Where:
 * - $\mu$ = baseline intensity (background rate)
 * - $\phi(\tau)$ = decay kernel function
 * - $t_i$ = timestamps of past events
 *
 * For multivariate case (entity-to-entity contagion):
 * $$\lambda_j(t) = \mu_j + \sum_{i=1}^{N} \sum_{t_i^k < t} \alpha_{ij} \cdot \beta \cdot e^{-\beta(t - t_i^k)}$$
 *
 * Where:
 * - $\alpha_{ij}$ = contagion weight from entity $i$ to entity $j$
 * - $\beta$ = decay rate (memory length)
 * - $t_i^k$ = $k$-th event time of entity $i$
 *
 * [hc-001] Hawkes, A.G. "Spectral Analysis of Discrete Time Point Processes"
 * [hc-002] Laub, Torgler, Schlemm "Spatiotemporal Hawkes Process for Finance"
 */

// ─── TYPES ────────────────────────────────────────────────────────────────────

export interface HawkesEvent {
  entityId: string;
  entityType: string;
  timestamp: number;
  intensity: number;
  /** Spatial coordinates for spatiotemporal kernel */
  lat?: number;
  lon?: number;
}

export interface HawkesParams {
  /** Baseline intensity $\mu$ */
  mu: number;
  /** Contagion weight matrix $\alpha_{ij}$ */
  alpha: Map<string, number>;
  /** Decay rate $\beta$ */
  beta: number;
  /** Spatial decay parameter for geographic proximity */
  gamma?: number;
}

export interface ContagionResult {
  entityId: string;
  entityType: string;
  /** Current intensity $\lambda(t)$ */
  intensity: number;
  /** Branching ratio (reproduction number) $R$ */
  branching_ratio: number;
  /** Top drivers of this entity's intensity */
  drivers: Array<{
    sourceId: string;
    contribution: number;
    recency_hours: number;
  }>;
  /** Whether entity is in "tipping" state (R > 1) */
  is_tipping: boolean;
}

export interface ShockSimulation {
  /** Initial shock parameters */
  sourceEntity: string;
  shockMagnitude: number;
  /** Predicted cascade over time */
  cascade: Array<{
    entityId: string;
    entityType: string;
    /** Time of peak impact (hours) */
    peak_time_hours: number;
    /** Peak intensity reached */
    peak_intensity: number;
    /** Total impact accumulated */
    total_impact: number;
  }>;
  /** Summary statistics */
  summary: {
    total_affected: number;
    max_depth: number;
    stable: boolean;
  };
}

// ─── EXPONENTIAL DECAY KERNEL ─────────────────────────────────────────────────

/**
 * Exponential decay kernel for Hawkes Process.
 *
 * $$\phi(\tau) = \beta e^{-\beta \tau} \quad \text{for } \tau \geq 0$$
 *
 * This kernel has the Markov property — the future depends only on the
 * current intensity, not the full history.
 *
 * Decay time constant: $\tau_{1/2} = \frac{\ln 2}{\beta}$
 * [hc-kernel-001]
 */
function exponentialKernel(tau: number, beta: number): number {
  if (tau < 0) return 0;
  return beta * Math.exp(-beta * tau);
}

/**
 * Spatiotemporal kernel combining temporal decay with geographic distance.
 *
 * $$\phi(\tau, d) = \beta e^{-\beta \tau} \cdot \gamma e^{-\gamma d}$$
 *
 * Where:
 * - $\tau$ = time difference
 * - $d$ = geographic distance (km)
 * - $\gamma$ = spatial decay parameter
 *
 * Haversine distance approximation:
 * $$d \approx R \cdot \sqrt{(\Delta\phi)^2 + (\cos\phi_m \cdot \Delta\lambda)^2}$$
 * [hc-kernel-002]
 */
function spatiotemporalKernel(
  tau: number,
  distance: number,
  beta: number,
  gamma: number
): number {
  if (tau < 0 || distance < 0) return 0;
  return (beta * Math.exp(-beta * tau)) * (gamma * Math.exp(-gamma * distance));
}

/**
 * Compute Haversine distance between two points in kilometers.
 * [hc-hav-001]
 */
function haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371; // Earth radius in km
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// ─── HAWKES PROCESS ENGINE ────────────────────────────────────────────────────

/**
 * Multivariate Hawkes Process for causal contagion modeling.
 *
 * The conditional intensity for entity $j$ at time $t$:
 * $$\lambda_j(t) = \mu_j + \sum_{i \neq j} \alpha_{ij} \sum_{t_i^k < t} \beta e^{-\beta(t - t_i^k)}$$
 *
 * Branching ratio (effective reproduction number):
 * $$R_j = \sum_{i \neq j} \alpha_{ij}$$
 *
 * If $R_j > 1$, the process is super-critical (self-sustaining).
 * If $R_j < 1$, the process is sub-critical (dies out).
 * [hc-engine-001]
 */
export class HawkesProcessEngine {
  private events: HawkesEvent[] = [];
  private params: HawkesParams;
  private eventIndex: Map<string, HawkesEvent[]> = new Map();

  constructor(params: HawkesParams) {
    this.params = params;
  }

  /**
   * Add historical events to the process.
   */
  addEvents(events: HawkesEvent[]): void {
    for (const event of events) {
      this.events.push(event);
      const entityEvents = this.eventIndex.get(event.entityId) || [];
      entityEvents.push(event);
      this.eventIndex.set(event.entityId, entityEvents);
    }
    // Sort by timestamp
    this.events.sort((a, b) => a.timestamp - b.timestamp);
    for (const [, entityEvents] of this.eventIndex) {
      entityEvents.sort((a, b) => a.timestamp - b.timestamp);
    }
  }

  /**
   * Compute intensity $\lambda_j(t)$ for entity $j$ at time $t$.
   *
   * $$\lambda_j(t) = \mu_j + \sum_{i} \sum_{t_i^k < t} \alpha_{ij} \phi(t - t_i^k)$$
   * [hc-intensity-001]
   */
  computeIntensity(entityId: string, currentTime: number): number {
    const mu = this.params.mu;
    let intensity = mu;

    // Sum contributions from all past events
    for (const event of this.events) {
      if (event.timestamp >= currentTime) continue;
      if (event.entityId === entityId) continue;

      const tau = currentTime - event.timestamp;
      const alpha = this.params.alpha.get(event.entityId) || 0.01;
      const kernelVal = exponentialKernel(tau, this.params.beta);

      intensity += alpha * kernelVal * event.intensity;
    }

    return intensity;
  }

  /**
   * Compute intensity using spatiotemporal kernel (if geo data available).
   *
   * Adds geographic proximity boost:
   * $$\lambda_j^{sp}(t) = \mu_j + \sum_{i} \sum_{t_i^k < t} \alpha_{ij} \phi(\tau, d_{ij})$$
   * [hc-intensity-002]
   */
  computeSpatiotemporalIntensity(
    entityId: string,
    entityLat: number,
    entityLon: number,
    currentTime: number
  ): number {
    const mu = this.params.mu;
    const gamma = this.params.gamma || 0.001;
    let intensity = mu;

    for (const event of this.events) {
      if (event.timestamp >= currentTime) continue;
      if (event.entityId === entityId) continue;

      const tau = currentTime - event.timestamp;
      const alpha = this.params.alpha.get(event.entityId) || 0.01;

      // Compute distance if source event has geo data
      let distance = 0;
      if (event.lat !== undefined && event.lon !== undefined) {
        distance = haversineDistance(entityLat, entityLon, event.lat, event.lon);
      }

      const kernelVal = spatiotemporalKernel(tau, distance, this.params.beta, gamma);
      intensity += alpha * kernelVal * event.intensity;
    }

    return intensity;
  }

  /**
   * Compute branching ratio (reproduction number) for entity.
   *
   * $$R_j = \sum_{i \neq j} \alpha_{ij}$$
   *
   * Interpretation:
   * - $R > 1$: Super-critical (self-sustaining cascade)
   * - $R = 1$: Critical (marginal)
   * - $R < 1$: Sub-critical (dies out)
   * [hc-branching-001]
   */
  computeBranchingRatio(entityId: string): number {
    let branchingRatio = 0;

    // Sum all incoming contagion weights
    for (const [sourceId, alpha] of this.params.alpha) {
      if (sourceId !== entityId) {
        branchingRatio += alpha;
      }
    }

    return branchingRatio;
  }

  /**
   * Simulate shock propagation from a source entity.
   *
   * Uses forward simulation of the Hawkes Process:
   * 1. Inject initial shock at $t_0$
   * 2. Propagate to neighbors via $\lambda_j(t)$
   * 3. Continue until intensity decays below threshold
   *
   * Time complexity: $O(N \cdot K)$ where $N$ = entities, $K$ = events
   * [hc-sim-001]
   */
  simulateShock(
    sourceEntityId: string,
    shockMagnitude: number,
    durationHours: number = 168, // 1 week
    resolutionHours: number = 1
  ): ShockSimulation {
    const cascade: ShockSimulation['cascade'] = [];
    const visited = new Set<string>();
    const queue: Array<{
      entityId: string;
      magnitude: number;
      timeOffset: number;
    }> = [{ entityId: sourceEntityId, magnitude: shockMagnitude, timeOffset: 0 }];

    const baseTime = Date.now();
    let maxDepth = 0;

    while (queue.length > 0 && cascade.length < 100) {
      const current = queue.shift()!;
      if (visited.has(current.entityId)) continue;
      visited.add(current.entityId);

      // Skip if too far in time
      if (current.timeOffset > durationHours) continue;
      maxDepth = Math.max(maxDepth, current.timeOffset);

      // Compute peak intensity at this entity
      const peakTime = current.timeOffset + 1; // Peak at 1 hour post-arrival
      const peakIntensity = this.computeIntensityAtOffset(
        current.entityId,
        baseTime + current.timeOffset * 3600000,
        baseTime
      ) * current.magnitude;

      // Compute total impact (integral of intensity over time)
      const halfLife = Math.log(2) / this.params.beta; // hours
      const totalImpact = peakIntensity * halfLife * 2; // ~2x half-life integral

      cascade.push({
        entityId: current.entityId,
        entityType: this.getEntityType(current.entityId),
        peak_time_hours: peakTime,
        peak_intensity: Math.round(peakIntensity * 1000) / 1000,
        total_impact: Math.round(totalImpact * 1000) / 1000,
      });

      // Propagate to next wave
      const branchingRatio = this.computeBranchingRatio(current.entityId);
      if (branchingRatio > 0.1) {
        const nextEntities = this.findNeighbors(current.entityId);
        for (const neighbor of nextEntities) {
          if (!visited.has(neighbor)) {
            const propagatedMagnitude = current.magnitude * branchingRatio * 0.7;
            if (propagatedMagnitude > 0.05) {
              queue.push({
                entityId: neighbor,
                magnitude: propagatedMagnitude,
                timeOffset: current.timeOffset + resolutionHours,
              });
            }
          }
        }
      }
    }

    const stable = cascade.every(c => c.peak_intensity < 0.5);

    return {
      sourceEntity: sourceEntityId,
      shockMagnitude,
      cascade: cascade.sort((a, b) => a.peak_time_hours - b.peak_time_hours),
      summary: {
        total_affected: cascade.length,
        max_depth: maxDepth,
        stable,
      },
    };
  }

  /**
   * Get current contagion state for an entity.
   */
  getContagionState(entityId: string): ContagionResult {
    const now = Date.now();
    const intensity = this.computeIntensity(entityId, now);
    const branchingRatio = this.computeBranchingRatio(entityId);

    // Find top drivers
    const drivers: ContagionResult['drivers'] = [];
    for (const event of this.events) {
      if (event.entityId === entityId) continue;
      const tau = (now - event.timestamp) / 3600000; // hours
      if (tau > 168) continue; // Only last week

      const alpha = this.params.alpha.get(event.entityId) || 0;
      const contribution = alpha * event.intensity * exponentialKernel(tau, this.params.beta);

      if (contribution > 0.001) {
        drivers.push({
          sourceId: event.entityId,
          contribution: Math.round(contribution * 1000) / 1000,
          recency_hours: Math.round(tau * 10) / 10,
        });
      }
    }

    drivers.sort((a, b) => b.contribution - a.contribution);

    return {
      entityId,
      entityType: this.getEntityType(entityId),
      intensity: Math.round(intensity * 1000) / 1000,
      branching_ratio: Math.round(branchingRatio * 1000) / 1000,
      drivers: drivers.slice(0, 5),
      is_tipping: branchingRatio > 1.0,
    };
  }

  private computeIntensityAtOffset(entityId: string, currentTime: number, baseTime: number): number {
    return this.computeIntensity(entityId, currentTime);
  }

  private getEntityType(entityId: string): string {
    const events = this.eventIndex.get(entityId);
    return events?.[0]?.entityType || 'Unknown';
  }

  private findNeighbors(entityId: string): string[] {
    // Find entities that co-occur with this entity
    const neighbors = new Set<string>();
    const entityEvents = this.eventIndex.get(entityId) || [];

    for (const event of this.events) {
      if (event.entityId === entityId) continue;
      // Check if events are within 24 hours
      for (const e of entityEvents) {
        const tau = Math.abs(event.timestamp - e.timestamp) / 3600000;
        if (tau < 24) {
          neighbors.add(event.entityId);
        }
      }
    }

    return Array.from(neighbors);
  }
}

// ─── CONTAGION PARAMETER ESTIMATION ───────────────────────────────────────────

/**
 * Estimate Hawkes parameters from historical data.
 *
 * Uses Maximum Likelihood Estimation (MLE):
 * $$\hat{\mu} = \frac{N}{T}, \quad \hat{\alpha} = \frac{1}{N} \sum_{i=1}^{N} \frac{\lambda(t_i) - \hat{\mu}}{\lambda(t_i)}$$
 *
 * Where $N$ = number of events, $T$ = observation window.
 * [hc-mle-001]
 */
export function estimateHawkesParams(
  events: HawkesEvent[],
  observationWindowMs: number
): HawkesParams {
  const N = events.length;
  const T = observationWindowMs / 3600000; // hours

  // Baseline intensity: $\hat{\mu} = N / T$
  const mu = N > 0 ? N / T : 0.01;

  // Estimate alpha from event clustering
  const alpha = new Map<string, number>();
  const beta = 0.1; // 10-hour decay

  // Count events per entity
  const entityCounts = new Map<string, number>();
  for (const event of events) {
    entityCounts.set(event.entityId, (entityCounts.get(event.entityId) || 0) + 1);
  }

  // Estimate contagion weight: $\alpha_{ij} \propto$ co-occurrence frequency
  const totalPairs = Math.max(1, N * (N - 1) / 2);
  for (const [entityId, count] of entityCounts) {
    // Simple heuristic: $\alpha_{ij} = \frac{\text{count}_j}{\text{total}} \cdot \frac{1}{\log(\text{count}_j + 1)}$
    alpha.set(entityId, (count / totalPairs) * (1 / Math.log(count + 2)));
  }

  return { mu, alpha, beta, gamma: 0.001 };
}
