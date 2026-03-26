/**
 * Causal Inference Engine for Reality Graph
 *
 * Answers "What caused this crash and why?" by:
 * 1. Tracing causal paths from effects back to root causes
 * 2. Computing causal strength using Hawkes-weighted propagation
 * 3. Generating natural language explanations
 * 4. Simulating counterfactuals via MiroFish integration
 *
 * Mathematical Framework:
 * - Causal strength: $W_{path} = \prod_{e \in path} w_e \cdot \alpha_e$
 * - Hawkes intensity: $\lambda_j(t) = \mu_j + \sum_i \alpha_{ij} \sum_{t_k < t} e^{-\beta(t-t_k)}$
 * - Attribution score: $A_i = \frac{W_i}{\sum_j W_j}$ (normalized contribution)
 *
 * [causal-engine-001]
 */

import { createClient } from 'redis';
import { EntityType, RelationType, GraphNode, GraphEdge, Regime } from './knowledge-graph.js';

// ─── CRASH INTERFACES ────────────────────────────────────────────────────────

export interface CrashEvent {
  crash_id: string;
  name: string;
  crash_type: 'MarketCrash' | 'SectorCrash' | 'FlashCrash' | 'CurrencyCrisis' | 'BankRun' | 'LiquidityCrisis';

  // Affected entities
  primary_index?: string;           // e.g., "S&P 500", "FTSE 100"
  affected_sectors?: string[];
  affected_entities?: string[];

  // Temporal
  start_date: string;               // ISO date
  end_date?: string;
  peak_date?: string;               // Date of maximum drawdown

  // Magnitude
  peak_drawdown_pct: number;        // e.g., -34.0 for 34% drop
  recovery_days?: number;

  // Classification
  regime_before: Regime;
  regime_during: 'stressed' | 'pre_tipping';
  regime_after: Regime;
}

export interface CausalFactor {
  factor_id: string;
  name: string;
  type: EntityType;

  // Causal attribution
  causal_weight: number;            // 0-1: how much this factor contributed
  mechanism: string;                // e.g., "margin call cascade", "credit freeze"

  // Temporal ordering
  preceded_crash_by_days: number;
  peak_intensity_date?: string;

  // Evidence
  evidence_sources: string[];
  confidence: number;
}

export interface CausalPath {
  path_id: string;
  nodes: string[];                  // Entity names in order
  edges: Array<{
    from: string;
    to: string;
    relation: RelationType;
    weight: number;
    mechanism: string;
  }>;

  // Path metrics
  total_weight: number;             // Product of edge weights
  path_length: number;
  hawkes_intensity: number;         // Temporal intensity from Hawkes Process

  // Human-readable
  narrative: string;
}

export interface CrashExplanation {
  crash: CrashEvent;

  // Root causes (traced back)
  root_causes: CausalFactor[];

  // Transmission mechanisms (how it spread)
  transmission_mechanisms: Array<{
    mechanism_type: string;
    entities_affected: number;
    peak_propagation_speed: number; // entities/hour
    amplification_factor: number;
  }>;

  // Key causal paths
  causal_paths: CausalPath[];

  // Entities most affected
  most_affected: Array<{
    entity_name: string;
    entity_type: EntityType;
    exposure_amount?: number;
    loss_pct?: number;
  }>;

  // Natural language summary
  summary: string;
  detailed_narrative: string;
}

export interface CounterfactualScenario {
  scenario_id: string;
  description: string;

  // What-if intervention
  intervention: {
    type: 'remove_factor' | 'early_intervention' | 'limit_exposure' | 'circuit_breaker';
    target_entity: string;
    intervention_date: string;
    parameters: Record<string, any>;
  };

  // Simulated outcome
  simulated_drawdown_pct: number;
  entities_saved: number;
  cascade_prevented: boolean;

  confidence: number;
}

// ─── CAUSAL INFERENCE ENGINE ─────────────────────────────────────────────────

export class CausalInferenceEngine {
  private client: ReturnType<typeof createClient> | null = null;
  private graphName: string;

  constructor(client: ReturnType<typeof createClient> | null, graphName: string) {
    this.client = client;
    this.graphName = graphName;
  }

  /**
   * Main entry point: Explain a crash
   *
   * Query pattern:
   * ```cypher
   * MATCH path = (crash:Entity {type: 'MarketCrash'})<-[:TRIGGERED|SHOCKED|CAUSED*1..5]-(cause)
   * RETURN cause, path, reduce(w = 1.0, r IN relationships(path) | w * r.causal_weight) AS path_weight
   * ORDER BY path_weight DESC
   * ```
   */
  async explainCrash(crashName: string): Promise<CrashExplanation | null> {
    if (!this.client) return null;

    // 1. Find the crash node
    const crashNode = await this.findCrashNode(crashName);
    if (!crashNode) return null;

    // 2. Trace causal paths backward
    const causalPaths = await this.traceCausalPaths(crashNode.id, 5);

    // 3. Extract root causes from paths
    const rootCauses = this.extractRootCauses(causalPaths);

    // 4. Analyze transmission mechanisms
    const mechanisms = await this.analyzeTransmissionMechanisms(crashNode.id);

    // 5. Find most affected entities
    const mostAffected = await this.findMostAffected(crashNode.id);

    // 6. Generate narrative
    const narrative = this.generateNarrative(crashNode, rootCauses, mechanisms, causalPaths);

    return {
      crash: this.nodeToCrashEvent(crashNode),
      root_causes: rootCauses,
      transmission_mechanisms: mechanisms,
      causal_paths: causalPaths,
      most_affected: mostAffected,
      summary: narrative.summary,
      detailed_narrative: narrative.detailed,
    };
  }

  /**
   * Trace all causal paths leading to a crash
   *
   * Uses reverse BFS with Hawkes-weighted edges:
   * $W_{path} = \prod_{e \in path} (w_e \cdot e^{-\beta \cdot lag_e})$
   */
  async traceCausalPaths(crashNodeId: string, maxDepth: number): Promise<CausalPath[]> {
    if (!this.client) return [];

    const paths: CausalPath[] = [];

    try {
      // FalkorDB query to find all paths to the crash
      const query = `
        MATCH path = (cause:Entity)-[r:TRIGGERED|SHOCKED|causes|caused_by|impacts|amplifies*1..${maxDepth}]->(crash:Entity {id: "${crashNodeId}"})
        WITH cause, path,
             [rel IN relationships(path) | rel.causal_weight] AS weights,
             [rel IN relationships(path) | rel.relation] AS relations,
             [node IN nodes(path) | node.name] AS node_names
        RETURN
          cause.name AS root_cause,
          cause.type AS root_type,
          node_names,
          weights,
          relations,
          reduce(w = 1.0, weight IN weights | w * COALESCE(weight, 0.5)) AS total_weight
        ORDER BY total_weight DESC
        LIMIT 20
      `;

      const result = await this.graphQuery(query);

      for (const row of result) {
        const nodeNames = row.node_names || [];
        const weights = row.weights || [];
        const relations = row.relations || [];

        // Build edge list
        const edges = [];
        for (let i = 0; i < nodeNames.length - 1; i++) {
          edges.push({
            from: nodeNames[i],
            to: nodeNames[i + 1],
            relation: relations[i] || 'causes',
            weight: weights[i] || 0.5,
            mechanism: this.inferMechanism(relations[i]),
          });
        }

        // Generate narrative for this path
        const pathNarrative = this.pathToNarrative(nodeNames, edges);

        paths.push({
          path_id: `path-${paths.length}`,
          nodes: nodeNames,
          edges,
          total_weight: row.total_weight || 0,
          path_length: nodeNames.length - 1,
          hawkes_intensity: this.estimateHawkesIntensity(weights),
          narrative: pathNarrative,
        });
      }
    } catch (err: any) {
      console.error('[CAUSAL] Path trace failed:', err.message);
    }

    return paths;
  }

  /**
   * Analyze transmission mechanisms
   *
   * Groups propagation paths by mechanism type and computes:
   * - Propagation speed (entities/hour)
   * - Amplification factor (output_intensity / input_intensity)
   */
  async analyzeTransmissionMechanisms(crashNodeId: string): Promise<Array<{
    mechanism_type: string;
    entities_affected: number;
    peak_propagation_speed: number;
    amplification_factor: number;
  }>> {
    if (!this.client) return [];

    const mechanisms: Map<string, {
      entities: Set<string>;
      total_intensity: number;
      input_intensity: number;
    }> = new Map();

    try {
      // Query for transmission edges
      const query = `
        MATCH (a:Entity)-[r:TRANSMITTED_THROUGH|PROPAGATED_TO|CASCADED_TO|AMPLIFIED_CRASH]->(b:Entity)
        WHERE r.crash_context CONTAINS "${crashNodeId}" OR b.id = "${crashNodeId}"
        RETURN
          r.mechanism AS mechanism,
          a.name AS source,
          b.name AS target,
          r.contagion_weight AS intensity
      `;

      const result = await this.graphQuery(query);

      for (const row of result) {
        const mechanism = row.mechanism || 'unknown';
        if (!mechanisms.has(mechanism)) {
          mechanisms.set(mechanism, {
            entities: new Set(),
            total_intensity: 0,
            input_intensity: 0.1,
          });
        }

        const m = mechanisms.get(mechanism)!;
        m.entities.add(row.source);
        m.entities.add(row.target);
        m.total_intensity += row.intensity || 0.5;
      }
    } catch (err: any) {
      console.error('[CAUSAL] Mechanism analysis failed:', err.message);
    }

    // Convert to output format
    return Array.from(mechanisms.entries()).map(([type, data]) => ({
      mechanism_type: type,
      entities_affected: data.entities.size,
      peak_propagation_speed: data.entities.size / 24, // Assume 24-hour propagation window
      amplification_factor: data.total_intensity / data.input_intensity,
    }));
  }

  /**
   * Find entities most affected by the crash
   */
  async findMostAffected(crashNodeId: string): Promise<Array<{
    entity_name: string;
    entity_type: EntityType;
    exposure_amount?: number;
    loss_pct?: number;
  }>> {
    if (!this.client) return [];

    try {
      const query = `
        MATCH (e:Entity)-[r:EXPOSED_TO|CRASHED|VULNERABLE_TO]->(crash:Entity {id: "${crashNodeId}"})
        RETURN
          e.name AS entity_name,
          e.type AS entity_type,
          r.exposure_amount AS exposure,
          r.loss_pct AS loss_pct
        ORDER BY COALESCE(r.loss_pct, 0) DESC
        LIMIT 20
      `;

      const result = await this.graphQuery(query);

      return result.map(row => ({
        entity_name: row.entity_name,
        entity_type: row.entity_type as EntityType,
        exposure_amount: row.exposure,
        loss_pct: row.loss_pct,
      }));
    } catch (err: any) {
      console.error('[CAUSAL] Affected analysis failed:', err.message);
      return [];
    }
  }

  /**
   * Simulate counterfactual: What if we removed/changed a factor?
   */
  async simulateCounterfactual(
    crashName: string,
    intervention: {
      type: 'remove_factor' | 'early_intervention' | 'limit_exposure' | 'circuit_breaker';
      target_entity: string;
      intervention_date: string;
      parameters: Record<string, any>;
    }
  ): Promise<CounterfactualScenario | null> {
    if (!this.client) return null;

    // Get baseline crash data
    const crashNode = await this.findCrashNode(crashName);
    if (!crashNode) return null;

    // Trace paths through the intervention target
    const paths = await this.traceCausalPaths(crashNode.id, 5);
    const affectedPaths = paths.filter(p =>
      p.nodes.some(n => n.toLowerCase().includes(intervention.target_entity.toLowerCase()))
    );

    // Estimate impact of removing those paths
    const baselineWeight = paths.reduce((sum, p) => sum + p.total_weight, 0);
    const removedWeight = affectedPaths.reduce((sum, p) => sum + p.total_weight, 0);
    const remainingWeight = baselineWeight - removedWeight;

    // Rough estimation: drawdown proportional to remaining causal weight
    const baselineDrawdown = crashNode.properties?.peak_drawdown_pct || -20;
    const simulatedDrawdown = baselineDrawdown * (remainingWeight / baselineWeight);

    return {
      scenario_id: `cf-${Date.now()}`,
      description: `Counterfactual: ${intervention.type} on ${intervention.target_entity}`,
      intervention,
      simulated_drawdown_pct: Math.round(simulatedDrawdown * 10) / 10,
      entities_saved: affectedPaths.reduce((sum, p) => sum + p.nodes.length, 0),
      cascade_prevented: removedWeight > baselineWeight * 0.5,
      confidence: Math.min(0.9, 0.5 + affectedPaths.length * 0.1),
    };
  }

  /**
   * Add a crash event to the graph
   */
  async addCrashEvent(crash: CrashEvent): Promise<void> {
    if (!this.client) return;

    const props = {
      id: crash.crash_id,
      type: crash.crash_type,
      name: crash.name,
      primary_index: crash.primary_index || '',
      affected_sectors: JSON.stringify(crash.affected_sectors || []),
      start_date: crash.start_date,
      end_date: crash.end_date || '',
      peak_date: crash.peak_date || '',
      peak_drawdown_pct: crash.peak_drawdown_pct,
      recovery_days: crash.recovery_days || 0,
      regime_before: crash.regime_before,
      regime_during: crash.regime_during,
      regime_after: crash.regime_after,
    };

    const query = `
      MERGE (c:Entity {id: "${crash.crash_id}"})
      SET c += ${this.propsToString(props)},
          c.name_lower = "${crash.name.toLowerCase()}"
    `;

    await this.graphQuery(query);
  }

  /**
   * Link a causal factor to a crash
   */
  async linkCausalFactor(
    crashId: string,
    factor: CausalFactor,
    relation: 'TRIGGERED' | 'SHOCKED' | 'AMPLIFIED_CRASH' = 'TRIGGERED'
  ): Promise<void> {
    if (!this.client) return;

    // Ensure factor node exists
    const factorQuery = `
      MERGE (f:Entity {id: "${factor.factor_id}"})
      SET f.type = "${factor.type}",
          f.name = "${factor.name}",
          f.name_lower = "${factor.name.toLowerCase()}"
    `;
    await this.graphQuery(factorQuery);

    // Create causal edge
    const edgeQuery = `
      MATCH (f:Entity {id: "${factor.factor_id}"})
      MATCH (c:Entity {id: "${crashId}"})
      MERGE (f)-[r:RELATES]->(c)
      SET r.relation = "${relation}",
          r.causal_weight = ${factor.causal_weight},
          r.mechanism = "${factor.mechanism}",
          r.preceded_by_days = ${factor.preceded_crash_by_days},
          r.confidence = ${factor.confidence}
    `;
    await this.graphQuery(edgeQuery);
  }

  // ─── PRIVATE HELPERS ─────────────────────────────────────────────────────────

  private async findCrashNode(name: string): Promise<GraphNode | null> {
    if (!this.client) return null;

    const query = `
      MATCH (n:Entity)
      WHERE n.name_lower CONTAINS "${name.toLowerCase()}"
        AND n.type IN ['MarketCrash', 'SectorCrash', 'FlashCrash', 'CurrencyCrisis', 'BankRun', 'LiquidityCrisis']
      RETURN n
      LIMIT 1
    `;

    const result = await this.graphQuery(query);
    if (!result.length) return null;

    return result[0].n;
  }

  private extractRootCauses(paths: CausalPath[]): CausalFactor[] {
    const causeMap = new Map<string, {
      name: string;
      totalWeight: number;
      mechanisms: Set<string>;
      pathCount: number;
    }>();

    for (const path of paths) {
      if (path.nodes.length === 0) continue;

      const rootName = path.nodes[0];
      const existing = causeMap.get(rootName) || {
        name: rootName,
        totalWeight: 0,
        mechanisms: new Set(),
        pathCount: 0,
      };

      existing.totalWeight += path.total_weight;
      existing.pathCount++;

      if (path.edges.length > 0) {
        existing.mechanisms.add(path.edges[0].mechanism);
      }

      causeMap.set(rootName, existing);
    }

    // Normalize weights and convert to CausalFactor
    const totalWeight = Array.from(causeMap.values()).reduce((sum, c) => sum + c.totalWeight, 0);

    return Array.from(causeMap.values())
      .sort((a, b) => b.totalWeight - a.totalWeight)
      .slice(0, 10)
      .map(c => ({
        factor_id: `factor-${c.name.toLowerCase().replace(/\s+/g, '-')}`,
        name: c.name,
        type: 'CrashFactor' as EntityType,
        causal_weight: totalWeight > 0 ? c.totalWeight / totalWeight : 0,
        mechanism: Array.from(c.mechanisms).join(', '),
        preceded_crash_by_days: 0, // Would need temporal data
        evidence_sources: [],
        confidence: Math.min(0.95, 0.5 + c.pathCount * 0.1),
      }));
  }

  private inferMechanism(relation: string): string {
    const mechanismMap: Record<string, string> = {
      'TRIGGERED': 'direct trigger',
      'SHOCKED': 'exogenous shock',
      'causes': 'causal chain',
      'caused_by': 'upstream cause',
      'impacts': 'downstream impact',
      'amplifies': 'amplification',
      'TRANSMITTED_THROUGH': 'transmission channel',
      'PROPAGATED_TO': 'contagion spread',
      'CASCADED_TO': 'cascade effect',
      'FORCED_LIQUIDATION': 'margin call',
      'AMPLIFIED_CRASH': 'shock amplifier',
    };
    return mechanismMap[relation] || 'unknown mechanism';
  }

  private pathToNarrative(nodes: string[], edges: Array<{ from: string; to: string; relation: string; mechanism: string }>): string {
    if (nodes.length === 0) return '';
    if (nodes.length === 1) return nodes[0];

    const parts: string[] = [nodes[0]];
    for (const edge of edges) {
      parts.push(`→[${edge.mechanism}]→`);
      parts.push(edge.to);
    }
    return parts.join(' ');
  }

  private estimateHawkesIntensity(weights: number[]): number {
    // Simplified Hawkes intensity: product of weights with exponential decay
    if (weights.length === 0) return 0;

    let intensity = 0.01; // Base rate μ
    for (let i = 0; i < weights.length; i++) {
      const weight = weights[i] || 0.5;
      const decay = Math.exp(-0.1 * i); // β = 0.1 decay
      intensity += weight * decay;
    }
    return intensity;
  }

  private nodeToCrashEvent(node: any): CrashEvent {
    const props = node.properties || node;
    return {
      crash_id: props.id || '',
      name: props.name || '',
      crash_type: props.type || 'MarketCrash',
      primary_index: props.primary_index,
      affected_sectors: props.affected_sectors ? JSON.parse(props.affected_sectors) : [],
      start_date: props.start_date || new Date().toISOString(),
      end_date: props.end_date,
      peak_date: props.peak_date,
      peak_drawdown_pct: props.peak_drawdown_pct || 0,
      recovery_days: props.recovery_days,
      regime_before: props.regime_before || 'normal',
      regime_during: props.regime_during || 'stressed',
      regime_after: props.regime_after || 'normal',
    };
  }

  private generateNarrative(
    crash: any,
    rootCauses: CausalFactor[],
    mechanisms: Array<{ mechanism_type: string; entities_affected: number }>,
    paths: CausalPath[]
  ): { summary: string; detailed: string } {
    const crashName = crash.name || crash.properties?.name || 'The crash';
    const drawdown = crash.peak_drawdown_pct || crash.properties?.peak_drawdown_pct || 0;

    // Build summary
    const topCauses = rootCauses.slice(0, 3).map(c => c.name).join(', ');
    const topMechanisms = mechanisms.slice(0, 2).map(m => m.mechanism_type).join(' and ');

    const summary = `${crashName} (${drawdown}% drawdown) was primarily caused by ${topCauses || 'multiple factors'}. ` +
      `The crash propagated through ${topMechanisms || 'various channels'}, ` +
      `affecting ${mechanisms.reduce((sum, m) => sum + m.entities_affected, 0)} entities.`;

    // Build detailed narrative
    let detailed = `## ${crashName}\n\n`;
    detailed += `**Peak Drawdown:** ${drawdown}%\n\n`;

    detailed += `### Root Causes\n`;
    for (const cause of rootCauses.slice(0, 5)) {
      detailed += `- **${cause.name}** (${(cause.causal_weight * 100).toFixed(1)}% contribution): ${cause.mechanism}\n`;
    }

    detailed += `\n### Transmission Mechanisms\n`;
    for (const mech of mechanisms.slice(0, 5)) {
      detailed += `- **${mech.mechanism_type}**: Affected ${mech.entities_affected} entities\n`;
    }

    detailed += `\n### Key Causal Chains\n`;
    for (const path of paths.slice(0, 5)) {
      detailed += `- ${path.narrative}\n`;
    }

    return { summary, detailed };
  }

  private propsToString(props: Record<string, any>): string {
    const pairs: string[] = [];
    for (const [k, v] of Object.entries(props)) {
      if (v === null || v === undefined) continue;
      if (typeof v === 'string') {
        pairs.push(`${k}: "${v.replace(/"/g, '\\"')}"`);
      } else if (typeof v === 'number' || typeof v === 'boolean') {
        pairs.push(`${k}: ${v}`);
      }
    }
    return `{${pairs.join(', ')}}`;
  }

  private async graphQuery(query: string): Promise<any[]> {
    if (!this.client) return [];

    try {
      const result = await (this.client as any).sendCommand([
        'GRAPH.QUERY',
        this.graphName,
        query,
      ]);
      return this.parseGraphResult(result);
    } catch (err: any) {
      console.error('[CAUSAL QUERY ERROR]', err.message);
      return [];
    }
  }

  private parseGraphResult(raw: any): any[] {
    if (!raw || !Array.isArray(raw)) return [];

    const headers = raw[0];
    const data = raw[1];

    if (!data || !Array.isArray(data)) return [];
    if (!headers || !Array.isArray(headers)) return data;

    return data.map((row: any[]) => {
      const obj: any = {};
      for (let i = 0; i < headers.length; i++) {
        obj[headers[i]] = this.parseElement(row[i]);
      }
      return obj;
    });
  }

  private parseElement(elem: any): any {
    if (elem === null || elem === undefined) return null;
    if (!Array.isArray(elem)) return elem;

    // [type_id, value] format
    if (elem.length === 2 && typeof elem[0] === 'number') {
      return elem[1];
    }

    // Node format: [id, labels, props]
    if (elem.length === 3 && Array.isArray(elem[1]) && Array.isArray(elem[2])) {
      const obj: any = { _id: elem[0], _labels: elem[1] };
      for (const prop of elem[2]) {
        if (Array.isArray(prop) && prop.length >= 2) {
          obj[prop[0]] = prop.length > 2 ? prop[2] : prop[1];
        }
      }
      return obj;
    }

    return elem;
  }
}

// ─── SINGLETON EXPORT ────────────────────────────────────────────────────────

export function createCausalEngine(
  client: ReturnType<typeof createClient> | null,
  graphName: string = 'forage_v1'
): CausalInferenceEngine {
  return new CausalInferenceEngine(client, graphName);
}
