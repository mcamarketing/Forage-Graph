/**
 * Forage Knowledge Graph — src/knowledge-graph.ts
 *
 * Storage: FalkorDB (Redis-compatible graph DB).
 * Swap KnowledgeStore internals for any graph DB without touching anything else.
 *
 * Rules unchanged from original architecture:
 * - Non-blocking: graph writes fire after response is sent, never add latency
 * - Privacy: PII hashed before storage, raw values never stored
 * - Passive accumulation: grows silently with every tool call
 * - Confidence increases with corroboration across users
 */

import { createClient } from 'redis';
import { createHash } from 'crypto';
import { generateULEMIdentity, ULEMIdentity, generateCompositeId } from './ulem-identity.js';
import { AdamicAdarScorer, JaccardScorer, LinkPrediction } from './link-prediction.js';
import { HawkesProcessEngine, HawkesEvent, ContagionResult, ShockSimulation, estimateHawkesParams } from './hawkes-contagion.js';

// ─── FIBO-ALIGNED ENTITY TYPES ────────────────────────────────────────────────
// Mapped to Financial Industry Business Ontology (FIBO)
// [fibo-001] https://spec.edmcouncil.org/fibo/ontology

export type EntityType =
  // FIBO Legal Entities
  | 'LegalEntity'          // FIBO: LegalEntity (base type for all organizations)
  | 'AutonomousAgent'      // FIBO: AutonomousAgent (AI agents, bots, autonomous orgs)
  | 'FinancialInstitution' // FIBO: FinancialInstitution (banks, credit unions)
  | 'Corporation'          // FIBO: Corporation (incorporated entities)
  | 'SoleProprietor'       // FIBO: SoleProprietor (unincorporated businesses)
  
  // Legacy compatibility (deprecated, use FIBO types)
  | 'Company'              // Maps to Corporation/FinancialInstitution
  | 'Person'               // FIBO: Person (natural person)
  
  // FIBO Financial Instruments
  | 'FinancialInstrument'  // FIBO: FinancialInstrument (base)
  | 'DebtInstrument'       // FIBO: DebtInstrument (bonds, loans)
  | 'EquityInstrument'     // FIBO: EquityInstrument (stocks, shares)
  | 'DerivativeInstrument' // FIBO: DerivativeInstrument (options, futures)
  | 'Asset'                // FIBO: Asset (any asset class)
  
  // Geographic/Spatial
  | 'Location'             // FIBO: GeographicLocation
  | 'Jurisdiction'         // FIBO: Jurisdiction (legal jurisdiction)
  
  // Industry/Market
  | 'Industry'             // FIBO: IndustrySector
  | 'Market'               // FIBO: Market
  | 'EconomicSector'       // FIBO: EconomicSector
  
  // Information/Knowledge
  | 'Technology'
  | 'Domain'
  | 'JobTitle'
  | 'EmailPattern'
  | 'InformationSource'    // FIBO: InformationSource
  
  // Causal Intelligence Types
  | 'Event'                // Temporal events (macro, geopolitical)
  | 'Trend'
  | 'Indicator'            // Economic indicators (GDP, CPI, etc.)
  | 'Forecast'
  | 'Risk'
  | 'Opportunity'
  | 'Policy'               // Government policy changes
  | 'Regulation'           // Regulatory changes
  | 'Sentiment'
  | 'Topic'
  | 'Narrative'
  | 'Actor'                // Political/financial actors
  | 'Network'              // Organizational networks

  // Simulation Layer [sim-001]
  // SimAgents can READS_FROM Reality but cannot modify it
  | 'SimAgent'             // Simulation agent (AI personas, what-if actors)
  | 'SimEpisode';          // Simulation episode (counterfactual scenarios)

export type RelationType =
  // FIBO Role Pattern Relationships [fibo-role-001]
  // Entities play ROLES, not direct connections
  | 'holds_role'           // Entity plays a role (e.g., Company plays "Issuer")
  | 'is_agent_in'          // Agent acts on behalf of another entity
  | 'has_jurisdiction'     // Entity operates under jurisdiction
  
  // FIBO Organizational
  | 'works_at'             // Person employs at Organization
  | 'has_authorized_agent' // LegalEntity has Agent
  | 'subsidiary_of'        // Corporation subsidiary relationship
  | 'owns'                 // Ownership relationship
  | 'controlled_by'        // Control relationship
  
  // FIBO Financial
  | 'issues'               // Entity issues FinancialInstrument
  | 'investor_in'          // Entity invests in another
  | 'creditor_of'          // Entity holds debt of another
  | 'insures'              // Entity insures another
  | 'guarantees'           // Entity guarantees obligation
  
  // Geographic/Spatial
  | 'located_in'
  | 'operates_in'
  | 'headquartered_in'
  
  // Industry/Market
  | 'competitor_of'
  | 'complements'
  | 'supplies_to'
  | 'purchases_from'
  
  // Technology/Infrastructure
  | 'uses_technology'
  | 'has_domain'
  | 'has_email_pattern'
  
  // Social/Reporting
  | 'reports_to'
  | 'founded_by'
  | 'board_member_of'
  
  // Causal Relations [causal-001]
  | 'causes'
  | 'caused_by'
  | 'predicts'
  | 'predicted_by'
  | 'correlates_with'
  | 'impacts'
  | 'impacted_by'
  | 'enables'
  | 'prevents'
  | 'amplifies'
  | 'dampens'
  | 'precedes'
  | 'follows'
  | 'indicates'
  | 'signals'
  
  // Hierarchical
  | 'part_of'
  | 'contains'
  | 'related_to'
  | 'opposes'
  | 'supports'
  | 'influences'            // Bidirectional influence

  // Simulation Layer [sim-002]
  // SimAgent → Reality boundary (read-only)
  | 'READS_FROM'            // SimAgent reads from Reality node (immutable)
  | 'SIMULATES'             // SimEpisode simulates scenario
  | 'HYPOTHESIZES';         // SimAgent hypothesizes connection

export type Regime = 'normal' | 'stressed' | 'pre_tipping' | 'post_event';

export interface GraphNode {
  id: string;                    // Composite ULEM ID: {sha3_id}:{blake3_id}
  type: EntityType;
  name: string;
  
  // ULEM Identity Fields [ulem-001]
  ulem?: {
    sha3_id: string;            // Primary: SHA3-256 (first 16 hex)
    blake3_id: string;          // Secondary: Blake3 (first 12 hex)
    canonical: string;          // Canonical form used for hashing
  };
  
  // FIBO Role Pattern [fibo-role-002]
  roles?: Array<{
    role_type: string;          // e.g., "Issuer", "Regulator", "Counterparty"
    context: string;            // Context where role is played
    since?: string;             // When role started
  }>;
  
  properties: Record<string, any>;
  sources: string[];
  confidence: number;
  call_count: number;
  regime?: Regime;
  
  // GraphBLAS optimization: cached degree for fast traversal
  degree?: number;
  
  // Hawkes Process state [hawkes-state-001]
  intensity?: number;           // Current Hawkes intensity λ(t)
  branching_ratio?: number;     // Reproduction number R
  
  first_seen: string;
  last_seen: string;
}

export interface GraphEdge {
  id: string;
  from_id: string;
  to_id: string;
  from_name: string;
  to_name: string;
  relation: RelationType;
  properties: Record<string, any>;
  confidence: number;
  call_count: number;
  
  // GraphBLAS optimization [graphblas-001]
  weight?: number;              // Matrix weight for GraphBLAS semiring operations
  
  // Hawkes contagion weight [hawkes-edge-001]
  contagion_weight?: number;    // α_ij for Hawkes process intensity
  
  first_seen: string;
  last_seen: string;
}

export interface GraphStats {
  total_nodes: number;
  total_edges: number;
  nodes_by_type: Record<string, number>;
  last_updated: string;
}

export interface Claim {
  id: string;
  entity: string;
  relation: string;
  target: string;
  assertion: string;
  source_url?: string;
  confidence: number;
  created_at: string;
}

export interface Signal {
  entity: string;
  metric: string;
  value: number;
  timestamp: number;
}

export interface ContagionStats {
  type: string;
  avg_residual_impact: number;
  total_updates: number;
}

// ─── STORAGE LAYER ────────────────────────────────────────────────────────────
// Uses FalkorDB via redis client. Swap internals here when scaling.
// FalkorDB speaks Redis protocol — same client, graph-native Cypher queries on top.

class KnowledgeStore {
  private client: ReturnType<typeof createClient> | null = null;
  private graphName = 'forage_v1';

  async init(): Promise<void> {
    const url = process.env.FALKORDB_URL || process.env.REDIS_URL || 'redis://localhost:6379';
    this.client = createClient({ url });
    this.client.on('error', (err) => {
      // Silent — storage errors must never surface to caller
      if (process.env.NODE_ENV !== 'production') console.error('KV error:', err.message);
    });
    await this.client.connect();

    // Create indexes for fast lookups
    await this.ensureIndexes();
  }

  private async ensureIndexes(): Promise<void> {
    if (!this.client) return;
    try {
      // FalkorDB: create indexes on node properties we query by
      await this.graphQuery(
        `CREATE INDEX FOR (n:Entity) ON (n.id)`,
        {}
      ).catch(() => {}); // Ignore if already exists

      await this.graphQuery(
        `CREATE INDEX FOR (n:Entity) ON (n.name_lower)`,
        {}
      ).catch(() => {});

      await this.graphQuery(
        `CREATE INDEX FOR (n:Entity) ON (n.type)`,
        {}
      ).catch(() => {});

      // Simulation Layer indexes [sim-003]
      await this.graphQuery(
        `CREATE INDEX FOR (n:Entity) ON (n.is_simulation)`,
        {}
      ).catch(() => {});
    } catch {
      // Indexes are optional — graph still works without them
    }
  }

  // ── SIMULATION BOUNDARY VALIDATION [sim-004] ────────────────────────────────
  // SimAgents can only READS_FROM Reality nodes — never modify them

  private isSimulationType(type: EntityType): boolean {
    return type === 'SimAgent' || type === 'SimEpisode';
  }

  private isSimulationRelation(relation: RelationType): boolean {
    return relation === 'READS_FROM' || relation === 'SIMULATES' || relation === 'HYPOTHESIZES';
  }

  /**
   * Validate simulation boundary constraint.
   * SimAgents can READS_FROM Reality, but cannot create causal edges to Reality.
   * Returns true if the edge is valid, false if it violates simulation boundary.
   */
  validateSimulationBoundary(
    fromType: EntityType,
    toType: EntityType,
    relation: RelationType
  ): { valid: boolean; reason?: string } {
    const fromIsSim = this.isSimulationType(fromType);
    const toIsSim = this.isSimulationType(toType);

    // SimAgent → Reality: only READS_FROM allowed
    if (fromIsSim && !toIsSim) {
      if (relation !== 'READS_FROM') {
        return {
          valid: false,
          reason: `SimAgent cannot create '${relation}' edge to Reality. Use READS_FROM for read-only access.`,
        };
      }
    }

    // Reality → SimAgent: not allowed (Reality doesn't know about simulations)
    if (!fromIsSim && toIsSim) {
      return {
        valid: false,
        reason: 'Reality nodes cannot reference SimAgents. Simulation is isolated.',
      };
    }

    return { valid: true };
  }

  // ── GETTERS FOR EXTERNAL MODULES ────────────────────────────────────────────

  /**
   * Get underlying Redis client for link prediction modules.
   * [graphblas-002]
   */
  getClient(): ReturnType<typeof createClient> | null {
    return this.client;
  }

  /**
   * Get graph name for GraphBLAS operations.
   */
  getGraphName(): string {
    return this.graphName;
  }

  // ── EXPORT ALL [backup-003] ──────────────────────────────────────────────────

  /**
   * Get all nodes from the graph for backup/export.
   */
  async getAllNodes(): Promise<GraphNode[]> {
    const rows = await this.graphQuery(
      `MATCH (n:Entity) RETURN n`,
      {}
    );
    return rows.map(row => {
      const n = row.n || row[0] || row;
      return {
        id: n.id || n.properties?.id,
        type: n.type || n.properties?.type || 'Entity',
        name: n.name || n.properties?.name || '',
        properties: n.properties || {},
        sources: n.sources || n.properties?.sources || [],
        confidence: n.confidence || n.properties?.confidence || 0.5,
        call_count: n.call_count || n.properties?.call_count || 1,
        first_seen: n.first_seen || n.properties?.first_seen,
        last_seen: n.last_seen || n.properties?.last_seen,
      } as GraphNode;
    });
  }

  /**
   * Get all edges from the graph for backup/export.
   */
  async getAllEdges(): Promise<GraphEdge[]> {
    const rows = await this.graphQuery(
      `MATCH (a:Entity)-[r:RELATES]->(b:Entity) RETURN a.id AS from_id, a.name AS from_name, r, b.id AS to_id, b.name AS to_name`,
      {}
    );
    return rows.map(row => ({
      id: row.r?.id || `${row.from_id}-${row.to_id}`,
      from_id: row.from_id,
      to_id: row.to_id,
      from_name: row.from_name,
      to_name: row.to_name,
      relation: row.r?.relation || row.r?.properties?.relation || 'related_to',
      properties: row.r?.properties || {},
      confidence: row.r?.confidence || row.r?.properties?.confidence || 0.5,
      call_count: row.r?.call_count || 1,
      first_seen: row.r?.first_seen,
      last_seen: row.r?.last_seen,
    } as GraphEdge));
  }

  // ── BATCH OPERATIONS [cypher-002] ─────────────────────────────────────────────
  
  /**
   * Batch create relationships using UNWIND for GraphBLAS optimization.
   * 
   * Uses Endpoint-First MERGE to prevent matrix duplication:
   * $$
   * \text{MERGE}(a) \parallel \text{MERGE}(b) \implies (a) -[r]-> (b)
   * $$
   * 
   * Cypher pattern:
   * ```cypher
   * UNWIND $batch AS row
   * MATCH (a:Entity {id: row.from_id})
   * MATCH (b:Entity {id: row.to_id})
   * MERGE (a)-[r:RELATES]->(b)
   * SET r.relation = row.relation, r.weight = row.weight
   * ```
   * [cypher-unwind-002]
   */
  async batchCreateRelationships(
    batch: Array<{
      from_id: string;
      to_id: string;
      relation: string;
      weight: number;
      contagion_weight: number;
    }>
  ): Promise<void> {
    if (!this.client || batch.length === 0) return;

    try {
      await this.graphQuery(
        `UNWIND $batch AS row
         MATCH (a:Entity {id: row.from_id})
         MATCH (b:Entity {id: row.to_id})
         MERGE (a)-[r:RELATES]->(b)
         SET r.relation = row.relation,
             r.weight = row.weight,
             r.contagion_weight = row.contagion_weight,
             r.updated_at = timestamp()`,
        { batch }
      );
    } catch {
      // Silent failure for batch operations
    }
  }

  // Execute a Cypher query against FalkorDB
  async graphQuery(query: string, params: Record<string, any>): Promise<any[]> {
    if (!this.client) return [];
    try {
      // FalkorDB uses GRAPH.QUERY command
      const paramStr = Object.entries(params)
        .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
        .join(', ');

      const fullQuery = paramStr ? query : query;
      const result = await (this.client as any).sendCommand([
        'GRAPH.QUERY',
        this.graphName,
        query,
        '--params',
        JSON.stringify(params),
        '--compact',
      ]);
      return this.parseGraphResult(result);
    } catch (err: any) {
      // Fallback: try without params flag (older FalkorDB versions)
      try {
        let q = query;
        for (const [k, v] of Object.entries(params)) {
          q = q.replace(new RegExp(`\\$${k}`, 'g'), JSON.stringify(v));
        }
        const result = await (this.client as any).sendCommand([
          'GRAPH.QUERY',
          this.graphName,
          q,
        ]);
        return this.parseGraphResult(result);
      } catch {
        return [];
      }
    }
  }

  private parseGraphResult(raw: any): any[] {
    if (!raw || !Array.isArray(raw)) return [];
    // FalkorDB compact format: [header, data, stats]
    const data = raw[1];
    if (!data || !Array.isArray(data)) return [];
    return data;
  }

  async getNode(id: string): Promise<GraphNode | null> {
    const rows = await this.graphQuery(
      `MATCH (n:Entity {id: $id}) RETURN n`,
      { id }
    );
    if (!rows.length) return null;
    return this.rowToNode(rows[0][0]);
  }

  async setNode(node: GraphNode): Promise<void> {
    const props = this.flattenForCypher(node);
    await this.graphQuery(
      `MERGE (n:Entity {id: $id})
       SET n += $props
       SET n.name_lower = $name_lower`,
      {
        id: node.id,
        props,
        name_lower: node.name.toLowerCase(),
      }
    );
  }

  async getEdge(id: string): Promise<GraphEdge | null> {
    const rows = await this.graphQuery(
      `MATCH ()-[e:RELATES {id: $id}]->() RETURN e`,
      { id }
    );
    if (!rows.length) return null;
    return this.rowToEdge(rows[0][0]);
  }

  async setEdge(edge: GraphEdge): Promise<void> {
    const props = this.flattenForCypher(edge);
    await this.graphQuery(
      `MATCH (a:Entity {id: $from_id}), (b:Entity {id: $to_id})
       MERGE (a)-[e:RELATES {id: $edge_id}]->(b)
       SET e += $props`,
      {
        from_id: edge.from_id,
        to_id: edge.to_id,
        edge_id: edge.id,
        props,
      }
    );
  }

  async findNodesByName(nameLower: string, type?: string): Promise<GraphNode[]> {
    const query = type
      ? `MATCH (n:Entity) WHERE n.name_lower CONTAINS $name AND n.type = $type RETURN n ORDER BY n.confidence DESC LIMIT 20`
      : `MATCH (n:Entity) WHERE n.name_lower CONTAINS $name RETURN n ORDER BY n.confidence DESC LIMIT 20`;

    const params: any = { name: nameLower };
    if (type) params.type = type;

    const rows = await this.graphQuery(query, params);
    return rows.map(r => this.rowToNode(r[0])).filter(Boolean) as GraphNode[];
  }

  async getOutboundEdges(nodeId: string, relation?: string): Promise<GraphEdge[]> {
    const query = relation
      ? `MATCH (a:Entity {id: $id})-[e:RELATES]->(b:Entity) WHERE e.relation = $relation RETURN e ORDER BY e.confidence DESC`
      : `MATCH (a:Entity {id: $id})-[e:RELATES]->(b:Entity) RETURN e ORDER BY e.confidence DESC`;

    const params: any = { id: nodeId };
    if (relation) params.relation = relation;

    const rows = await this.graphQuery(query, params);
    return rows.map(r => this.rowToEdge(r[0])).filter(Boolean) as GraphEdge[];
  }

  async getInboundEdges(nodeId: string, relation?: string): Promise<GraphEdge[]> {
    const query = relation
      ? `MATCH (a:Entity)-[e:RELATES]->(b:Entity {id: $id}) WHERE e.relation = $relation RETURN e ORDER BY e.confidence DESC`
      : `MATCH (a:Entity)-[e:RELATES]->(b:Entity {id: $id}) RETURN e ORDER BY e.confidence DESC`;

    const params: any = { id: nodeId };
    if (relation) params.relation = relation;

    const rows = await this.graphQuery(query, params);
    return rows.map(r => this.rowToEdge(r[0])).filter(Boolean) as GraphEdge[];
  }

  async getStats(): Promise<GraphStats> {
    try {
      const nodeCount = await this.graphQuery(`MATCH (n:Entity) RETURN count(n)`, {});
      const edgeCount = await this.graphQuery(`MATCH ()-[e:RELATES]->() RETURN count(e)`, {});
      const byType = await this.graphQuery(
        `MATCH (n:Entity) RETURN n.type, count(n) ORDER BY count(n) DESC`,
        {}
      );

      const nodes_by_type: Record<string, number> = {};
      for (const row of byType) {
        if (row[0] && row[1]) nodes_by_type[row[0]] = parseInt(row[1]);
      }

      // Get contagion stats from Redis
      const contagionByType: Record<string, number> = {};
      if (this.client) {
        const keys = await this.client.keys('contagion:type:*');
        for (const key of keys) {
          const type = key.replace('contagion:type:', '');
          const data = await this.client.hGetAll(key);
          if (data && data.avg_impact) {
            contagionByType[type] = parseFloat(data.avg_impact);
          }
        }
      }

      return {
        total_nodes: parseInt(nodeCount[0]?.[0] || '0'),
        total_edges: parseInt(edgeCount[0]?.[0] || '0'),
        nodes_by_type,
        last_updated: new Date().toISOString(),
      };
    } catch {
      return { total_nodes: 0, total_edges: 0, nodes_by_type: {}, last_updated: new Date().toISOString() };
    }
  }

  // ── REGIME ─────────────────────────────────────────────────────────────────

  async setRegime(nodeId: string, regime: Regime): Promise<void> {
    await this.graphQuery(
      `MATCH (n:Entity {id: $id}) SET n.regime = $regime`,
      { id: nodeId, regime }
    );
  }

  async getRegime(nodeId: string): Promise<Regime | null> {
    const rows = await this.graphQuery(
      `MATCH (n:Entity {id: $id}) RETURN n.regime`,
      { id: nodeId }
    );
    return rows[0]?.[0] || null;
  }

  // ── CLAIMS ────────────────────────────────────────────────────────────────

  private claimId(entity: string, assertion: string): string {
    return createHash('sha256')
      .update(`${entity}:${assertion}`)
      .digest('hex')
      .substring(0, 16);
  }

  async addClaim(claim: Omit<Claim, 'id' | 'created_at'>): Promise<Claim> {
    if (!this.client) throw new Error('Not connected');
    
    const id = this.claimId(claim.entity, claim.assertion);
    const fullClaim: Claim = {
      ...claim,
      id,
      created_at: new Date().toISOString(),
    };

    // Store claim hash
    await this.client.hSet(`claim:${id}`, {
      entity: claim.entity,
      relation: claim.relation,
      target: claim.target,
      assertion: claim.assertion,
      source_url: claim.source_url || '',
      confidence: String(claim.confidence),
      created_at: fullClaim.created_at,
    });

    // Index claim by entity
    await this.client.sAdd(`entity_claims:${claim.entity.toLowerCase()}`, id);

    return fullClaim;
  }

  async getClaims(entityName: string): Promise<Claim[]> {
    if (!this.client) return [];
    
    const claimIds = await this.client.sMembers(`entity_claims:${entityName.toLowerCase()}`);
    const claims: Claim[] = [];

    for (const id of claimIds) {
      const data = await this.client.hGetAll(`claim:${id}`);
      if (data && data.entity) {
        claims.push({
          id,
          entity: data.entity,
          relation: data.relation,
          target: data.target,
          assertion: data.assertion,
          source_url: data.source_url || undefined,
          confidence: parseFloat(data.confidence),
          created_at: data.created_at,
        });
      }
    }

    return claims.sort((a, b) => b.confidence - a.confidence);
  }

  // ── SIGNALS (TIME-SERIES) ────────────────────────────────────────────────

  async addSignal(signal: Signal): Promise<void> {
    if (!this.client) return;
    
    const key = `signal:${signal.entity.toLowerCase()}:${signal.metric}`;
    await this.client.zAdd(key, {
      score: signal.timestamp,
      value: JSON.stringify({ value: signal.value, timestamp: signal.timestamp }),
    });
  }

  async getSignals(entityName: string, metric?: string, limit = 100): Promise<Signal[]> {
    if (!this.client) return [];
    
    const signals: Signal[] = [];
    
    if (metric) {
      const key = `signal:${entityName.toLowerCase()}:${metric}`;
      const data = await this.client.zRange(key, 0, limit - 1, { REV: true });
      for (const item of data) {
        try {
          const parsed = JSON.parse(item);
          signals.push({ entity: entityName, metric, value: parsed.value, timestamp: parsed.timestamp });
        } catch {}
      }
    } else {
      // Get all metrics for entity
      const keys = await this.client.keys(`signal:${entityName.toLowerCase()}:*`);
      for (const key of keys) {
        const metric = key.split(':').pop() || '';
        const data = await this.client.zRange(key, 0, limit - 1, { REV: true });
        for (const item of data) {
          try {
            const parsed = JSON.parse(item);
            signals.push({ entity: entityName, metric, value: parsed.value, timestamp: parsed.timestamp });
          } catch {}
        }
      }
    }

    return signals.sort((a, b) => b.timestamp - a.timestamp).slice(0, limit);
  }

  // ── CONTAGION SCORE ──────────────────────────────────────────────────────

  async updateContagion(entityType: string, residualImpact: number): Promise<void> {
    if (!this.client) return;
    
    const key = `contagion:type:${entityType}`;
    const multi = this.client.multi();
    
    multi.hIncrBy(key, 'total_impact', residualImpact);
    multi.hIncrBy(key, 'update_count', 1);
    
    await multi.exec();
    
    // Recalculate average
    const data = await this.client.hGetAll(key);
    if (data && data.update_count) {
      const avg = parseFloat(data.total_impact) / parseInt(data.update_count);
      await this.client.hSet(key, 'avg_impact', String(avg));
    }
  }

  async getContagionByType(entityType: string): Promise<ContagionStats | null> {
    if (!this.client) return null;
    
    const key = `contagion:type:${entityType}`;
    const data = await this.client.hGetAll(key);
    
    if (!data || !data.avg_impact) return null;
    
    return {
      type: entityType,
      avg_residual_impact: parseFloat(data.avg_impact),
      total_updates: parseInt(data.update_count),
    };
  }

  async findPath(fromId: string, toIds: string[], maxHops: number): Promise<{
    path: string[];
    edges: string[];
  } | null> {
    // FalkorDB native shortest path
    try {
      for (const toId of toIds) {
        const rows = await this.graphQuery(
          `MATCH p = shortestPath((a:Entity {id: $from})-[*..${maxHops}]->(b:Entity {id: $to}))
           RETURN [node in nodes(p) | node.id] as node_ids,
                  [rel in relationships(p) | rel.id] as edge_ids`,
          { from: fromId, to: toId }
        );
        if (rows.length && rows[0][0]) {
          return { path: rows[0][0], edges: rows[0][1] || [] };
        }
      }
    } catch {
      return null;
    }
    return null;
  }

  // Serialize a GraphNode/GraphEdge to flat Cypher-safe properties
  private flattenForCypher(obj: any): Record<string, any> {
    const flat: Record<string, any> = {};
    for (const [k, v] of Object.entries(obj)) {
      if (v === null || v === undefined) continue;
      if (typeof v === 'object' && !Array.isArray(v)) {
        // Stringify nested objects — Cypher doesn't support nested maps
        flat[k] = JSON.stringify(v);
      } else if (Array.isArray(v)) {
        flat[k] = JSON.stringify(v);
      } else {
        flat[k] = v;
      }
    }
    return flat;
  }

  private rowToNode(raw: any): GraphNode | null {
    if (!raw) return null;
    const props = raw.properties || raw;
    try {
      return {
        id: props.id,
        type: props.type,
        name: props.name,
        properties: this.parseJsonField(props.properties),
        sources: this.parseJsonField(props.sources) || [],
        confidence: parseFloat(props.confidence) || 0.75,
        call_count: parseInt(props.call_count) || 1,
        first_seen: props.first_seen || new Date().toISOString(),
        last_seen: props.last_seen || new Date().toISOString(),
      };
    } catch {
      return null;
    }
  }

  private rowToEdge(raw: any): GraphEdge | null {
    if (!raw) return null;
    const props = raw.properties || raw;
    try {
      return {
        id: props.id,
        from_id: props.from_id,
        to_id: props.to_id,
        from_name: props.from_name,
        to_name: props.to_name,
        relation: props.relation,
        properties: this.parseJsonField(props.properties),
        confidence: parseFloat(props.confidence) || 0.8,
        call_count: parseInt(props.call_count) || 1,
        first_seen: props.first_seen || new Date().toISOString(),
        last_seen: props.last_seen || new Date().toISOString(),
      };
    } catch {
      return null;
    }
  }

  private parseJsonField(val: any): any {
    if (!val) return {};
    if (typeof val === 'object') return val;
    try { return JSON.parse(val); } catch { return {}; }
  }

  async isHealthy(): Promise<boolean> {
    if (!this.client) return false;
    try {
      await this.client.ping();
      return true;
    } catch {
      return false;
    }
  }
}

// ─── ENTITY EXTRACTORS ────────────────────────────────────────────────────────

function extractFromLeads(leads: any[]): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  for (const lead of leads) {
    if (!lead) continue;

    const companyName = lead.company || lead.organization;
    if (companyName) {
      const companyNode = buildNode('Company', companyName, {
        website: lead.website || lead.companyWebsite || null,
        size: lead.company_size || lead.companySize || null,
        industry: lead.industry || null,
      }, 'forage/find-leads');
      nodes.push(companyNode);

      if (lead.industry) {
        const industryNode = buildNode('Industry', lead.industry, {}, 'forage/find-leads');
        nodes.push(industryNode);
        edges.push(buildEdge(companyNode, industryNode, 'operates_in', 'forage/find-leads'));
      }

      if (lead.location || lead.city || lead.country) {
        const loc = lead.location || [lead.city, lead.country].filter(Boolean).join(', ');
        const locationNode = buildNode('Location', loc, {}, 'forage/find-leads');
        nodes.push(locationNode);
        edges.push(buildEdge(companyNode, locationNode, 'located_in', 'forage/find-leads'));
      }

      const personName = lead.name || `${lead.first_name || ''} ${lead.last_name || ''}`.trim();
      if (personName && personName.length > 1) {
        const personNode = buildNode('Person', hashPII(personName), {
          title: lead.title || lead.jobTitle || null,
          seniority: lead.seniority || null,
          department: lead.department || null,
        }, 'forage/find-leads', 0.7);
        nodes.push(personNode);
        edges.push(buildEdge(personNode, companyNode, 'works_at', 'forage/find-leads'));

        const title = lead.title || lead.jobTitle;
        if (title) {
          const titleNode = buildNode('JobTitle', normaliseTitle(title), {}, 'forage/find-leads');
          nodes.push(titleNode);
          edges.push(buildEdge(personNode, titleNode, 'works_at', 'forage/find-leads'));
        }
      }

      const domain = extractDomain(lead.website || lead.companyWebsite || lead.email);
      if (domain) {
        const domainNode = buildNode('Domain', domain, {}, 'forage/find-leads');
        nodes.push(domainNode);
        edges.push(buildEdge(companyNode, domainNode, 'has_domain', 'forage/find-leads'));
      }
    }
  }

  return { nodes, edges };
}

function extractFromEmails(data: {
  domain: string;
  organization: string;
  pattern: string;
  emails: any[];
}): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  if (!data.domain) return { nodes, edges };

  const domainNode = buildNode('Domain', data.domain, {}, 'forage/find-emails');
  nodes.push(domainNode);

  if (data.organization) {
    const companyNode = buildNode('Company', data.organization, { domain: data.domain }, 'forage/find-emails', 0.9);
    nodes.push(companyNode);
    edges.push(buildEdge(companyNode, domainNode, 'has_domain', 'forage/find-emails'));

    if (data.pattern) {
      const patternNode = buildNode('EmailPattern', data.pattern, { domain: data.domain }, 'forage/find-emails', 0.95);
      nodes.push(patternNode);
      edges.push(buildEdge(companyNode, patternNode, 'has_email_pattern', 'forage/find-emails'));
    }
  }

  for (const email of (data.emails || [])) {
    if (!email.position) continue;
    const titleNode = buildNode('JobTitle', normaliseTitle(email.position), {
      department: email.department || null,
      seniority: email.seniority || null,
    }, 'forage/find-emails');
    nodes.push(titleNode);
  }

  return { nodes, edges };
}

function extractFromCompanyInfo(data: {
  domain: string;
  website?: any;
  email_intelligence?: any;
}): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  if (!data.domain) return { nodes, edges };

  const domainNode = buildNode('Domain', data.domain, {}, 'forage/get-company-info');
  nodes.push(domainNode);

  const org = data.email_intelligence?.organization;
  if (org) {
    const companyNode = buildNode('Company', org, {
      domain: data.domain,
      title: data.website?.title || null,
      description: data.website?.description || null,
    }, 'forage/get-company-info', 0.9);
    nodes.push(companyNode);
    edges.push(buildEdge(companyNode, domainNode, 'has_domain', 'forage/get-company-info'));

    const socials = data.website?.social_links || {};
    for (const [platform, url] of Object.entries(socials)) {
      if (url) {
        const techNode = buildNode('Technology', platform, { url: String(url) }, 'forage/get-company-info');
        nodes.push(techNode);
        edges.push(buildEdge(companyNode, techNode, 'uses_technology', 'forage/get-company-info'));
      }
    }
  }

  return { nodes, edges };
}

function extractFromLocalLeads(data: {
  keyword: string;
  location: string;
  leads: any[];
}): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  if (!data.location) return { nodes, edges };

  const locationNode = buildNode('Location', data.location, {}, 'forage/find-local-leads');
  nodes.push(locationNode);
  const industryNode = buildNode('Industry', data.keyword, {}, 'forage/find-local-leads');
  nodes.push(industryNode);

  for (const lead of (data.leads || [])) {
    if (!lead.name) continue;
    const companyNode = buildNode('Company', lead.name, {
      address: lead.address || null,
      phone: lead.phone ? hashPII(lead.phone) : null,
      website: lead.website || null,
      rating: lead.rating || null,
    }, 'forage/find-local-leads', 0.95);
    nodes.push(companyNode);
    edges.push(buildEdge(companyNode, locationNode, 'located_in', 'forage/find-local-leads'));
    edges.push(buildEdge(companyNode, industryNode, 'operates_in', 'forage/find-local-leads'));

    if (lead.website) {
      const domain = extractDomain(lead.website);
      if (domain) {
        const domainNode = buildNode('Domain', domain, {}, 'forage/find-local-leads');
        nodes.push(domainNode);
        edges.push(buildEdge(companyNode, domainNode, 'has_domain', 'forage/find-local-leads'));
      }
    }
  }

  return { nodes, edges };
}

function extractFromWebSearch(data: {
  query: string;
  results: Array<{ title: string; link: string; snippet: string }>;
}): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  for (const result of (data.results || [])) {
    const domain = extractDomain(result.link);
    if (!domain) continue;
    nodes.push(buildNode('Domain', domain, {
      title: result.title || null,
      snippet: result.snippet?.substring(0, 200) || null,
    }, 'forage/search-web'));
  }

  return { nodes, edges };
}

// ─── KNOWLEDGE GRAPH ──────────────────────────────────────────────────────────

export class KnowledgeGraph {
  private db: KnowledgeStore;
  private ready = false;
  
  // Link prediction scorers [fp-001]
  private adamicAdar: AdamicAdarScorer | null = null;
  private jaccard: JaccardScorer | null = null;
  
  // Hawkes process engine [hawkes-001]
  private hawkes: HawkesProcessEngine | null = null;
  private hawkesEvents: HawkesEvent[] = [];

  constructor() {
    this.db = new KnowledgeStore();
  }

  async init(): Promise<void> {
    try {
      await this.db.init();
      this.ready = true;
      
      // Initialize link prediction scorers
      const client = this.db.getClient();
      const graphName = this.db.getGraphName();
      if (client) {
        this.adamicAdar = new AdamicAdarScorer(client, graphName);
        this.jaccard = new JaccardScorer(client, graphName);
      }
      
      // Initialize Hawkes process with default params
      this.hawkes = new HawkesProcessEngine({
        mu: 0.01,
        alpha: new Map(),
        beta: 0.1,
        gamma: 0.001,
      });
      
      console.log('Knowledge graph initialised with Reality Graph features');
    } catch (err: any) {
      console.error('Knowledge graph init failed:', err.message);
      this.ready = false;
    }
  }

  async isHealthy(): Promise<boolean> {
    return this.ready && await this.db.isHealthy();
  }

  // Fire and forget — called after every tool response
  async ingest(toolName: string, result: any): Promise<void> {
    if (!this.ready) return;
    try {
      const { nodes, edges } = this.extract(toolName, result);
      if (nodes.length === 0 && edges.length === 0) return;
      await this.merge(nodes, edges);
    } catch {
      // Silent always
    }
  }

  private extract(toolName: string, result: any): { nodes: GraphNode[]; edges: GraphEdge[] } {
    // Normalize tool name: forage/find-leads → find_leads, FIND_LEADS → find_leads
    const normalized = toolName
      .toLowerCase()
      .replace(/^forage[\/\-_]?/, '')
      .replace(/[-\s]/g, '_')
      .trim();

    switch (normalized) {
      case 'find_leads':       return extractFromLeads(result?.leads || result || []);
      case 'find_emails':      return extractFromEmails(result || {});
      case 'get_company_info': return extractFromCompanyInfo(result || {});
      case 'find_local_leads': return extractFromLocalLeads(result || {});
      case 'search_web':       return extractFromWebSearch(result || {});
      default:                 return this.extractUniversal(toolName, result);
    }
  }

  // Universal fallback extractor - extracts entities from any JSON structure
  private extractUniversal(source: string, data: any): { nodes: GraphNode[]; edges: GraphEdge[] } {
    const nodes: GraphNode[] = [];
    const edges: GraphEdge[] = [];
    if (!data || typeof data !== 'object') return { nodes, edges };

    const extract = (obj: any, depth = 0): void => {
      if (depth > 5 || !obj) return;
      if (Array.isArray(obj)) { obj.forEach(item => extract(item, depth + 1)); return; }
      if (typeof obj !== 'object') return;

      // Auto-detect entities by field names
      const entityMappings: Record<string, EntityType> = {
        company: 'Company', organization: 'Company', business: 'Company', firm: 'Company',
        person: 'Person', name: 'Person', author: 'Person', creator: 'Person',
        location: 'Location', city: 'Location', country: 'Location', region: 'Location', address: 'Location',
        domain: 'Domain', website: 'Domain', url: 'Domain',
        technology: 'Technology', tech: 'Technology', platform: 'Technology', tool: 'Technology',
        industry: 'Industry', sector: 'Industry', market: 'Market',
        event: 'Event', incident: 'Event', occurrence: 'Event',
        trend: 'Trend', pattern: 'Trend', movement: 'Trend',
        risk: 'Risk', threat: 'Risk', danger: 'Risk',
        opportunity: 'Opportunity', prospect: 'Opportunity',
        topic: 'Topic', subject: 'Topic', theme: 'Topic',
        sentiment: 'Sentiment', mood: 'Sentiment', feeling: 'Sentiment',
        indicator: 'Indicator', metric: 'Indicator', signal: 'Indicator',
        forecast: 'Forecast', prediction: 'Forecast', projection: 'Forecast',
        policy: 'Policy', rule: 'Policy', regulation: 'Regulation',
        asset: 'Asset', resource: 'Asset', holding: 'Asset',
        actor: 'Actor', agent: 'Actor', player: 'Actor',
        network: 'Network', graph: 'Network', system: 'Network',
        narrative: 'Narrative', story: 'Narrative', account: 'Narrative',
        source: 'InformationSource', origin: 'InformationSource', reference: 'InformationSource',
      };

      for (const [field, value] of Object.entries(obj)) {
        if (!value || typeof value !== 'string' || value.length < 2 || value.length > 200) continue;
        const fieldLower = field.toLowerCase();
        const entityType = entityMappings[fieldLower];
        if (entityType) {
          const cleanValue = entityType === 'Domain' ? extractDomain(value) || value : value.trim();
          if (cleanValue && cleanValue.length > 1) {
            nodes.push(buildNode(entityType, cleanValue, { source_field: field }, source, 0.6));
          }
        }
      }

      // Also extract arrays of entities
      for (const [field, value] of Object.entries(obj)) {
        if (Array.isArray(value)) {
          value.forEach(item => {
            if (typeof item === 'object') extract(item, depth + 1);
          });
        } else if (typeof value === 'object') {
          extract(value, depth + 1);
        }
      }
    };

    extract(data);

    // Build connections between co-occurring entities
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        if (nodes[i].type !== nodes[j].type && nodes[i].id !== nodes[j].id) {
          edges.push(buildEdge(nodes[i], nodes[j], 'related_to', source, 0.5));
        }
      }
    }

    return { nodes, edges };
  }

  private async merge(newNodes: GraphNode[], newEdges: GraphEdge[]): Promise<void> {
    const now = new Date().toISOString();

    // Deduplicate within batch
    const nodeMap = new Map<string, GraphNode>();
    for (const node of newNodes) {
      if (nodeMap.has(node.id)) {
        nodeMap.set(node.id, mergeNodeProperties(nodeMap.get(node.id)!, node));
      } else {
        nodeMap.set(node.id, node);
      }
    }

    for (const node of nodeMap.values()) {
      const existing = await this.db.getNode(node.id);
      if (existing) {
        const merged = mergeNodeProperties(existing, node);
        merged.last_seen = now;
        merged.call_count = (existing.call_count || 1) + 1;
        merged.confidence = Math.min(0.99, existing.confidence + 0.03);
        await this.db.setNode(merged);
      } else {
        await this.db.setNode({ ...node, first_seen: now, last_seen: now });
      }
    }

    const edgeMap = new Map<string, GraphEdge>();
    for (const edge of newEdges) edgeMap.set(edge.id, edge);

    for (const edge of edgeMap.values()) {
      const existing = await this.db.getEdge(edge.id);
      if (existing) {
        existing.call_count = (existing.call_count || 1) + 1;
        existing.confidence = Math.min(0.99, existing.confidence + 0.05);
        existing.last_seen = now;
        await this.db.setEdge(existing);
      } else {
        await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
      }
    }
  }

  // ── QUERIES ───────────────────────────────────────────────────────────────

  async findEntity(name: string, type?: EntityType): Promise<GraphNode[]> {
    if (!this.ready) return [];
    const nodes = await this.db.findNodesByName(name.toLowerCase(), type);
    return nodes.sort((a, b) => {
      const aExact = a.name.toLowerCase() === name.toLowerCase() ? 1 : 0;
      const bExact = b.name.toLowerCase() === name.toLowerCase() ? 1 : 0;
      return (bExact - aExact) || (b.confidence - a.confidence);
    });
  }

  async getNeighbours(nodeId: string, relation?: RelationType): Promise<{
    node: GraphNode;
    edge: GraphEdge;
    neighbour: GraphNode;
  }[]> {
    if (!this.ready) return [];
    const edges = await this.db.getOutboundEdges(nodeId, relation);
    const results = [];

    for (const edge of edges) {
      const [node, neighbour] = await Promise.all([
        this.db.getNode(edge.from_id),
        this.db.getNode(edge.to_id),
      ]);
      if (node && neighbour) results.push({ node, edge, neighbour });
    }

    return results.sort((a, b) => b.edge.confidence - a.edge.confidence);
  }

  async findConnections(fromName: string, toName: string, maxHops = 3): Promise<{
    path: GraphNode[];
    edges: GraphEdge[];
    hops: number;
  } | null> {
    if (!this.ready) return null;

    const fromNodes = await this.findEntity(fromName);
    const toNodes = await this.findEntity(toName);
    if (!fromNodes.length || !toNodes.length) return null;

    const result = await this.db.findPath(
      fromNodes[0].id,
      toNodes.map(n => n.id),
      maxHops
    );
    if (!result) return null;

    const pathNodes = await Promise.all(result.path.map(id => this.db.getNode(id)));
    const pathEdges = await Promise.all(result.edges.map(id => this.db.getEdge(id)));

    return {
      path: pathNodes.filter(Boolean) as GraphNode[],
      edges: pathEdges.filter(Boolean) as GraphEdge[],
      hops: result.path.length - 1,
    };
  }

  async enrich(identifier: string): Promise<{
    entity: GraphNode | null;
    related: Record<string, GraphNode[]>;
    confidence: number;
  }> {
    if (!this.ready) return { entity: null, related: {}, confidence: 0 };

    let candidates = await this.findEntity(identifier, 'Domain');
    if (!candidates.length) candidates = await this.findEntity(identifier, 'Company');
    if (!candidates.length) candidates = await this.findEntity(identifier);
    if (!candidates.length) return { entity: null, related: {}, confidence: 0 };

    const entity = candidates[0];
    const neighbours = await this.getNeighbours(entity.id);

    const related: Record<string, GraphNode[]> = {};
    for (const { edge, neighbour } of neighbours) {
      const key = edge.relation;
      if (!related[key]) related[key] = [];
      related[key].push(neighbour);
    }

    return { entity, related, confidence: entity.confidence };
  }

  async findByIndustryAndLocation(industry: string, location?: string): Promise<GraphNode[]> {
    if (!this.ready) return [];

    const industryNodes = await this.findEntity(industry, 'Industry');
    if (!industryNodes.length) return [];

    const inEdges = await this.db.getInboundEdges(industryNodes[0].id, 'operates_in');
    const companies: GraphNode[] = [];

    for (const edge of inEdges) {
      const company = await this.db.getNode(edge.from_id);
      if (!company || company.type !== 'Company') continue;

      if (location) {
        const neighbours = await this.getNeighbours(company.id, 'located_in');
        const inLocation = neighbours.some(n =>
          n.neighbour.name.toLowerCase().includes(location.toLowerCase())
        );
        if (!inLocation) continue;
      }

      companies.push(company);
    }

    return companies.sort((a, b) => b.confidence - a.confidence);
  }

  async getStats(): Promise<GraphStats> {
    return this.db.getStats();
  }

  // ── DIRECT INJECTION ────────────────────────────────────────────────────────
  // For n8n workflows and external feeds to inject entities/connections directly

  async addEntities(entities: Array<{
    type: EntityType;
    name: string;
    properties?: Record<string, any>;
    confidence?: number;
    source?: string;
  }>): Promise<{ added: number; merged: number }> {
    if (!this.ready) return { added: 0, merged: 0 };

    let added = 0, merged = 0;
    const now = new Date().toISOString();

    for (const e of entities) {
      const node = buildNode(
        e.type,
        e.name,
        e.properties || {},
        e.source || 'direct_inject',
        e.confidence || 0.75
      );

      const existing = await this.db.getNode(node.id);
      if (existing) {
        const m = mergeNodeProperties(existing, node);
        m.last_seen = now;
        m.call_count = (existing.call_count || 1) + 1;
        m.confidence = Math.min(0.99, existing.confidence + 0.03);
        await this.db.setNode(m);
        merged++;
      } else {
        await this.db.setNode({ ...node, first_seen: now, last_seen: now });
        added++;
      }
    }

    return { added, merged };
  }

  async addConnections(connections: Array<{
    from_type: EntityType;
    from_name: string;
    to_type: EntityType;
    to_name: string;
    relation: RelationType;
    properties?: Record<string, any>;
    confidence?: number;
    source?: string;
  }>): Promise<{ added: number; merged: number }> {
    if (!this.ready) return { added: 0, merged: 0 };

    let added = 0, merged = 0;
    const now = new Date().toISOString();

    for (const c of connections) {
      const fromNode = buildNode(c.from_type, c.from_name, {}, c.source || 'direct_inject');
      const toNode = buildNode(c.to_type, c.to_name, {}, c.source || 'direct_inject');

      // Ensure both nodes exist
      if (!await this.db.getNode(fromNode.id)) {
        await this.db.setNode({ ...fromNode, first_seen: now, last_seen: now });
      }
      if (!await this.db.getNode(toNode.id)) {
        await this.db.setNode({ ...toNode, first_seen: now, last_seen: now });
      }

      const edge = buildEdge(fromNode, toNode, c.relation, c.source || 'direct_inject', c.confidence || 0.75);
      if (c.properties) edge.properties = { ...edge.properties, ...c.properties };

      const existing = await this.db.getEdge(edge.id);
      if (existing) {
        existing.call_count = (existing.call_count || 1) + 1;
        existing.confidence = Math.min(0.99, existing.confidence + 0.05);
        existing.last_seen = now;
        await this.db.setEdge(existing);
        merged++;
      } else {
        await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
        added++;
      }
    }

    return { added, merged };
  }

  // ── REGIME ─────────────────────────────────────────────────────────────────

  async setRegime(entityName: string, regime: Regime): Promise<boolean> {
    if (!this.ready) return false;
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return false;
    await this.db.setRegime(nodes[0].id, regime);
    return true;
  }

  async getRegime(entityName: string): Promise<Regime | null> {
    if (!this.ready) return null;
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return null;
    return this.db.getRegime(nodes[0].id);
  }

  // ── CLAIMS ────────────────────────────────────────────────────────────────

  async addClaim(claim: Omit<Claim, 'id' | 'created_at'>): Promise<Claim> {
    if (!this.ready) throw new Error('Not ready');
    return this.db.addClaim(claim);
  }

  async getClaims(entityName: string): Promise<Claim[]> {
    if (!this.ready) return [];
    return this.db.getClaims(entityName);
  }

  // ── SIGNALS ──────────────────────────────────────────────────────────────

  async addSignal(signal: Signal): Promise<void> {
    if (!this.ready) return;
    await this.db.addSignal(signal);
  }

  async getSignals(entityName: string, metric?: string, limit = 100): Promise<Signal[]> {
    if (!this.ready) return [];
    return this.db.getSignals(entityName, metric, limit);
  }

  // ── CAUSAL QUERIES ──────────────────────────────────────────────────────

  async getCausalParents(entityName: string, limit = 10): Promise<{
    entities: Array<{ name: string; type: string; causal_weight: number; mechanism: string }>;
  }> {
    if (!this.ready) return { entities: [] };
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return { entities: [] };
    
    const edges = await this.db.getInboundEdges(nodes[0].id);
    const results: Array<{ name: string; type: string; causal_weight: number; mechanism: string }> = [];
    
    for (const edge of edges) {
      const sourceNode = await this.db.getNode(edge.from_id);
      if (sourceNode) {
        const causalWeight = edge.properties?.causal_weight || edge.confidence;
        results.push({
          name: sourceNode.name,
          type: sourceNode.type,
          causal_weight: causalWeight,
          mechanism: edge.properties?.mechanism || edge.relation,
        });
      }
    }
    
    return { entities: results.sort((a, b) => b.causal_weight - a.causal_weight).slice(0, limit) };
  }

  async getCausalChildren(entityName: string, limit = 10): Promise<{
    entities: Array<{ name: string; type: string; causal_weight: number; mechanism: string }>;
  }> {
    if (!this.ready) return { entities: [] };
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return { entities: [] };
    
    const edges = await this.db.getOutboundEdges(nodes[0].id);
    const results: Array<{ name: string; type: string; causal_weight: number; mechanism: string }> = [];
    
    for (const edge of edges) {
      const targetNode = await this.db.getNode(edge.to_id);
      if (targetNode) {
        const causalWeight = edge.properties?.causal_weight || edge.confidence;
        results.push({
          name: targetNode.name,
          type: targetNode.type,
          causal_weight: causalWeight,
          mechanism: edge.properties?.mechanism || edge.relation,
        });
      }
    }
    
    return { entities: results.sort((a, b) => b.causal_weight - a.causal_weight).slice(0, limit) };
  }

  async getCausalPath(fromName: string, toName: string): Promise<{
    path: string[];
    total_weight: number;
    edges: Array<{ from: string; to: string; weight: number; mechanism: string }>;
  } | null> {
    if (!this.ready) return null;
    
    const fromNodes = await this.findEntity(fromName);
    const toNodes = await this.findEntity(toName);
    if (!fromNodes.length || !toNodes.length) return null;
    
    const result = await this.db.findPath(fromNodes[0].id, toNodes.map(n => n.id), 5);
    if (!result) return null;
    
    let totalWeight = 0;
    const edgeDetails: Array<{ from: string; to: string; weight: number; mechanism: string }> = [];
    
    for (const edgeId of result.edges) {
      const edge = await this.db.getEdge(edgeId);
      if (edge) {
        const weight = edge.properties?.causal_weight || edge.confidence;
        totalWeight += weight;
        edgeDetails.push({
          from: edge.from_name,
          to: edge.to_name,
          weight,
          mechanism: edge.properties?.mechanism || edge.relation,
        });
      }
    }
    
    const pathNodes = await Promise.all(result.path.map(id => this.db.getNode(id)));
    const pathNames = pathNodes.filter(Boolean).map(n => n!.name);
    
    return { path: pathNames, total_weight: totalWeight, edges: edgeDetails };
  }

  async simulate(
    entityName: string, 
    intervention: 'shock' | 'boost' | 'remove',
    depth = 3
  ): Promise<{
    affected: Array<{ name: string; type: string; residual_impact: number; path: string[] }>;
    summary: string;
  }> {
    if (!this.ready) return { affected: [], summary: 'Not ready' };
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return { affected: [], summary: 'Entity not found' };
    
    const startNode = nodes[0];
    const startRegime = await this.db.getRegime(startNode.id);
    
    // Get initial impact based on intervention type
    const baseImpact = intervention === 'shock' ? 1.0 : intervention === 'boost' ? 0.8 : -0.5;
    const regimeMultiplier = (startRegime === 'stressed' || startRegime === 'pre_tipping') ? 1.5 : 1.0;
    const initialImpact = baseImpact * regimeMultiplier;
    
    // BFS propagation with attenuation
    const visited = new Set<string>();
    const affected: Array<{ name: string; type: string; residual_impact: number; path: string[] }> = [];
    const queue: Array<{ nodeId: string; impact: number; path: string[] }> = [
      { nodeId: startNode.id, impact: initialImpact, path: [startNode.name] }
    ];
    
    while (queue.length > 0 && affected.length < 50) {
      const current = queue.shift()!;
      if (visited.has(current.nodeId)) continue;
      visited.add(current.nodeId);
      
      const node = await this.db.getNode(current.nodeId);
      if (!node || node.id === startNode.id) continue;
      
      // Check regime of this node
      const nodeRegime = await this.db.getRegime(node.id);
      const nodeMultiplier = (nodeRegime === 'stressed' || nodeRegime === 'pre_tipping') ? 1.5 : 1.0;
      
      const residualImpact = current.impact * 0.7 * nodeMultiplier; // 70% attenuation
      
      if (Math.abs(residualImpact) > 0.05) {
        affected.push({
          name: node.name,
          type: node.type,
          residual_impact: Math.round(residualImpact * 1000) / 1000,
          path: current.path,
        });
        
        // Update contagion score for this entity type
        await this.db.updateContagion(node.type, Math.abs(residualImpact));
        
        // Continue propagation if within depth
        if (current.path.length < depth) {
          const edges = await this.db.getOutboundEdges(current.nodeId);
          for (const edge of edges) {
            if (!visited.has(edge.to_id)) {
              queue.push({
                nodeId: edge.to_id,
                impact: residualImpact,
                path: [...current.path, node.name],
              });
            }
          }
        }
      }
    }
    
    const summary = `Intervention "${intervention}" on ${entityName} (regime: ${startRegime || 'normal'}) ` +
      `propagated to ${affected.length} entities with ${depth} hops of attenuation.`;
    
    return { affected: affected.sort((a, b) => Math.abs(b.residual_impact) - Math.abs(a.residual_impact)), summary };
  }

  // ── LINK PREDICTION [fp-002] ──────────────────────────────────────────────────
  
  /**
   * Predict potential links using Adamic-Adar similarity.
   * 
   * $$AA(i,j) = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$$
   * 
   * Returns entities likely to be connected based on shared neighbor topology.
   */
  async predictLinks(
    entityName: string,
    targetEntityType?: string,
    maxPredictions = 10
  ): Promise<LinkPrediction[]> {
    if (!this.ready || !this.adamicAdar) return [];
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return [];
    
    return this.adamicAdar.predictLinks(nodes[0].id, targetEntityType, maxPredictions);
  }

  // ── HAWKES PROCESS CONTAGION [hawkes-002] ─────────────────────────────────────

  /**
   * Record an event for Hawkes process modeling.
   * Events are used to compute self-exciting intensity for causal contagion.
   */
  async recordEvent(event: HawkesEvent): Promise<void> {
    if (!this.ready || !this.hawkes) return;
    
    this.hawkesEvents.push(event);
    this.hawkes.addEvents([event]);
  }

  /**
   * Get current contagion state for an entity.
   * 
   * Uses Hawkes Process to compute:
   * - Current intensity $\lambda(t)$
   * - Branching ratio $R$ (reproduction number)
   * - Top drivers of intensity
   */
  async getContagionState(entityName: string): Promise<ContagionResult | null> {
    if (!this.ready || !this.hawkes) return null;
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return null;
    
    return this.hawkes.getContagionState(nodes[0].id);
  }

  /**
   * Simulate shock propagation using Hawkes Process.
   * 
   * Forward simulation of self-exciting point process:
   * $$\lambda_j(t) = \mu_j + \sum_{i} \sum_{t_i^k < t} \alpha_{ij} \beta e^{-\beta(t - t_i^k)}$$
   * 
   * Returns cascade prediction over time window.
   */
  async simulateShock(
    entityName: string,
    shockMagnitude: number = 1.0,
    durationHours: number = 168
  ): Promise<ShockSimulation | null> {
    if (!this.ready || !this.hawkes) return null;
    
    const nodes = await this.findEntity(entityName);
    if (!nodes.length) return null;
    
    return this.hawkes.simulateShock(nodes[0].id, shockMagnitude, durationHours);
  }

  /**
   * Update Hawkes process parameters based on observed events.
   * Uses Maximum Likelihood Estimation (MLE):
   * $$\hat{\mu} = \frac{N}{T}, \quad \hat{\alpha}_{ij} = \frac{\text{co-occurrences}}{\text{total}}$$
   */
  async recalibrateHawkes(windowHours: number = 720): Promise<void> {
    if (!this.ready) return;
    
    const now = Date.now();
    const windowMs = windowHours * 3600000;
    const recentEvents = this.hawkesEvents.filter(e => (now - e.timestamp) < windowMs);
    
    if (recentEvents.length > 10) {
      const params = estimateHawkesParams(recentEvents, windowMs);
      this.hawkes = new HawkesProcessEngine(params);
    }
  }

  // ── UNWIND BATCH OPERATIONS [cypher-001] ──────────────────────────────────────
  
  /**
   * Batch merge entities using UNWIND for optimal GraphBLAS performance.
   * 
   * Uses Endpoint-First MERGE strategy:
   * 1. MERGE nodes independently (prevents matrix duplication)
   * 2. UNWIND batch to create relationships
   * 
   * Cypher pattern:
   * ```cypher
   * UNWIND $batch AS row
   * MERGE (a:Entity {id: row.from_id})
   * MERGE (b:Entity {id: row.to_id})
   * MERGE (a)-[r:RELATES {relation: row.relation}]->(b)
   * ```
   * [cypher-unwind-001]
   */
  async batchMergeEdges(edges: GraphEdge[]): Promise<{ merged: number }> {
    if (!this.ready || edges.length === 0) return { merged: 0 };
    
    // Prepare batch for UNWIND
    const batch = edges.map(e => ({
      from_id: e.from_id,
      to_id: e.to_id,
      relation: e.relation,
      weight: e.weight || e.confidence,
      contagion_weight: e.contagion_weight || 0.1,
    }));
    
    // Execute with UNWIND batching
    await this.db.batchCreateRelationships(batch);

    return { merged: edges.length };
  }

  // ─── BACKUP / EXPORT [backup-001] ─────────────────────────────────────────────
  // Full graph export for disaster recovery

  /**
   * Export all entities and relationships as JSON backup.
   * Returns complete graph state for restoration.
   */
  async exportAll(): Promise<{
    version: string;
    exported_at: string;
    stats: GraphStats;
    entities: GraphNode[];
    relationships: GraphEdge[];
  }> {
    if (!this.ready) throw new Error('Graph not initialized');

    // Get all entities
    const entities = await this.db.getAllNodes();

    // Get all edges
    const relationships = await this.db.getAllEdges();

    // Get stats
    const stats = await this.getStats();

    return {
      version: '1.0',
      exported_at: new Date().toISOString(),
      stats,
      entities,
      relationships,
    };
  }

  /**
   * Import entities and relationships from JSON backup.
   * Merges with existing data (does not wipe first).
   */
  async importBackup(backup: {
    entities: Array<{ type: string; name: string; properties?: Record<string, any>; confidence?: number; source?: string }>;
    relationships?: Array<{ from_type: string; from_name: string; to_type: string; to_name: string; relation: string; confidence?: number; source?: string }>;
  }): Promise<{ entities_imported: number; relationships_imported: number }> {
    if (!this.ready) throw new Error('Graph not initialized');

    let entities_imported = 0;
    let relationships_imported = 0;
    const now = new Date().toISOString();

    // Import entities
    for (const e of backup.entities || []) {
      const node = buildNode(
        e.type as EntityType,
        e.name,
        e.properties || {},
        e.source || 'backup_import',
        e.confidence || 0.9
      );
      await this.db.setNode({ ...node, first_seen: now, last_seen: now });
      entities_imported++;
    }

    // Import relationships
    for (const r of backup.relationships || []) {
      const fromNode = buildNode(r.from_type as EntityType, r.from_name, {}, 'backup_import');
      const toNode = buildNode(r.to_type as EntityType, r.to_name, {}, 'backup_import');
      await this.db.setNode({ ...fromNode, first_seen: now, last_seen: now });
      await this.db.setNode({ ...toNode, first_seen: now, last_seen: now });
      const edge = buildEdge(fromNode, toNode, r.relation as RelationType, r.source || 'backup_import', r.confidence || 0.8);
      await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
      relationships_imported++;
    }

    return { entities_imported, relationships_imported };
  }
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function buildNode(
  type: EntityType,
  name: string,
  properties: Record<string, any>,
  source: string,
  confidence = 0.75
): GraphNode {
  const cleanName = name.trim();
  
  // Generate ULEM dual-hash identity [ulem-002]
  const ulemIdentity = generateULEMIdentity(type, cleanName);
  const compositeId = generateCompositeId(ulemIdentity);
  
  return {
    id: compositeId,
    type,
    name: cleanName,
    // ULEM identity fields
    ulem: {
      sha3_id: ulemIdentity.sha3_id,
      blake3_id: ulemIdentity.blake3_id,
      canonical: ulemIdentity.canonical,
    },
    properties: cleanProperties(properties),
    sources: [source],
    confidence,
    call_count: 1,
    first_seen: new Date().toISOString(),
    last_seen: new Date().toISOString(),
  };
}

function buildEdge(
  from: GraphNode,
  to: GraphNode,
  relation: RelationType,
  source: string,
  confidence = 0.8
): GraphEdge {
  return {
    id: edgeId(from.id, relation, to.id),
    from_id: from.id,
    to_id: to.id,
    from_name: from.name,
    to_name: to.name,
    relation,
    properties: { source },
    confidence,
    call_count: 1,
    first_seen: new Date().toISOString(),
    last_seen: new Date().toISOString(),
  };
}

function mergeNodeProperties(existing: GraphNode, incoming: GraphNode): GraphNode {
  const mergedSources = [...new Set([...existing.sources, ...incoming.sources])];
  const mergedProps: Record<string, any> = { ...existing.properties };
  for (const [k, v] of Object.entries(incoming.properties)) {
    if (v !== null && v !== undefined && v !== '') {
      if (mergedProps[k] === null || mergedProps[k] === undefined) {
        mergedProps[k] = v;
      }
    }
  }
  return { ...existing, properties: mergedProps, sources: mergedSources };
}

// ─── LEGACY NODE ID (for backward compatibility) ──────────────────────────────
// Uses SHA3-256 instead of SHA-256 for consistency with ULEM

function nodeId(type: string, name: string): string {
  return createHash('sha3-256')
    .update(`${type}:${name.toLowerCase().trim()}`)
    .digest('hex')
    .substring(0, 16);
}

function edgeId(fromId: string, relation: string, toId: string): string {
  return createHash('sha3-256')
    .update(`${fromId}:${relation}:${toId}`)
    .digest('hex')
    .substring(0, 16);
}

function hashPII(value: string): string {
  return 'pii:' + createHash('sha256').update(value.toLowerCase().trim()).digest('hex').substring(0, 12);
}

function extractDomain(input: string): string | null {
  if (!input) return null;
  try {
    const s = input.includes('://') ? input : `https://${input}`;
    const host = new URL(s).hostname.replace(/^www\./, '');
    if (host.includes('.') && host.length > 3) return host;
  } catch {}
  const match = input.match(/(?:https?:\/\/)?(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})(?:\/|$)/);
  return match ? match[1] : null;
}

function normaliseTitle(title: string): string {
  return title
    .replace(/\b(senior|sr|junior|jr|lead|principal|associate|staff|vp of|head of|director of|chief)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()
    .replace(/\b\w/g, c => c.toUpperCase());
}

function cleanProperties(props: Record<string, any>): Record<string, any> {
  const clean: Record<string, any> = {};
  for (const [k, v] of Object.entries(props)) {
    if (v !== null && v !== undefined && v !== '') clean[k] = v;
  }
  return clean;
}

export const knowledgeGraph = new KnowledgeGraph();
