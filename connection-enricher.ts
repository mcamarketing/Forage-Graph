/**
 * connection-enricher.ts
 * 
 * Automated connection enrichment engine using:
 * 1. FIBO ontology inference (free, deterministic)
 * 2. Structural link prediction (Adamic-Adar)
 * 3. Temporal co-occurrence analysis
 * 
 * Runs continuously on Railway to grow connection density.
 */

import { KnowledgeGraph, GraphNode as Entity, GraphEdge as Relationship, EntityType, RelationType } from './knowledge-graph.js';

// Enrichment configuration
const MIN_CONFIDENCE = 0.6;
const MAX_CONNECTIONS_PER_ENTITY = 20;
const ENRICHMENT_BATCH_SIZE = 50;
const ENRICHMENT_INTERVAL_MS = 5000; // 5 seconds

interface EnrichmentRule {
  name: string;
  match: (entity: Entity) => boolean;
  infer: (entity: Entity, graph: KnowledgeGraph) => Promise<Partial<Relationship>[]>;
}

interface LinkPrediction {
  from_id: string;
  to_id: string;
  relation: RelationType;
  confidence: number;
  source: string;
  weight?: number;
}

export class ConnectionEnricher {
  private graph: KnowledgeGraph;
  private isRunning = false;
  private rules: EnrichmentRule[];

  constructor(graph: KnowledgeGraph) {
    this.graph = graph;
    this.rules = this.initializeRules();
  }

  /**
   * Start continuous enrichment loop
   */
  async start(): Promise<void> {
    if (this.isRunning) return;
    
    this.isRunning = true;
    console.log('[ENRICH] Starting connection enrichment engine...');
    
    while (this.isRunning) {
      try {
        await this.enrichBatch();
        await this.sleep(ENRICHMENT_INTERVAL_MS);
      } catch (error) {
        console.error('[ENRICH] Error in enrichment loop:', error);
        await this.sleep(10000); // Wait longer on error
      }
    }
  }

  /**
   * Stop enrichment
   */
  stop(): void {
    this.isRunning = false;
    console.log('[ENRICH] Stopped');
  }

  /**
   * Enrich a batch of sparse entities
   */
  private async enrichBatch(): Promise<void> {
    // Find entities with few connections
    const sparseEntities = await this.getSparseEntities(ENRICHMENT_BATCH_SIZE);
    
    if (sparseEntities.length === 0) {
      return; // Nothing to enrich
    }

    console.log(`[ENRICH] Processing ${sparseEntities.length} entities...`);

    for (const entity of sparseEntities) {
      try {
        // Run all inference strategies in parallel
        const [fiboConnections, structuralConnections, temporalConnections] = await Promise.all([
          this.inferFIBOConnections(entity),
          this.inferStructuralConnections(entity),
          this.inferTemporalConnections(entity)
        ]);

        // Merge and deduplicate
        const allConnections = this.deduplicateConnections([
          ...fiboConnections,
          ...structuralConnections,
          ...temporalConnections
        ]);

        // Filter by confidence threshold
        const highConfidenceConnections = allConnections.filter(
          c => c.confidence >= MIN_CONFIDENCE
        );

        // Batch create relationships
        if (highConfidenceConnections.length > 0) {
          await this.batchCreateRelationships(highConfidenceConnections);
          
          // Update entity metadata
          await this.updateEntityEnrichmentStatus(
            entity.id!, 
            highConfidenceConnections.length
          );

          console.log(`[ENRICH] Created ${highConfidenceConnections.length} connections for ${entity.name}`);
        }
      } catch (error) {
        console.error(`[ENRICH] Failed to enrich entity ${entity.id}:`, error);
      }
    }
  }

  /**
   * Get entities with sparse connections
   */
  private async getSparseEntities(limit: number): Promise<Entity[]> {
    return await this.graph.rawCypherQuery(`
      MATCH (e:Entity)
      WHERE e.connection_count IS NULL OR e.connection_count < 5
      RETURN e
      ORDER BY e.last_enriched ASC NULLS FIRST, e.created_at ASC
      LIMIT $limit
    `, { limit }) as Entity[];
  }

  /**
   * Initialize FIBO and logical inference rules
   */
  private initializeRules(): EnrichmentRule[] {
    return [
      // FIBO Transitive: A works_at B + B subsidiary_of C → A works_at C
      {
        name: 'fibo_transitive_employment',
        match: (e) => e.type === 'Person' && !!e.properties?.employer,
        infer: async (entity, graph) => {
          const connections: Partial<Relationship>[] = [];
          
          // Get employer
          const employer = await graph.rawCypherQuery(`
            MATCH (p:Entity {id: $personId})-[r:works_at]->(e:Entity)
            RETURN e
          `, { personId: entity.id });

          if (employer) {
            // Check for parent company
            const parent = await graph.rawCypherQuery(`
              MATCH (e:Entity {id: $employerId})-[r:subsidiary_of]->(p:Entity)
              RETURN p
            `, { employerId: employer[0]?.id });

            if (parent && parent[0]) {
              connections.push({
                from_id: entity.id,
                to_id: parent[0].id,
                relation: 'works_at' as RelationType,
                confidence: 0.7,
                properties: {
                  inferred: true,
                  rule: 'fibo_transitive',
                  via: employer[0].name
                }
              });
            }
          }

          return connections;
        }
      },

      // FIBO Inverse: A owns B → B owned_by A
      {
        name: 'fibo_inverse_ownership',
        match: (e) => e.type === 'LegalEntity' || e.type === 'Corporation',
        infer: async (entity, graph) => {
          const connections: Partial<Relationship>[] = [];
          
          // Check if entity owns others
          const owned = await graph.rawCypherQuery(`
            MATCH (owner:Entity {id: $id})-[r:owns]->(owned:Entity)
            RETURN owned
          `, { id: entity.id });

          for (const o of owned || []) {
            connections.push({
              from_id: o.id,
              to_id: entity.id,
              relation: 'owned_by' as RelationType,
              confidence: 0.95, // High confidence for inverse
              properties: {
                inferred: true,
                rule: 'fibo_inverse',
                original_relation: 'owns'
              }
            });
          }

          return connections;
        }
      },

      // FIBO Symmetric: A competitor_of B → B competitor_of A
      {
        name: 'fibo_symmetric_competition',
        match: (e) => e.type === 'Corporation' || e.type === 'Product',
        infer: async (entity, graph) => {
          const connections: Partial<Relationship>[] = [];
          
          const competitors = await graph.rawCypherQuery(`
            MATCH (e:Entity {id: $id})-[r:competitor_of]->(c:Entity)
            RETURN c
          `, { id: entity.id });

          for (const c of competitors || []) {
            // Check if reverse exists
            const reverseExists = await graph.rawCypherQuery(`
              MATCH (c:Entity {id: $cId})-[r:competitor_of]->(e:Entity {id: $eId})
              RETURN r
            `, { cId: c.id, eId: entity.id });

            if (!reverseExists || reverseExists.length === 0) {
              connections.push({
                from_id: c.id,
                to_id: entity.id,
                relation: 'competitor_of' as RelationType,
                confidence: 0.95,
                properties: {
                  inferred: true,
                  rule: 'fibo_symmetric'
                }
              });
            }
          }

          return connections;
        }
      },

      // Geographic containment: City in Region → City in Nation
      {
        name: 'geo_containment',
        match: (e) => e.type === 'City' || e.type === 'Region',
        infer: async (entity, graph) => {
          const connections: Partial<Relationship>[] = [];
          
          if (entity.type === 'City') {
            // City → Region → Nation
            const region = await graph.rawCypherQuery(`
              MATCH (city:Entity {id: $id})-[r:located_in]->(region:Entity)
              WHERE region.type = 'Region'
              RETURN region
            `, { id: entity.id });

            if (region && region[0]) {
              const nation = await graph.rawCypherQuery(`
                MATCH (region:Entity {id: $regionId})-[r:located_in]->(nation:Entity)
                WHERE nation.type = 'Nation'
                RETURN nation
              `, { regionId: region[0].id });

              if (nation && nation[0]) {
                connections.push({
                  from_id: entity.id,
                  to_id: nation[0].id,
                  relation: 'located_in' as RelationType,
                  confidence: 0.85,
                  properties: {
                    inferred: true,
                    rule: 'geo_transitive',
                    via_region: region[0].name
                  }
                });
              }
            }
          }

          return connections;
        }
      }
    ];
  }

  /**
   * Infer connections using FIBO ontology rules
   */
  private async inferFIBOConnections(entity: Entity): Promise<LinkPrediction[]> {
    const connections: LinkPrediction[] = [];

    // Apply matching rules
    for (const rule of this.rules) {
      try {
        if (rule.match(entity)) {
          const inferred = await rule.infer(entity, this.graph);
          connections.push(...inferred.map(i => ({
            from_id: i.from_id!,
            to_id: i.to_id!,
            relation: i.relation!,
            confidence: i.confidence || 0.7,
            source: rule.name,
            weight: i.weight
          })));
        }
      } catch (error) {
        console.error(`[ENRICH] Rule ${rule.name} failed:`, error);
      }
    }

    return connections;
  }

  /**
   * Infer connections using structural graph metrics (Adamic-Adar)
   */
  private async inferStructuralConnections(entity: Entity): Promise<LinkPrediction[]> {
    const connections: LinkPrediction[] = [];

    try {
      // Get neighbors of the entity
      const neighbors = await this.graph.rawCypherQuery(`
        MATCH (e:Entity {id: $id})-[:RELATES]-(neighbor:Entity)
        RETURN neighbor
      `, { id: entity.id }) as Entity[];

      if (neighbors.length < 2) {
        return connections; // Need at least 2 neighbors for Adamic-Adar
      }

      // For each pair of neighbors, calculate Adamic-Adar score
      for (let i = 0; i < neighbors.length; i++) {
        for (let j = i + 1; j < neighbors.length; j++) {
          const neighborA = neighbors[i];
          const neighborB = neighbors[j];

          // Check if already connected
          const existingConnection = await this.graph.rawCypherQuery(`
            MATCH (a:Entity {id: $aId})-[:RELATES]-(b:Entity {id: $bId})
            RETURN count(*) as count
          `, { aId: neighborA.id, bId: neighborB.id });

          if (existingConnection && existingConnection[0]?.count > 0) {
            continue; // Already connected
          }

          // Calculate Adamic-Adar score
          const score = await this.calculateAdamicAdar(neighborA.id!, neighborB.id!);

          if (score > 0.3) { // Threshold for prediction
            connections.push({
              from_id: neighborA.id!,
              to_id: neighborB.id!,
              relation: 'similar_to' as RelationType,
              confidence: Math.min(score * 1.5, 0.9),
              source: 'adamic_adar',
              weight: score
            });
          }
        }
      }

      // Also predict connections to the center entity's neighbors' neighbors
      const secondDegreeConnections = await this.inferSecondDegreeConnections(
        entity.id!, 
        neighbors
      );
      connections.push(...secondDegreeConnections);

    } catch (error) {
      console.error('[ENRICH] Structural inference failed:', error);
    }

    return connections;
  }

  /**
   * Calculate Adamic-Adar similarity between two nodes
   */
  private async calculateAdamicAdar(nodeA: string, nodeB: string): Promise<number> {
    try {
      const result = await this.graph.rawCypherQuery(`
        MATCH (a:Entity {id: $aId})-[:RELATES]-(common:Entity)-[:RELATES]-(b:Entity {id: $bId})
        WITH common, count {.*} as degree
        RETURN sum(1.0 / log(degree + 1)) as score
      `, { aId: nodeA, bId: nodeB });

      return result && result[0] ? result[0].score || 0 : 0;
    } catch {
      return 0;
    }
  }

  /**
   * Infer second-degree connections (friends of friends)
   */
  private async inferSecondDegreeConnections(
    entityId: string, 
    neighbors: Entity[]
  ): Promise<LinkPrediction[]> {
    const connections: LinkPrediction[] = [];

    for (const neighbor of neighbors) {
      // Get neighbors of neighbor (excluding the original entity)
      const secondDegree = await this.graph.rawCypherQuery(`
        MATCH (n:Entity {id: $nId})-[:RELATES]-(friend:Entity)
        WHERE friend.id <> $entityId
        RETURN friend
        LIMIT 10
      `, { nId: neighbor.id, entityId });

      for (const friend of secondDegree || []) {
        // Calculate common neighbor count
        const commonCount = await this.graph.rawCypherQuery(`
          MATCH (e:Entity {id: $eId})-[:RELATES]-(common:Entity)-[:RELATES]-(f:Entity {id: $fId})
          RETURN count(common) as common_neighbors
        `, { eId: entityId, fId: friend.id });

        const commonNeighbors = commonCount && commonCount[0]?.common_neighbors || 0;

        if (commonNeighbors >= 2) {
          connections.push({
            from_id: entityId,
            to_id: friend.id,
            relation: 'related_to' as RelationType,
            confidence: Math.min(0.5 + (commonNeighbors * 0.1), 0.8),
            source: 'second_degree',
            weight: commonNeighbors
          });
        }
      }
    }

    return connections;
  }

  /**
   * Infer connections based on temporal co-occurrence
   */
  private async inferTemporalConnections(entity: Entity): Promise<LinkPrediction[]> {
    const connections: LinkPrediction[] = [];

    try {
      // Find entities that appear in same sources (news, reports, etc.)
      const cooccurring = await this.graph.rawCypherQuery(`
        MATCH (a:Entity {id: $id})-[m:MENTIONED_IN]->(s:Source)
        MATCH (b:Entity)-[m2:MENTIONED_IN]->(s)
        WHERE a <> b
        WITH b, count(s) as cooccurrence, collect(s.timestamp) as timestamps
        WHERE cooccurrence >= 2
        RETURN b, cooccurrence, timestamps
        ORDER BY cooccurrence DESC
        LIMIT 20
      `, { id: entity.id });

      for (const row of cooccurring || []) {
        const timeSpan = this.calculateTimeSpan(row.timestamps);
        
        // Higher confidence if co-occurrence is recent and frequent
        const recencyBoost = timeSpan < 30 ? 0.2 : 0; // Within 30 days
        const frequencyBoost = Math.min(row.cooccurrence * 0.05, 0.3);
        
        connections.push({
          from_id: entity.id!,
          to_id: row.b.id,
          relation: 'cooccurs_with' as RelationType,
          confidence: Math.min(0.6 + frequencyBoost + recencyBoost, 0.95),
          source: 'temporal_cooccurrence',
          weight: row.cooccurrence
        });
      }

      // Also check for entities appearing in same simulations
      const simCooccurring = await this.graph.rawCypherQuery(`
        MATCH (a:Entity {id: $id})-[:SIMULATES|PARTICIPATES_IN]->(sim:SimEpisode)
        MATCH (b:Entity)-[:SIMULATES|PARTICIPATES_IN]->(sim)
        WHERE a <> b
        WITH b, count(sim) as sim_count
        WHERE sim_count >= 1
        RETURN b, sim_count
        ORDER BY sim_count DESC
        LIMIT 10
      `, { id: entity.id });

      for (const row of simCooccurring || []) {
        connections.push({
          from_id: entity.id!,
          to_id: row.b.id,
          relation: 'simulated_with' as RelationType,
          confidence: Math.min(0.7 + (row.sim_count * 0.1), 0.95),
          source: 'simulation_cooccurrence',
          weight: row.sim_count
        });
      }

    } catch (error) {
      console.error('[ENRICH] Temporal inference failed:', error);
    }

    return connections;
  }

  /**
   * Calculate time span in days from timestamps
   */
  private calculateTimeSpan(timestamps: string[]): number {
    if (!timestamps || timestamps.length < 2) return 0;
    
    const dates = timestamps.map(t => new Date(t)).sort();
    const first = dates[0];
    const last = dates[dates.length - 1];
    
    return Math.floor((last.getTime() - first.getTime()) / (1000 * 60 * 60 * 24));
  }

  /**
   * Deduplicate connections by from/to/relation
   */
  private deduplicateConnections(connections: LinkPrediction[]): LinkPrediction[] {
    const seen = new Set<string>();
    const unique: LinkPrediction[] = [];

    for (const conn of connections) {
      const key = `${conn.from_id}:${conn.to_id}:${conn.relation}`;
      
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(conn);
      } else {
        // If duplicate, keep the one with higher confidence
        const existing = unique.find(u => 
          u.from_id === conn.from_id && u.to_id === conn.to_id && u.relation === conn.relation
        );
        if (existing && conn.confidence > existing.confidence) {
          existing.confidence = conn.confidence;
          existing.weight = conn.weight;
        }
      }
    }

    return unique;
  }

  /**
   * Batch create relationships using UNWIND
   */
  private async batchCreateRelationships(connections: LinkPrediction[]): Promise<void> {
    if (connections.length === 0) return;

    // Prepare batch for Cypher
    const batch = connections.map(c => ({
      from_id: c.from_id,
      to_id: c.to_id,
      relation: c.relation,
      confidence: c.confidence,
      weight: c.weight || 0.5,
      inferred: true,
      enrichment_source: c.source,
      created_at: new Date().toISOString()
    }));

    await this.graph.rawCypherQuery(`
      UNWIND $batch as row
      MERGE (a:Entity {id: row.from_id})
      MERGE (b:Entity {id: row.to_id})
      MERGE (a)-[r:RELATES {type: row.relation}]->(b)
      SET r.confidence = row.confidence,
          r.weight = row.weight,
          r.inferred = row.inferred,
          r.enrichment_source = row.enrichment_source,
          r.created_at = datetime(row.created_at)
    `, { batch });
  }

  /**
   * Update entity enrichment status
   */
  private async updateEntityEnrichmentStatus(
    entityId: string, 
    newConnections: number
  ): Promise<void> {
    await this.graph.rawCypherQuery(`
      MATCH (e:Entity {id: $id})
      SET e.last_enriched = datetime(),
          e.connection_count = coalesce(e.connection_count, 0) + $newConnections,
          e.enrichment_version = '2.0'
    `, { id: entityId, newConnections });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Singleton
let enricherInstance: ConnectionEnricher | null = null;

export function getConnectionEnricher(graph: KnowledgeGraph): ConnectionEnricher {
  if (!enricherInstance) {
    enricherInstance = new ConnectionEnricher(graph);
  }
  return enricherInstance;
}

export default ConnectionEnricher;
