/**
 * mirofish-realtime-bridge.ts
 * 
 * Real-time bidirectional sync between Railway FalkorDB and MiroFish simulation.
 * Runs as background worker on Railway.
 */

import { createClient, RedisClientType } from 'redis';
import { KnowledgeGraph, GraphNode as Entity, GraphEdge as Relationship, EntityType } from './knowledge-graph.js';
import { EventEmitter } from 'events';

// Redis configuration - Railway provides REDIS_URL env var
const REDIS_URL = process.env.REDIS_URL || process.env.RAILWAY_REDIS_URL || 'redis://localhost:6379';

// MiroFish configuration
const MIROFISH_HOST = process.env.MIROFISH_HOST || 'http://localhost:7000';
const MIROFISH_API_KEY = process.env.MIROFISH_API_KEY || '';

// Sync configuration
const HIGH_PRIORITY_THRESHOLD = 0.8; // Confidence threshold for immediate sync
const BATCH_SIZE = 50;
const POLL_INTERVAL_MS = 100; // 100ms for real-time feel
const BATCH_INTERVAL_MS = 300000; // 5 minutes for bulk processing

interface MiroFishExportJob {
  entity_id: string;
  entity_type: EntityType;
  timestamp: number;
  priority: number;
  hop_depth: number;
}

interface MiroFishSimulationResult {
  sim_id: string;
  entity_id: string;
  predictions: Array<{
    entity: string;
    predicted_value: number;
    confidence: number;
    horizon: string;
  }>;
  causal_paths: Array<{
    from: string;
    to: string;
    path: string[];
    weight: number;
  }>;
  regime_transitions: Array<{
    entity: string;
    from_regime: string;
    to_regime: string;
    probability: number;
  }>;
  completed_at: string;
}

export class RealTimeMiroFishBridge {
  private redis: RedisClientType | null = null;
  private graph: KnowledgeGraph;
  private eventEmitter: EventEmitter;
  private isRunning = false;
  
  // Queue names
  private readonly PRIORITY_QUEUE = 'mirofish:priority';
  private readonly BATCH_QUEUE = 'mirofish:batch';
  private readonly RESULTS_CHANNEL = 'mirofish:results';
  private readonly EXPORT_CHANNEL = 'mirofish:export';

  constructor(graph: KnowledgeGraph) {
    this.graph = graph;
    this.eventEmitter = new EventEmitter();
  }

  /**
   * Initialize Redis connection and start sync loops
   */
  async initialize(): Promise<void> {
    console.log('[MIROFISH-BRIDGE] Initializing real-time sync...');
    
    // Connect to Redis
    this.redis = createClient({
      url: REDIS_URL,
      socket: {
        reconnectStrategy: (retries) => Math.min(retries * 100, 3000)
      }
    });

    this.redis.on('error', (err) => {
      console.error('[MIROFISH-BRIDGE] Redis error:', err);
    });

    await this.redis.connect();
    console.log('[MIROFISH-BRIDGE] Redis connected');

    // Subscribe to MiroFish results
    await this.subscribeToResults();
    
    // Start sync loops
    this.isRunning = true;
    this.startPrioritySyncLoop();
    this.startBatchSyncLoop();
    this.startExportListener();

    console.log('[MIROFISH-BRIDGE] Real-time sync initialized');
  }

  /**
   * Subscribe to graph entity creation events
   * Call this from server.ts when entities are created
   */
  async onEntityCreated(entity: Entity): Promise<void> {
    if (!this.redis) return;

    // Determine if entity is simulation-worthy
    if (!this.isSimulationWorthy(entity)) return;

    const priority = this.calculatePriority(entity);
    const job: MiroFishExportJob = {
      entity_id: entity.id,
      entity_type: entity.type,
      timestamp: Date.now(),
      priority,
      hop_depth: 3 // Default 3-hop neighborhood
    };

    // High priority → immediate queue
    if (priority >= HIGH_PRIORITY_THRESHOLD) {
      await this.redis.lPush(this.PRIORITY_QUEUE, JSON.stringify(job));
      console.log(`[MIROFISH-BRIDGE] Queued high-priority entity: ${entity.name}`);
    } else {
      // Low priority → batch queue
      await this.redis.lPush(this.BATCH_QUEUE, JSON.stringify(job));
    }
  }

  /**
   * Priority sync loop - handles urgent entities immediately
   */
  private async startPrioritySyncLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        if (!this.redis) {
          await this.sleep(1000);
          continue;
        }

        // Check priority queue
        const jobData = await this.redis.rPop(this.PRIORITY_QUEUE);
        
        if (jobData) {
          const job: MiroFishExportJob = JSON.parse(jobData);
          await this.exportAndSimulate(job);
        } else {
          // No high-priority jobs, sleep briefly
          await this.sleep(POLL_INTERVAL_MS);
        }
      } catch (error) {
        console.error('[MIROFISH-BRIDGE] Priority loop error:', error);
        await this.sleep(1000);
      }
    }
  }

  /**
   * Batch sync loop - processes low-priority entities in bulk
   */
  private async startBatchSyncLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        if (!this.redis) {
          await this.sleep(5000);
          continue;
        }

        // Get batch of low-priority jobs
        const batchData = await this.redis.lRange(this.BATCH_QUEUE, 0, BATCH_SIZE - 1);
        
        if (batchData.length > 0) {
          // Remove processed items from queue
          await this.redis.lTrim(this.BATCH_QUEUE, BATCH_SIZE, -1);
          
          // Process batch
          const jobs: MiroFishExportJob[] = batchData.map(d => JSON.parse(d));
          await this.exportBatch(jobs);
          
          console.log(`[MIROFISH-BRIDGE] Processed batch of ${jobs.length} entities`);
        }

        // Wait before next batch
        await this.sleep(BATCH_INTERVAL_MS);
      } catch (error) {
        console.error('[MIROFISH-BRIDGE] Batch loop error:', error);
        await this.sleep(5000);
      }
    }
  }

  /**
   * Subscribe to MiroFish simulation results
   */
  private async subscribeToResults(): Promise<void> {
    if (!this.redis) return;

    const subscriber = this.redis.duplicate();
    await subscriber.connect();

    await subscriber.subscribe(this.RESULTS_CHANNEL, async (message) => {
      try {
        const result: MiroFishSimulationResult = JSON.parse(message);
        await this.importSimulationResults(result);
      } catch (error) {
        console.error('[MIROFISH-BRIDGE] Failed to import results:', error);
      }
    });

    console.log('[MIROFISH-BRIDGE] Subscribed to results channel');
  }

  /**
   * Listen for export requests from other services
   */
  private async startExportListener(): Promise<void> {
    if (!this.redis) return;

    const subscriber = this.redis.duplicate();
    await subscriber.connect();

    await subscriber.subscribe(this.EXPORT_CHANNEL, async (message) => {
      try {
        const job: MiroFishExportJob = JSON.parse(message);
        
        // Route to appropriate queue
        if (job.priority >= HIGH_PRIORITY_THRESHOLD) {
          await this.redis!.lPush(this.PRIORITY_QUEUE, JSON.stringify(job));
        } else {
          await this.redis!.lPush(this.BATCH_QUEUE, JSON.stringify(job));
        }
      } catch (error) {
        console.error('[MIROFISH-BRIDGE] Export listener error:', error);
      }
    });
  }

  /**
   * Export single entity and run simulation
   */
  private async exportAndSimulate(job: MiroFishExportJob): Promise<void> {
    try {
      console.log(`[MIROFISH-BRIDGE] Exporting entity: ${job.entity_id}`);

      // 1. Extract N-hop neighborhood
      const subgraph = await this.extractSubgraph(job.entity_id, job.hop_depth);
      
      if (!subgraph) {
        console.log(`[MIROFISH-BRIDGE] No subgraph found for ${job.entity_id}`);
        return;
      }
      
      // 2. Export to MiroFish
      const simId = await this.exportToMiroFish(subgraph);
      
      if (simId) {
        console.log(`[MIROFISH-BRIDGE] Started simulation: ${simId}`);
        
        // 3. Poll for results (async)
        this.pollForResults(simId, job.entity_id);
      }
    } catch (error) {
      console.error(`[MIROFISH-BRIDGE] Export failed for ${job.entity_id}:`, error);
    }
  }

  /**
   * Export batch of entities
   */
  private async exportBatch(jobs: MiroFishExportJob[]): Promise<void> {
    try {
      console.log(`[MIROFISH-BRIDGE] Exporting batch of ${jobs.length} entities`);

      // Extract merged subgraph for all entities
      const mergedSubgraph = await this.extractMergedSubgraph(jobs);
      
      // Single batch export
      const simId = await this.exportToMiroFish(mergedSubgraph);
      
      if (simId) {
        console.log(`[MIROFISH-BRIDGE] Started batch simulation: ${simId}`);
        this.pollForResults(simId, 'batch');
      }
    } catch (error) {
      console.error('[MIROFISH-BRIDGE] Batch export failed:', error);
    }
  }

  /**
   * Extract N-hop neighborhood subgraph
   */
  private async extractSubgraph(centerId: string, depth: number): Promise<any> {
    // Query the graph for N-hop neighborhood
    const result = await this.graph.graphQuery(`
      MATCH path = (center:Entity {id: $centerId})-[*1..${depth}]-(neighbor:Entity)
      WITH center, neighbor, relationships(path) as rels
      RETURN center, collect(DISTINCT neighbor) as neighbors, collect(DISTINCT rels) as edges
    `, { centerId });

    return this.formatSubgraph(result);
  }

  /**
   * Extract merged subgraph for multiple entities
   */
  private async extractMergedSubgraph(jobs: MiroFishExportJob[]): Promise<any> {
    const entityIds = jobs.map(j => j.entity_id);
    
    const result = await this.graph.graphQuery(`
      UNWIND $entityIds as id
      MATCH (center:Entity {id: id})
      OPTIONAL MATCH path = (center)-[*1..2]-(neighbor:Entity)
      WITH center, collect(DISTINCT neighbor) as neighbors, collect(DISTINCT relationships(path)) as edges
      RETURN collect(DISTINCT center) as centers, 
             apoc.coll.flatten(collect(neighbors)) as all_neighbors,
             apoc.coll.flatten(collect(edges)) as all_edges
    `, { entityIds });

    return this.formatSubgraph(result);
  }

  /**
   * Format subgraph for MiroFish export
   */
  private formatSubgraph(queryResult: any): any {
    // Transform graph data to MiroFish format
    return {
      entities: queryResult.entities || [],
      relationships: queryResult.edges || [],
      metadata: {
        exported_at: new Date().toISOString(),
        source: 'railway_falkordb',
        version: '2.0'
      }
    };
  }

  /**
   * Export subgraph to MiroFish
   */
  private async exportToMiroFish(subgraph: any): Promise<string | null> {
    try {
      const response = await fetch(`${MIROFISH_HOST}/api/v1/simulations`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${MIROFISH_API_KEY}`
        },
        body: JSON.stringify({
          graph: subgraph,
          config: {
            agent_count: 1000,
            duration_ticks: 1000,
            temperature: 1.0,
            hawkes_params: {
              mu: 0.01,
              alpha: 0.5,
              beta: 0.1
            }
          }
        })
      });

      if (!response.ok) {
        throw new Error(`MiroFish export failed: ${response.status}`);
      }

      const data = await response.json();
      return data.simulation_id;
    } catch (error) {
      console.error('[MIROFISH-BRIDGE] Export to MiroFish failed:', error);
      return null;
    }
  }

  /**
   * Poll for simulation results
   */
  private async pollForResults(simId: string, entityId: string): Promise<void> {
    const maxAttempts = 60; // 5 minutes with 5s intervals
    let attempts = 0;

    while (attempts < maxAttempts) {
      try {
        const response = await fetch(`${MIROFISH_HOST}/api/v1/simulations/${simId}/results`, {
          headers: {
            'Authorization': `Bearer ${MIROFISH_API_KEY}`
          }
        });

        if (response.ok) {
          const result: MiroFishSimulationResult = await response.json();
          
          // Publish results to Redis for import
          if (this.redis) {
            await this.redis.publish(this.RESULTS_CHANNEL, JSON.stringify(result));
          }
          
          console.log(`[MIROFISH-BRIDGE] Simulation ${simId} completed`);
          return;
        }

        if (response.status === 202) {
          // Still running
          await this.sleep(5000);
          attempts++;
          continue;
        }

        throw new Error(`Unexpected status: ${response.status}`);
      } catch (error) {
        console.error(`[MIROFISH-BRIDGE] Poll error for ${simId}:`, error);
        await this.sleep(5000);
        attempts++;
      }
    }

    console.error(`[MIROFISH-BRIDGE] Simulation ${simId} timed out`);
  }

  /**
   * Import simulation results into graph
   */
  private async importSimulationResults(result: MiroFishSimulationResult): Promise<void> {
    console.log(`[MIROFISH-BRIDGE] Importing results from simulation: ${result.sim_id}`);

    try {
      // 1. Create SimEpisode node
      await this.createSimEpisode(result);

      // 2. Import predictions as signals
      await this.importPredictions(result);

      // 3. Extract and create causal relationships
      const newConnections = this.extractCausalConnections(result);
      await this.createCausalRelationships(newConnections);

      // 4. Update entity regimes
      await this.updateRegimes(result);

      console.log(`[MIROFISH-BRIDGE] Imported results: ${result.predictions.length} predictions, ${newConnections.length} new connections`);
    } catch (error) {
      console.error('[MIROFISH-BRIDGE] Import failed:', error);
    }
  }

  /**
   * Create SimEpisode node
   */
  private async createSimEpisode(result: MiroFishSimulationResult): Promise<void> {
    await this.graph.graphQuery(`
      MERGE (s:SimEpisode {id: $sim_id})
      SET s.entity_id = $entity_id,
          s.predictions_count = $predictions_count,
          s.completed_at = $completed_at,
          s.created_at = datetime()
    `, {
      sim_id: result.sim_id,
      entity_id: result.entity_id,
      predictions_count: result.predictions.length,
      completed_at: result.completed_at
    });
  }

  /**
   * Import predictions as time-series signals
   */
  private async importPredictions(result: MiroFishSimulationResult): Promise<void> {
    for (const pred of result.predictions) {
      await this.graph.graphQuery(`
        MATCH (e:Entity {name: $entity})
        MATCH (s:SimEpisode {id: $sim_id})
        MERGE (e)-[sig:SIGNAL {type: 'sim_prediction', horizon: $horizon}]->(s)
        SET sig.predicted_value = $predicted_value,
            sig.confidence = $confidence,
            sig.timestamp = datetime()
      `, {
        entity: pred.entity,
        sim_id: result.sim_id,
        horizon: pred.horizon,
        predicted_value: pred.predicted_value,
        confidence: pred.confidence
      });
    }
  }

  /**
   * Extract causal connections from simulation paths
   */
  private extractCausalConnections(result: MiroFishSimulationResult): Array<Partial<Relationship>> {
    const connections: Array<Partial<Relationship>> = [];

    for (const path of result.causal_paths) {
      // Create direct causal link
      connections.push({
        from: path.from,
        to: path.to,
        relation: 'causes',
        weight: path.weight,
        confidence: Math.min(path.weight * 1.5, 0.95),
        properties: {
          path_length: path.path.length,
          source: 'mirofish_simulation',
          sim_id: result.sim_id
        }
      });

      // Create intermediate connections for full path
      for (let i = 0; i < path.path.length - 1; i++) {
        connections.push({
          from: path.path[i],
          to: path.path[i + 1],
          relation: 'enables',
          weight: path.weight * 0.8,
          confidence: Math.min(path.weight * 1.2, 0.9),
          properties: {
            path_position: i,
            source: 'mirofish_simulation',
            sim_id: result.sim_id
          }
        });
      }
    }

    return connections;
  }

  /**
   * Create causal relationships in graph
   */
  private async createCausalRelationships(connections: Array<Partial<Relationship>>): Promise<void> {
    // Batch create using UNWIND
    await this.graph.graphQuery(`
      UNWIND $connections as conn
      MATCH (a:Entity {id: conn.from})
      MATCH (b:Entity {id: conn.to})
      MERGE (a)-[r:RELATES {type: conn.relation}]->(b)
      SET r.weight = conn.weight,
          r.confidence = conn.confidence,
          r += conn.properties,
          r.created_at = datetime()
    `, { connections });
  }

  /**
   * Update entity regimes based on simulation
   */
  private async updateRegimes(result: MiroFishSimulationResult): Promise<void> {
    for (const transition of result.regime_transitions) {
      await this.graph.graphQuery(`
        MATCH (e:Entity {name: $entity})
        SET e.regime = $to_regime,
            e.predicted_regime = $to_regime,
            e.regime_probability = $probability,
            e.regime_updated_at = datetime()
      `, {
        entity: transition.entity,
        to_regime: transition.to_regime,
        probability: transition.probability
      });
    }
  }

  /**
   * Determine if entity is worth simulating
   */
  private isSimulationWorthy(entity: Entity): boolean {
    const simulationTypes: EntityType[] = [
      'Corporation',
      'FinancialInstitution',
      'Nation',
      'MarketCrash',
      'Asset',
      'FinancialInstrument'
    ];

    return simulationTypes.includes(entity.type);
  }

  /**
   * Calculate simulation priority (0-1)
   */
  private calculatePriority(entity: Entity): number {
    let priority = 0.5;

    // Higher priority for financial entities
    if (['FinancialInstitution', 'MarketCrash'].includes(entity.type)) {
      priority += 0.3;
    }

    // Higher priority if entity has many connections
    if (entity.connection_count && entity.connection_count > 5) {
      priority += 0.2;
    }

    // Higher priority if in stressed regime
    if (entity.regime === 'stressed' || entity.regime === 'pre_tipping') {
      priority += 0.3;
    }

    return Math.min(priority, 1.0);
  }

  /**
   * Graceful shutdown
   */
  async shutdown(): Promise<void> {
    console.log('[MIROFISH-BRIDGE] Shutting down...');
    this.isRunning = false;
    
    if (this.redis) {
      await this.redis.quit();
    }
    
    console.log('[MIROFISH-BRIDGE] Shutdown complete');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Singleton instance
let bridgeInstance: RealTimeMiroFishBridge | null = null;

export function getMiroFishBridge(graph: KnowledgeGraph): RealTimeMiroFishBridge {
  if (!bridgeInstance) {
    bridgeInstance = new RealTimeMiroFishBridge(graph);
  }
  return bridgeInstance;
}

export default RealTimeMiroFishBridge;
