/**
 * Forage Graph API — src/server.ts
 *
 * Standalone Express server. Runs on Railway / any VPS.
 * The Apify actor POSTs to this after every tool call (fire and forget).
 * All graph data lives here — never on Apify infrastructure.
 *
 * Endpoints:
 *   POST /ingest              — receive tool output, extract entities, merge into graph
 *   POST /query               — find entities by name
 *   POST /enrich              — everything the graph knows about a domain/company
 *   POST /connections         — find relationship path between two entities
 *   POST /neighbors_2hop      — 2-hop neighborhood with parameterized rel_types [M1]
 *   POST /claim               — add a claim/provenance assertion
 *   GET  /claims/:entityName  — get all claims for an entity
 *   POST /regime              — set regime label on entity
 *   POST /signal              — add time-series signal data
 *   GET  /signals/:entityName — get signals for entity
 *   POST /causal_parents     — what drives this entity upstream
 *   POST /causal_children    — what this entity drives downstream
 *   POST /causal_path        — highest causal-weight path between entities
 *   POST /simulate           — propagate shock/boost/remove through graph
 *   GET  /stats              — graph size and coverage
 *   GET  /health             — liveness check
 *   GET  /metrics            — Prometheus-compatible metrics [M1]
 *   GET  /export             — export entire graph as JSON backup [backup-001]
 *   POST /import             — import graph from JSON backup [backup-001]
 *   POST /ingest_raw_batch   — batch ingest entities with relationships
 *
 * Entity Types: SimAgent, SimEpisode (simulation layer) [M1]
 * Relation Types: READS_FROM, SIMULATES, HYPOTHESIZES (simulation boundary) [M1]
 *
 * Auth: Bearer token via GRAPH_API_SECRET env var.
 * All write endpoints require auth. /health is open.
 */

import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { knowledgeGraph } from './knowledge-graph.js';

const app  = express();
const PORT = process.env.PORT || 3000;
const SECRET = process.env.GRAPH_API_SECRET;

// ─── OBSERVABILITY: LATENCY HISTOGRAM [obs-001] ────────────────────────────────
// Track request latencies for /metrics endpoint

interface LatencyBucket {
  le: number;      // Less than or equal to (ms)
  count: number;   // Requests in this bucket
}

const latencyHistogram: Map<string, LatencyBucket[]> = new Map();
const requestCounts: Map<string, number> = new Map();
const errorCounts: Map<string, number> = new Map();

function initHistogram(): LatencyBucket[] {
  return [
    { le: 10, count: 0 },
    { le: 50, count: 0 },
    { le: 100, count: 0 },
    { le: 250, count: 0 },
    { le: 500, count: 0 },
    { le: 1000, count: 0 },
    { le: 2500, count: 0 },
    { le: 5000, count: 0 },
    { le: Infinity, count: 0 },
  ];
}

function recordLatency(endpoint: string, durationMs: number): void {
  if (!latencyHistogram.has(endpoint)) {
    latencyHistogram.set(endpoint, initHistogram());
  }
  const buckets = latencyHistogram.get(endpoint)!;
  for (const bucket of buckets) {
    if (durationMs <= bucket.le) {
      bucket.count++;
      break;
    }
  }
  requestCounts.set(endpoint, (requestCounts.get(endpoint) || 0) + 1);
}

function recordError(endpoint: string): void {
  errorCounts.set(endpoint, (errorCounts.get(endpoint) || 0) + 1);
}

if (!SECRET) {
  console.error('GRAPH_API_SECRET env var is required');
  process.exit(1);
}

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────

app.use(express.json({ limit: '10mb' }));
app.use(cors({
  origin: process.env.CORS_ORIGIN || '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Request logging + latency tracking [obs-002]
app.use((req: Request, res: Response, next: NextFunction) => {
  const start = Date.now();
  console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);

  res.on('finish', () => {
    const duration = Date.now() - start;
    const endpoint = `${req.method} ${req.path.split('/')[1] || 'root'}`;
    recordLatency(endpoint, duration);
    if (res.statusCode >= 400) {
      recordError(endpoint);
    }
  });

  next();
});

// Auth — all routes except /health require Bearer token
function requireAuth(req: Request, res: Response, next: NextFunction): void {
  if (req.path === '/health') { next(); return; }
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ') || auth.slice(7) !== SECRET) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
}

app.use(requireAuth);

// ─── HEALTH ───────────────────────────────────────────────────────────────────

app.get('/health', async (_req: Request, res: Response) => {
  const healthy = await knowledgeGraph.isHealthy();
  res.status(healthy ? 200 : 503).json({
    status: healthy ? 'ok' : 'degraded',
    graph: healthy ? 'connected' : 'disconnected',
    ts: new Date().toISOString(),
  });
});

// ─── METRICS [obs-003] ───────────────────────────────────────────────────────
// Prometheus-compatible metrics endpoint with node/edge counts + latency histogram

app.get('/metrics', async (_req: Request, res: Response) => {
  try {
    const stats = await knowledgeGraph.getStats();

    // Build Prometheus-format output
    const lines: string[] = [];

    // Graph metrics
    lines.push('# HELP forage_graph_nodes_total Total number of nodes in the graph');
    lines.push('# TYPE forage_graph_nodes_total gauge');
    lines.push(`forage_graph_nodes_total ${stats.total_nodes}`);

    lines.push('# HELP forage_graph_edges_total Total number of edges in the graph');
    lines.push('# TYPE forage_graph_edges_total gauge');
    lines.push(`forage_graph_edges_total ${stats.total_edges}`);

    // Nodes by type
    lines.push('# HELP forage_graph_nodes_by_type Nodes by entity type');
    lines.push('# TYPE forage_graph_nodes_by_type gauge');
    for (const [type, count] of Object.entries(stats.nodes_by_type)) {
      lines.push(`forage_graph_nodes_by_type{type="${type}"} ${count}`);
    }

    // Request latency histogram
    lines.push('# HELP forage_http_request_duration_ms HTTP request latency histogram');
    lines.push('# TYPE forage_http_request_duration_ms histogram');
    for (const [endpoint, buckets] of latencyHistogram) {
      let cumulative = 0;
      for (const bucket of buckets) {
        cumulative += bucket.count;
        const le = bucket.le === Infinity ? '+Inf' : bucket.le;
        lines.push(`forage_http_request_duration_ms_bucket{endpoint="${endpoint}",le="${le}"} ${cumulative}`);
      }
      lines.push(`forage_http_request_duration_ms_count{endpoint="${endpoint}"} ${requestCounts.get(endpoint) || 0}`);
    }

    // Request counts
    lines.push('# HELP forage_http_requests_total Total HTTP requests');
    lines.push('# TYPE forage_http_requests_total counter');
    for (const [endpoint, count] of requestCounts) {
      lines.push(`forage_http_requests_total{endpoint="${endpoint}"} ${count}`);
    }

    // Error counts
    lines.push('# HELP forage_http_errors_total Total HTTP errors');
    lines.push('# TYPE forage_http_errors_total counter');
    for (const [endpoint, count] of errorCounts) {
      lines.push(`forage_http_errors_total{endpoint="${endpoint}"} ${count}`);
    }

    res.set('Content-Type', 'text/plain; charset=utf-8');
    res.send(lines.join('\n'));
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── INGEST ───────────────────────────────────────────────────────────────────
// Called by Apify actor after every tool response.
// Now waits for DB write to complete and reports actual status.
//
// Body: { tool_name: string, result: any }

app.post('/ingest', async (req: Request, res: Response) => {
  const { tool_name, result } = req.body;

  if (!tool_name || result === undefined) {
    res.status(400).json({ error: 'tool_name and result are required' });
    return;
  }

  try {
    // Wait for actual DB write to complete
    await knowledgeGraph.ingest(tool_name, result);
    res.status(202).json({ accepted: true, processed: true });
  } catch (err: any) {
    console.error('[INGEST ERROR]', err.message);
    res.status(500).json({ accepted: false, error: err.message });
  }
});

// ─── QUERY ────────────────────────────────────────────────────────────────────
// Find entities by name, optionally filtered by type.
//
// Body: { name: string, type?: EntityType, min_confidence?: number }

app.post('/query', async (req: Request, res: Response) => {
  try {
    const { name, type, min_confidence = 0.0 } = req.body;
    if (!name) { res.status(400).json({ error: 'name is required' }); return; }

    const entities = await knowledgeGraph.findEntity(name, type);
    const filtered = entities.filter(e => e.confidence >= min_confidence);

    res.json({
      query: name,
      type: type || 'any',
      count: filtered.length,
      entities: filtered.slice(0, 50).map(e => ({
        id: e.id,
        name: e.name,
        type: e.type,
        confidence: e.confidence,
        call_count: e.call_count,
        properties: e.properties,
        sources: e.sources,
        first_seen: e.first_seen,
        last_seen: e.last_seen,
      })),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── ENRICH ───────────────────────────────────────────────────────────────────
// Everything the graph knows about a company or domain — entity + all relationships.
//
// Body: { identifier: string }

app.post('/enrich', async (req: Request, res: Response) => {
  try {
    const { identifier } = req.body;
    if (!identifier) { res.status(400).json({ error: 'identifier is required' }); return; }

    const result = await knowledgeGraph.enrich(identifier);

    if (!result.entity) {
      res.json({
        identifier,
        found: false,
        message: 'Not yet in graph. Feed data through find_leads, find_emails, or get_company_info first.',
      });
      return;
    }

    res.json({
      identifier,
      found: true,
      entity: {
        id: result.entity.id,
        name: result.entity.name,
        type: result.entity.type,
        confidence: result.entity.confidence,
        call_count: result.entity.call_count,
        first_seen: result.entity.first_seen,
        last_seen: result.entity.last_seen,
        properties: result.entity.properties,
        sources: result.entity.sources,
      },
      relationships: Object.fromEntries(
        Object.entries(result.related).map(([relation, nodes]) => [
          relation,
          nodes.map(n => ({ name: n.name, type: n.type, confidence: n.confidence })),
        ])
      ),
      confidence: result.confidence,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── CONNECTIONS ──────────────────────────────────────────────────────────────
// Find relationship path between two entities.
//
// Body: { from: string, to: string, max_hops?: number }

app.post('/connections', async (req: Request, res: Response) => {
  try {
    const { from, to, max_hops = 3 } = req.body;
    if (!from || !to) { res.status(400).json({ error: 'from and to are required' }); return; }

    const hops = Math.min(Math.max(1, max_hops), 5);
    const result = await knowledgeGraph.findConnections(from, to, hops);

    if (!result) {
      res.json({
        from, to,
        connected: false,
        message: `No connection found within ${hops} hops. One or both entities may not yet be in the graph.`,
      });
      return;
    }

    res.json({
      from, to,
      connected: true,
      hops: result.hops,
      path: result.path.map(n => ({ name: n.name, type: n.type })),
      relationships: result.edges.map(e => ({
        from: e.from_name,
        relation: e.relation,
        to: e.to_name,
        confidence: e.confidence,
      })),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── 2-HOP NEIGHBORS [cypher-003] ─────────────────────────────────────────────
// Get 2-hop neighborhood with parameterized relation type filters.
//
// Body: { entity: string, rel_types?: string[], direction?: 'out' | 'in' | 'both', limit?: number }

app.post('/neighbors_2hop', async (req: Request, res: Response) => {
  try {
    const { entity, rel_types = [], direction = 'out', limit = 50 } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    // Find the entity first
    const entities = await knowledgeGraph.findEntity(entity);
    if (!entities.length) {
      res.json({ entity, found: false, message: 'Entity not found in graph' });
      return;
    }

    const startNodeId = entities[0].id;
    const safeLimit = Math.min(Math.max(1, limit), 200);

    // Build relationship type filter
    const relFilter = rel_types.length > 0
      ? `:RELATES {relation: $rel_types}`
      : ':RELATES';

    // Build direction pattern
    let pattern: string;
    if (direction === 'in') {
      pattern = `(a:Entity {id: $startId})<-[e1${relFilter}]-(m:Entity)<-[e2${relFilter}]-(b:Entity)`;
    } else if (direction === 'both') {
      pattern = `(a:Entity {id: $startId})-[e1${relFilter}]-(m:Entity)-[e2${relFilter}]-(b:Entity)`;
    } else {
      pattern = `(a:Entity {id: $startId})-[e1${relFilter}]->(m:Entity)-[e2${relFilter}]->(b:Entity)`;
    }

    // Use direct Cypher query via knowledgeGraph's internal DB
    // Since we can't expose graphQuery directly, we use findConnections approach
    // For now, return neighbors via existing methods
    const hop1 = await knowledgeGraph.getNeighbours(startNodeId);
    const hop2Results: Array<{
      via: { name: string; type: string };
      target: { name: string; type: string };
      relations: [string, string];
      confidence: number;
    }> = [];

    for (const { neighbour, edge } of hop1) {
      // Filter by rel_types if specified
      if (rel_types.length > 0 && !rel_types.includes(edge.relation)) continue;

      const hop2Neighbors = await knowledgeGraph.getNeighbours(neighbour.id);
      for (const { neighbour: target, edge: e2 } of hop2Neighbors) {
        if (rel_types.length > 0 && !rel_types.includes(e2.relation)) continue;
        if (target.id === startNodeId) continue; // Avoid cycles back to start

        hop2Results.push({
          via: { name: neighbour.name, type: neighbour.type },
          target: { name: target.name, type: target.type },
          relations: [edge.relation, e2.relation],
          confidence: Math.min(edge.confidence, e2.confidence),
        });
      }
    }

    // Dedupe and sort by confidence
    const seen = new Set<string>();
    const deduped = hop2Results.filter(r => {
      const key = `${r.via.name}:${r.target.name}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).sort((a, b) => b.confidence - a.confidence).slice(0, safeLimit);

    res.json({
      entity: entities[0].name,
      entity_type: entities[0].type,
      rel_types: rel_types.length > 0 ? rel_types : 'all',
      direction,
      hop_2_count: deduped.length,
      neighbors: deduped,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── INDUSTRY + LOCATION SEARCH ───────────────────────────────────────────────
// Find companies by industry and optional location — answered from graph, no live API.
//
// Body: { industry: string, location?: string, min_confidence?: number }

app.post('/search', async (req: Request, res: Response) => {
  try {
    const { industry, location, min_confidence = 0.0 } = req.body;
    if (!industry) { res.status(400).json({ error: 'industry is required' }); return; }

    const companies = await knowledgeGraph.findByIndustryAndLocation(industry, location);
    const filtered = companies.filter(c => c.confidence >= min_confidence);

    res.json({
      industry,
      location: location || null,
      count: filtered.length,
      companies: filtered.slice(0, 100).map(c => ({
        name: c.name,
        confidence: c.confidence,
        call_count: c.call_count,
        properties: c.properties,
        last_seen: c.last_seen,
      })),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── DIRECT INJECTION ─────────────────────────────────────────────────────────
// For n8n workflows and external feeds to inject entities/connections directly.
//
// POST /ingest/entities - Add entities directly
// Body: { entities: Array<{ type, name, properties?, confidence?, source? }> }

app.post('/ingest/entities', async (req: Request, res: Response) => {
  try {
    const { entities } = req.body;
    if (!entities || !Array.isArray(entities)) {
      res.status(400).json({ error: 'entities array is required' });
      return;
    }

    const result = await knowledgeGraph.addEntities(entities);
    res.status(201).json({ success: true, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// POST /ingest/connections - Add connections directly
// Body: { connections: Array<{ from_type, from_name, to_type, to_name, relation, properties?, confidence?, source? }> }

app.post('/ingest/connections', async (req: Request, res: Response) => {
  try {
    const { connections } = req.body;
    if (!connections || !Array.isArray(connections)) {
      res.status(400).json({ error: 'connections array is required' });
      return;
    }

    const result = await knowledgeGraph.addConnections(connections);
    res.status(201).json({ success: true, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// POST /ingest/bulk - Bulk ingest entities AND connections in one call
// Body: { entities?: Array<...>, connections?: Array<...> }

app.post('/ingest/bulk', async (req: Request, res: Response) => {
  try {
    const { entities = [], connections = [] } = req.body;

    const entityResult = await knowledgeGraph.addEntities(entities);
    const connResult = await knowledgeGraph.addConnections(connections);

    res.status(201).json({
      success: true,
      entities: entityResult,
      connections: connResult,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── DEBUG (temporary) ─────────────────────────────────────────────────────────
// Returns raw FalkorDB response to debug parsing issues

app.get('/debug/raw', async (_req: Request, res: Response) => {
  try {
    const db = (knowledgeGraph as any).db;
    if (!db || !db.client) {
      res.json({ error: 'No client' });
      return;
    }

    // Get raw FalkorDB response for a simple query
    const result = await (db.client as any).sendCommand([
      'GRAPH.QUERY',
      db.graphName || 'forage_v1',
      'MATCH (n:Entity) RETURN n.type AS type, n.name AS name, n.id AS id LIMIT 5',
    ]);

    res.json({
      raw: result,
      headers: result?.[0],
      data: result?.[1],
      stats: result?.[2],
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message, stack: err.stack });
  }
});

// ─── STATS ────────────────────────────────────────────────────────────────────

app.get('/stats', async (_req: Request, res: Response) => {
  try {
    const stats = await knowledgeGraph.getStats();
    res.json({
      total_entities: stats.total_nodes,
      total_relationships: stats.total_edges,
      entities_by_type: stats.nodes_by_type,
      last_updated: stats.last_updated,
      status: stats.total_nodes > 0 ? 'active' : 'empty — grows with every Forage tool call',
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── METRICS [M1-obs-001] ───────────────────────────────────────────────────────
// Prometheus-compatible metrics endpoint for observability

const metrics = {
  requests: { total: 0, errors: 0, by_endpoint: {} as Record<string, number> },
  queries: { latency_ms_sum: 0, latency_ms_count: 0, latencies: [] as number[] },
  ingestion: { entities_added: 0, relationships_added: 0, last_batch: null as string | null },
};

app.get('/metrics', async (_req: Request, res: Response) => {
  const stats = await knowledgeGraph.getStats().catch(() => ({ total_nodes: 0, total_edges: 0 }));
  
  const avgLatency = metrics.queries.latency_ms_count > 0 
    ? metrics.queries.latency_ms_sum / metrics.queries.latency_ms_count 
    : 0;
  
  res.set('Content-Type', 'text/plain');
  res.send(`# HELP graph_entities_total Total number of entities in graph
# TYPE graph_entities_total gauge
graph_entities_total ${stats.total_nodes}

# HELP graph_relationships_total Total number of relationships in graph
# TYPE graph_relationships_total gauge
graph_relationships_total ${stats.total_edges}

# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total ${metrics.requests.total}

# HELP http_requests_errors_total Total HTTP errors
# TYPE http_requests_errors_total counter
http_requests_errors_total ${metrics.requests.errors}

# HELP query_latency_avg_ms Average query latency in milliseconds
# TYPE query_latency_avg_ms gauge
query_latency_avg_ms ${avgLatency.toFixed(2)}

# HELP ingestion_entities_total Total entities ingested since start
# TYPE ingestion_entities_total counter
ingestion_entities_total ${metrics.ingestion.entities_added}

# HELP ingestion_relationships_total Total relationships ingested since start
# TYPE ingestion_relationships_total counter
ingestion_relationships_total ${metrics.ingestion.relationships_added}
`);
});

// Record metrics helper
function recordMetric(type: 'request' | 'error' | 'query_latency' | 'ingestion', data?: any) {
  if (type === 'request') {
    metrics.requests.total++;
  } else if (type === 'error') {
    metrics.requests.errors++;
  } else if (type === 'query_latency' && data?.ms) {
    metrics.queries.latency_ms_sum += data.ms;
    metrics.queries.latency_ms_count++;
    metrics.queries.latencies.push(data.ms);
    if (metrics.queries.latencies.length > 100) metrics.queries.latencies.shift();
  } else if (type === 'ingestion' && data) {
    metrics.ingestion.entities_added += data.entities || 0;
    metrics.ingestion.relationships_added += data.relationships || 0;
    metrics.ingestion.last_batch = new Date().toISOString();
  }
}

// ─── CLAIMS ───────────────────────────────────────────────────────────────────
// Add a provenance claim for an entity.
//
// Body: { entity: string, relation: string, target: string, assertion: string, source_url?: string, confidence?: number }

app.post('/claim', async (req: Request, res: Response) => {
  try {
    const { entity, relation, target, assertion, source_url, confidence = 0.8 } = req.body;
    if (!entity || !relation || !target || !assertion) {
      res.status(400).json({ error: 'entity, relation, target, and assertion are required' });
      return;
    }

    const claim = await knowledgeGraph.addClaim({
      entity,
      relation,
      target,
      assertion,
      source_url,
      confidence,
    });

    res.status(201).json({ success: true, claim });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get all claims for an entity.
//
// GET /claims/:entityName

app.get('/claims/:entityName', async (req: Request, res: Response) => {
  try {
    const { entityName } = req.params;
    const claims = await knowledgeGraph.getClaims(entityName);
    res.json({
      entity: entityName,
      count: claims.length,
      claims,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── REGIME ─────────────────────────────────────────────────────────────────
// Set regime label on an entity.
//
// Body: { entity: string, regime: 'normal' | 'stressed' | 'pre_tipping' | 'post_event' }

app.post('/regime', async (req: Request, res: Response) => {
  try {
    const { entity, regime } = req.body;
    if (!entity || !regime) {
      res.status(400).json({ error: 'entity and regime are required' });
      return;
    }

    const validRegimes = ['normal', 'stressed', 'pre_tipping', 'post_event'];
    if (!validRegimes.includes(regime)) {
      res.status(400).json({ error: 'regime must be one of: normal, stressed, pre_tipping, post_event' });
      return;
    }

    const success = await knowledgeGraph.setRegime(entity, regime);
    res.json({ entity, regime, updated: success });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── SIGNALS (TIME-SERIES) ────────────────────────────────────────────────
// Add time-series signal data.
//
// Body: { entity: string, metric: string, value: number, timestamp?: number }

app.post('/signal', async (req: Request, res: Response) => {
  try {
    const { entity, metric, value, timestamp } = req.body;
    if (!entity || !metric || value === undefined) {
      res.status(400).json({ error: 'entity, metric, and value are required' });
      return;
    }

    await knowledgeGraph.addSignal({
      entity,
      metric,
      value,
      timestamp: timestamp || Date.now(),
    });

    res.status(202).json({ accepted: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get signals for an entity.
//
// GET /signals/:entityName?metric=xxx&limit=100

app.get('/signals/:entityName', async (req: Request, res: Response) => {
  try {
    const { entityName } = req.params;
    const { metric, limit = '100' } = req.query;
    const signals = await knowledgeGraph.getSignals(
      entityName, 
      metric as string | undefined, 
      parseInt(limit as string)
    );
    res.json({
      entity: entityName,
      metric: metric || null,
      count: signals.length,
      signals,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── CAUSAL QUERIES ──────────────────────────────────────────────────────
// Get entities that drive this entity (causal parents).
//
// Body: { entity: string, limit?: number }

app.post('/causal_parents', async (req: Request, res: Response) => {
  try {
    const { entity, limit = 10 } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    const result = await knowledgeGraph.getCausalParents(entity, limit);
    res.json({ entity, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Get entities that this entity drives (causal children).
//
// Body: { entity: string, limit?: number }

app.post('/causal_children', async (req: Request, res: Response) => {
  try {
    const { entity, limit = 10 } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    const result = await knowledgeGraph.getCausalChildren(entity, limit);
    res.json({ entity, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Find highest causal-weight path between two entities.
//
// Body: { from: string, to: string }

app.post('/causal_path', async (req: Request, res: Response) => {
  try {
    const { from, to } = req.body;
    if (!from || !to) { res.status(400).json({ error: 'from and to are required' }); return; }

    const result = await knowledgeGraph.getCausalPath(from, to);
    if (!result) {
      res.json({ from, to, found: false, message: 'No path found between entities' });
      return;
    }
    res.json({ from, to, found: true, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── SIMULATE ─────────────────────────────────────────────────────────────
// Propagate a shock/boost/remove intervention through the graph.
//
// Body: { entity: string, intervention: 'shock' | 'boost' | 'remove', depth?: number }

app.post('/simulate', async (req: Request, res: Response) => {
  try {
    const { entity, intervention, depth = 3 } = req.body;
    if (!entity || !intervention) {
      res.status(400).json({ error: 'entity and intervention are required' });
      return;
    }

    const validInterventions = ['shock', 'boost', 'remove'];
    if (!validInterventions.includes(intervention)) {
      res.status(400).json({ error: 'intervention must be one of: shock, boost, remove' });
      return;
    }

    const result = await knowledgeGraph.simulate(entity, intervention, depth);
    res.json({ entity, intervention, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── LINK PREDICTION ──────────────────────────────────────────────────────────
// Predict potential links using Adamic-Adar similarity.
//
// $$AA(i,j) = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$$
//
// Body: { entity: string, target_type?: string, max_predictions?: number }

app.post('/predict_links', async (req: Request, res: Response) => {
  try {
    const { entity, target_type, max_predictions = 10 } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    const predictions = await knowledgeGraph.predictLinks(entity, target_type, max_predictions);
    
    res.json({
      entity,
      target_type: target_type || null,
      predictions: predictions.map(p => ({
        target: p.target,
        score: p.score,
        algorithm: p.algorithm,
        common_neighbors: p.common_neighbors,
        confidence: p.confidence,
      })),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── HAWKES CONTAGION ────────────────────────────────────────────────────────
// Get current contagion state for an entity using Hawkes Process.
//
// Intensity function: $\lambda(t) = \mu + \sum_{t_i < t} \phi(t - t_i)$
// Branching ratio: $R_j = \sum_{i \neq j} \alpha_{ij}$
//
// Body: { entity: string }

app.post('/contagion', async (req: Request, res: Response) => {
  try {
    const { entity } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    const state = await knowledgeGraph.getContagionState(entity);
    if (!state) {
      res.json({ entity, found: false, message: 'Entity not found or no contagion data' });
      return;
    }

    res.json({
      entity: state.entityId,
      entity_type: state.entityType,
      intensity: state.intensity,
      branching_ratio: state.branching_ratio,
      is_tipping: state.is_tipping,
      drivers: state.drivers,
      interpretation: state.branching_ratio > 1 
        ? 'Super-critical: Self-sustaining cascade likely'
        : state.branching_ratio > 0.5
        ? 'Critical: Cascade possible with trigger'
        : 'Sub-critical: Cascade unlikely',
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── SHOCK SIMULATION (HAWKES) ───────────────────────────────────────────────
// Simulate shock propagation using Hawkes Process.
//
// Forward simulation of self-exciting point process:
// $$\lambda_j(t) = \mu_j + \sum_{i} \sum_{t_i^k < t} \alpha_{ij} \beta e^{-\beta(t - t_i^k)}$$
//
// Body: { entity: string, magnitude?: number, duration_hours?: number }

app.post('/simulate_shock', async (req: Request, res: Response) => {
  try {
    const { entity, magnitude = 1.0, duration_hours = 168 } = req.body;
    if (!entity) { res.status(400).json({ error: 'entity is required' }); return; }

    const result = await knowledgeGraph.simulateShock(entity, magnitude, duration_hours);
    if (!result) {
      res.json({ entity, found: false, message: 'Entity not found' });
      return;
    }

    res.json({
      source: result.sourceEntity,
      shock_magnitude: result.shockMagnitude,
      cascade: result.cascade.map(c => ({
        entity: c.entityId,
        entity_type: c.entityType,
        peak_time_hours: c.peak_time_hours,
        peak_intensity: c.peak_intensity,
        total_impact: c.total_impact,
      })),
      summary: result.summary,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── RECORD HAWKES EVENT ─────────────────────────────────────────────────────
// Record an event for Hawkes Process modeling.
//
// Body: { entity: string, entity_type: string, intensity?: number, lat?: number, lon?: number }

app.post('/event', async (req: Request, res: Response) => {
  try {
    const { entity, entity_type, intensity = 1.0, lat, lon } = req.body;
    if (!entity || !entity_type) {
      res.status(400).json({ error: 'entity and entity_type are required' });
      return;
    }

    await knowledgeGraph.recordEvent({
      entityId: entity,
      entityType: entity_type,
      timestamp: Date.now(),
      intensity,
      lat,
      lon,
    });

    res.status(202).json({ accepted: true });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── RECALIBRATE HAWKES ──────────────────────────────────────────────────────
// Recalibrate Hawkes Process parameters using MLE.
//
// $$\hat{\mu} = \frac{N}{T}, \quad \hat{\alpha}_{ij} = \frac{\text{co-occurrences}}{\text{total}}$$
//
// Body: { window_hours?: number }

app.post('/recalibrate', async (req: Request, res: Response) => {
  try {
    const { window_hours = 720 } = req.body;
    await knowledgeGraph.recalibrateHawkes(window_hours);
    res.json({ success: true, message: 'Hawkes process recalibrated' });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── BACKUP / EXPORT [backup-002] ─────────────────────────────────────────────
// Full graph export for disaster recovery

app.get('/export', async (_req: Request, res: Response) => {
  try {
    const backup = await knowledgeGraph.exportAll();
    res.setHeader('Content-Disposition', `attachment; filename="forage-graph-backup-${new Date().toISOString().split('T')[0]}.json"`);
    res.json(backup);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/import', async (req: Request, res: Response) => {
  try {
    const backup = req.body;
    if (!backup || !backup.entities) {
      res.status(400).json({ error: 'Invalid backup format. Must include entities array.' });
      return;
    }
    const result = await knowledgeGraph.importBackup(backup);
    res.json({ success: true, ...result });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Ingest raw batch - for n8n workflows that prepare structured entity batches
app.post('/ingest_raw_batch', async (req: Request, res: Response) => {
  try {
    const { batch } = req.body;
    if (!batch || !Array.isArray(batch)) {
      res.status(400).json({ error: 'batch array is required' });
      return;
    }

    let entities_added = 0;
    let relationships_added = 0;

    for (const item of batch) {
      // Add entity
      if (item.name && item.type) {
        await knowledgeGraph.addEntities([{
          type: item.type,
          name: item.name,
          properties: item.properties || {},
          confidence: item.confidence || 0.9,
          source: item.source || 'raw_batch'
        }]);
        entities_added++;
      }

      // Add relationships
      if (item.relationships && Array.isArray(item.relationships)) {
        for (const rel of item.relationships) {
          await knowledgeGraph.addConnections([{
            from_type: item.type,
            from_name: item.name,
            to_type: rel.targetType,
            to_name: rel.targetName,
            relation: rel.relation,
            confidence: rel.confidence || 0.8,
            source: item.source || 'raw_batch'
          }]);
          relationships_added++;
        }
      }
    }

    res.status(201).json({ success: true, entities_added, relationships_added });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ─── START ────────────────────────────────────────────────────────────────────

async function start() {
  await knowledgeGraph.init();

  app.listen(PORT, () => {
    console.log(`Forage Reality Graph API running on port ${PORT}`);
    console.log(`Health: http://localhost:${PORT}/health`);
    console.log(`Features: FIBO schema, ULEM dual-hash, Hawkes contagion, Adamic-Adar prediction`);
  });
}

start().catch(err => {
  console.error('Failed to start:', err);
  process.exit(1);
});
