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
import express from 'express';
import cors from 'cors';
import { knowledgeGraph } from './knowledge-graph.js';
import { createCausalEngine } from './causal-inference.js';
const app = express();
const PORT = process.env.PORT || 3000;
const SECRET = process.env.GRAPH_API_SECRET;
const latencyHistogram = new Map();
const requestCounts = new Map();
const errorCounts = new Map();
function initHistogram() {
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
function recordLatency(endpoint, durationMs) {
    if (!latencyHistogram.has(endpoint)) {
        latencyHistogram.set(endpoint, initHistogram());
    }
    const buckets = latencyHistogram.get(endpoint);
    for (const bucket of buckets) {
        if (durationMs <= bucket.le) {
            bucket.count++;
            break;
        }
    }
    requestCounts.set(endpoint, (requestCounts.get(endpoint) || 0) + 1);
}
function recordError(endpoint) {
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
app.use((req, res, next) => {
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
function requireAuth(req, res, next) {
    if (req.path === '/health') {
        next();
        return;
    }
    const auth = req.headers.authorization;
    if (!auth || !auth.startsWith('Bearer ') || auth.slice(7) !== SECRET) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }
    next();
}
app.use(requireAuth);
// ─── HEALTH ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
    res.status(200).json({
        status: 'ok',
        ts: new Date().toISOString(),
    });
});
app.get('/health/detail', async (_req, res) => {
    const healthy = await knowledgeGraph.isHealthy();
    res.status(healthy ? 200 : 503).json({
        status: healthy ? 'ok' : 'degraded',
        graph: healthy ? 'connected' : 'disconnected',
        ts: new Date().toISOString(),
    });
});
// ─── METRICS [obs-003] ───────────────────────────────────────────────────────
// Prometheus-compatible metrics endpoint with node/edge counts + latency histogram
app.get('/metrics', async (_req, res) => {
    try {
        const stats = await knowledgeGraph.getStats();
        // Build Prometheus-format output
        const lines = [];
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── INGEST ───────────────────────────────────────────────────────────────────
// Called by Apify actor after every tool response.
// Now waits for DB write to complete and reports actual status.
//
// Body: { tool_name: string, result: any }
app.post('/ingest', async (req, res) => {
    const { tool_name, result } = req.body;
    if (!tool_name || result === undefined) {
        res.status(400).json({ error: 'tool_name and result are required' });
        return;
    }
    try {
        // Wait for actual DB write to complete
        await knowledgeGraph.ingest(tool_name, result);
        res.status(202).json({ accepted: true, processed: true });
    }
    catch (err) {
        console.error('[INGEST ERROR]', err.message);
        res.status(500).json({ accepted: false, error: err.message });
    }
});
// ─── QUERY ────────────────────────────────────────────────────────────────────
// Find entities by name, optionally filtered by type.
//
// Body: { name: string, type?: EntityType, min_confidence?: number }
app.post('/query', async (req, res) => {
    if (!knowledgeGraph.isReady()) {
        res.status(503).json({ error: 'Graph database not ready — retry shortly' });
        return;
    }
    try {
        const { name, type, min_confidence = 0.0, limit = 50 } = req.body;
        let entities;
        if (name) {
            // Named lookup (existing behaviour)
            entities = await knowledgeGraph.findEntity(name, type, limit);
        }
        else if (type) {
            // Type-only listing — used by Oracle and agent collectors
            entities = await knowledgeGraph.findEntity('', type, limit);
        }
        else {
            res.status(400).json({ error: 'name or type is required' });
            return;
        }
        const filtered = entities.filter((e) => e.confidence >= min_confidence);
        const results = filtered.slice(0, Math.min(limit, 200)).map((e) => ({
            id: e.id,
            name: e.name,
            type: e.type,
            confidence: e.confidence,
            call_count: e.call_count,
            properties: e.properties,
            sources: e.sources,
            first_seen: e.first_seen,
            last_seen: e.last_seen,
        }));
        res.json({
            query: name || '',
            type: type || 'any',
            count: results.length,
            entities: results,
            nodes: results, // alias for Oracle / agent compatibility
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── ENRICH ───────────────────────────────────────────────────────────────────
// Everything the graph knows about a company or domain — entity + all relationships.
//
// Body: { identifier: string }
app.post('/enrich', async (req, res) => {
    try {
        const { identifier } = req.body;
        if (!identifier) {
            res.status(400).json({ error: 'identifier is required' });
            return;
        }
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
            relationships: Object.fromEntries(Object.entries(result.related).map(([relation, nodes]) => [
                relation,
                nodes.map(n => ({ name: n.name, type: n.type, confidence: n.confidence })),
            ])),
            confidence: result.confidence,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── CONNECTIONS ──────────────────────────────────────────────────────────────
// Find relationship path between two entities.
//
// Body: { from: string, to: string, max_hops?: number }
app.post('/connections', async (req, res) => {
    try {
        const { from, to, max_hops = 3 } = req.body;
        if (!from || !to) {
            res.status(400).json({ error: 'from and to are required' });
            return;
        }
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── 2-HOP NEIGHBORS [cypher-003] ─────────────────────────────────────────────
// Get 2-hop neighborhood with parameterized relation type filters.
//
// Body: { entity: string, rel_types?: string[], direction?: 'out' | 'in' | 'both', limit?: number }
app.post('/neighbors_2hop', async (req, res) => {
    try {
        const { entity, rel_types = [], direction = 'out', limit = 50 } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
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
        let pattern;
        if (direction === 'in') {
            pattern = `(a:Entity {id: $startId})<-[e1${relFilter}]-(m:Entity)<-[e2${relFilter}]-(b:Entity)`;
        }
        else if (direction === 'both') {
            pattern = `(a:Entity {id: $startId})-[e1${relFilter}]-(m:Entity)-[e2${relFilter}]-(b:Entity)`;
        }
        else {
            pattern = `(a:Entity {id: $startId})-[e1${relFilter}]->(m:Entity)-[e2${relFilter}]->(b:Entity)`;
        }
        // Use direct Cypher query via knowledgeGraph's internal DB
        // Since we can't expose graphQuery directly, we use findConnections approach
        // For now, return neighbors via existing methods
        const hop1 = await knowledgeGraph.getNeighbours(startNodeId);
        const hop2Results = [];
        for (const { neighbour, edge } of hop1) {
            // Filter by rel_types if specified
            if (rel_types.length > 0 && !rel_types.includes(edge.relation))
                continue;
            const hop2Neighbors = await knowledgeGraph.getNeighbours(neighbour.id);
            for (const { neighbour: target, edge: e2 } of hop2Neighbors) {
                if (rel_types.length > 0 && !rel_types.includes(e2.relation))
                    continue;
                if (target.id === startNodeId)
                    continue; // Avoid cycles back to start
                hop2Results.push({
                    via: { name: neighbour.name, type: neighbour.type },
                    target: { name: target.name, type: target.type },
                    relations: [edge.relation, e2.relation],
                    confidence: Math.min(edge.confidence, e2.confidence),
                });
            }
        }
        // Dedupe and sort by confidence
        const seen = new Set();
        const deduped = hop2Results.filter(r => {
            const key = `${r.via.name}:${r.target.name}`;
            if (seen.has(key))
                return false;
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── INDUSTRY + LOCATION SEARCH ───────────────────────────────────────────────
// Find companies by industry and optional location — answered from graph, no live API.
//
// Body: { industry: string, location?: string, min_confidence?: number }
app.post('/search', async (req, res) => {
    try {
        const { industry, location, min_confidence = 0.0 } = req.body;
        if (!industry) {
            res.status(400).json({ error: 'industry is required' });
            return;
        }
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── DIRECT INJECTION ─────────────────────────────────────────────────────────
// For n8n workflows and external feeds to inject entities/connections directly.
//
// POST /ingest/entities - Add entities directly
// Body: { entities: Array<{ type, name, properties?, confidence?, source? }> }
app.post('/ingest/entities', async (req, res) => {
    try {
        const { entities } = req.body;
        if (!entities || !Array.isArray(entities)) {
            res.status(400).json({ error: 'entities array is required' });
            return;
        }
        const result = await knowledgeGraph.addEntities(entities);
        res.status(201).json({ success: true, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// POST /ingest/connections - Add connections directly
// Body: { connections: Array<{ from_type, from_name, to_type, to_name, relation, properties?, confidence?, source? }> }
app.post('/ingest/connections', async (req, res) => {
    try {
        const { connections } = req.body;
        if (!connections || !Array.isArray(connections)) {
            res.status(400).json({ error: 'connections array is required' });
            return;
        }
        const result = await knowledgeGraph.addConnections(connections);
        res.status(201).json({ success: true, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// POST /ingest/bulk - Bulk ingest entities AND connections in one call
// Body: { entities?: Array<...>, connections?: Array<...> }
app.post('/ingest/bulk', async (req, res) => {
    if (!knowledgeGraph.isReady()) {
        res.status(503).json({ error: 'Graph database not ready — retry shortly' });
        return;
    }
    try {
        // Accept both 'nodes' (agent/collector convention) and 'entities' (internal convention)
        const { entities, nodes, connections = [] } = req.body;
        const rawList = entities ?? nodes ?? [];
        // Normalize flat agent node format into addEntities shape.
        // Agents send: { id, type, name, source, confidence, venue, yes_price, ... }
        // addEntities expects: { type, name, source, confidence, properties }
        // Unknown top-level fields are folded into properties so no data is lost.
        const KNOWN_FIELDS = new Set(['type', 'name', 'source', 'confidence', 'properties']);
        const entityList = rawList.map((n) => {
            const { type, name, source, confidence, properties: existingProps, ...rest } = n;
            // Remove internal/index fields that shouldn't go into properties
            delete rest.id;
            delete rest._force_new;
            return {
                type,
                name,
                source,
                confidence,
                properties: { ...existingProps, ...rest },
            };
        });
        const entityResult = await knowledgeGraph.addEntities(entityList);
        const connResult = await knowledgeGraph.addConnections(connections);
        res.status(201).json({
            success: true,
            entities: entityResult,
            connections: connResult,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── DEBUG (temporary) ─────────────────────────────────────────────────────────
// Returns raw FalkorDB response to debug parsing issues
app.get('/debug/raw', async (_req, res) => {
    try {
        const db = knowledgeGraph.db;
        if (!db || !db.client) {
            res.json({ error: 'No client' });
            return;
        }
        // Get raw FalkorDB response for a simple query
        const result = await db.client.sendCommand([
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
    }
    catch (err) {
        res.status(500).json({ error: err.message, stack: err.stack });
    }
});
// ─── STATS ────────────────────────────────────────────────────────────────────
app.get('/stats', async (_req, res) => {
    try {
        const stats = await knowledgeGraph.getStats();
        res.json({
            total_entities: stats.total_nodes,
            total_relationships: stats.total_edges,
            entities_by_type: stats.nodes_by_type,
            last_updated: stats.last_updated,
            status: stats.total_nodes > 0 ? 'active' : 'empty — grows with every Forage tool call',
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── METRICS [M1-obs-001] ───────────────────────────────────────────────────────
// Prometheus-compatible metrics endpoint for observability
const metrics = {
    requests: { total: 0, errors: 0, by_endpoint: {} },
    queries: { latency_ms_sum: 0, latency_ms_count: 0, latencies: [] },
    ingestion: { entities_added: 0, relationships_added: 0, last_batch: null },
};
app.get('/metrics', async (_req, res) => {
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
function recordMetric(type, data) {
    if (type === 'request') {
        metrics.requests.total++;
    }
    else if (type === 'error') {
        metrics.requests.errors++;
    }
    else if (type === 'query_latency' && data?.ms) {
        metrics.queries.latency_ms_sum += data.ms;
        metrics.queries.latency_ms_count++;
        metrics.queries.latencies.push(data.ms);
        if (metrics.queries.latencies.length > 100)
            metrics.queries.latencies.shift();
    }
    else if (type === 'ingestion' && data) {
        metrics.ingestion.entities_added += data.entities || 0;
        metrics.ingestion.relationships_added += data.relationships || 0;
        metrics.ingestion.last_batch = new Date().toISOString();
    }
}
// ─── CLAIMS ───────────────────────────────────────────────────────────────────
// Add a provenance claim for an entity.
//
// Body: { entity: string, relation: string, target: string, assertion: string, source_url?: string, confidence?: number }
app.post('/claim', async (req, res) => {
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// Get all claims for an entity.
//
// GET /claims/:entityName
app.get('/claims/:entityName', async (req, res) => {
    try {
        const { entityName } = req.params;
        const claims = await knowledgeGraph.getClaims(entityName);
        res.json({
            entity: entityName,
            count: claims.length,
            claims,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── REGIME ─────────────────────────────────────────────────────────────────
// Set regime label on an entity.
//
// Body: { entity: string, regime: 'normal' | 'stressed' | 'pre_tipping' | 'post_event' }
app.post('/regime', async (req, res) => {
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── SIGNALS (TIME-SERIES) ────────────────────────────────────────────────
// Add time-series signal data.
//
// Body: { entity: string, metric: string, value: number, timestamp?: number }
app.post('/signal', async (req, res) => {
    if (!knowledgeGraph.isReady()) {
        res.status(503).json({ error: 'Graph database not ready — retry shortly' });
        return;
    }
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// Get signals for an entity.
//
// GET /signals/:entityName?metric=xxx&limit=100
app.get('/signals/:entityName', async (req, res) => {
    try {
        const { entityName } = req.params;
        const { metric, limit = '100' } = req.query;
        const signals = await knowledgeGraph.getSignals(entityName, metric, parseInt(limit));
        res.json({
            entity: entityName,
            metric: metric || null,
            count: signals.length,
            signals,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── CAUSAL QUERIES ──────────────────────────────────────────────────────
// Get entities that drive this entity (causal parents).
//
// Body: { entity: string, limit?: number }
app.post('/causal_parents', async (req, res) => {
    try {
        const { entity, limit = 10 } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
        const result = await knowledgeGraph.getCausalParents(entity, limit);
        res.json({ entity, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// Get entities that this entity drives (causal children).
//
// Body: { entity: string, limit?: number }
app.post('/causal_children', async (req, res) => {
    try {
        const { entity, limit = 10 } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
        const result = await knowledgeGraph.getCausalChildren(entity, limit);
        res.json({ entity, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// Find highest causal-weight path between two entities.
//
// Body: { from: string, to: string }
app.post('/causal_path', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (!from || !to) {
            res.status(400).json({ error: 'from and to are required' });
            return;
        }
        const result = await knowledgeGraph.getCausalPath(from, to);
        if (!result) {
            res.json({ from, to, found: false, message: 'No path found between entities' });
            return;
        }
        res.json({ from, to, found: true, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── SIMULATE ─────────────────────────────────────────────────────────────
// Propagate a shock/boost/remove intervention through the graph.
//
// Body: { entity: string, intervention: 'shock' | 'boost' | 'remove', depth?: number }
app.post('/simulate', async (req, res) => {
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── LINK PREDICTION ──────────────────────────────────────────────────────────
// Predict potential links using Adamic-Adar similarity.
//
// $$AA(i,j) = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$$
//
// Body: { entity: string, target_type?: string, max_predictions?: number }
app.post('/predict_links', async (req, res) => {
    try {
        const { entity, target_type, max_predictions = 10 } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
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
    }
    catch (err) {
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
app.post('/contagion', async (req, res) => {
    try {
        const { entity } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
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
    }
    catch (err) {
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
app.post('/simulate_shock', async (req, res) => {
    try {
        const { entity, magnitude = 1.0, duration_hours = 168 } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── RECORD HAWKES EVENT ─────────────────────────────────────────────────────
// Record an event for Hawkes Process modeling.
//
// Body: { entity: string, entity_type: string, intensity?: number, lat?: number, lon?: number }
app.post('/event', async (req, res) => {
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── RECALIBRATE HAWKES ──────────────────────────────────────────────────────
// Recalibrate Hawkes Process parameters using MLE.
//
// $$\hat{\mu} = \frac{N}{T}, \quad \hat{\alpha}_{ij} = \frac{\text{co-occurrences}}{\text{total}}$$
//
// Body: { window_hours?: number }
app.post('/recalibrate', async (req, res) => {
    try {
        const { window_hours = 720 } = req.body;
        await knowledgeGraph.recalibrateHawkes(window_hours);
        res.json({ success: true, message: 'Hawkes process recalibrated' });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── BACKUP / EXPORT [backup-002] ─────────────────────────────────────────────
// Full graph export for disaster recovery
app.get('/export', async (_req, res) => {
    try {
        const backup = await knowledgeGraph.exportAll();
        res.setHeader('Content-Disposition', `attachment; filename="forage-graph-backup-${new Date().toISOString().split('T')[0]}.json"`);
        res.json(backup);
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
app.post('/import', async (req, res) => {
    try {
        const backup = req.body;
        if (!backup || !backup.entities) {
            res.status(400).json({ error: 'Invalid backup format. Must include entities array.' });
            return;
        }
        const result = await knowledgeGraph.importBackup(backup);
        res.json({ success: true, ...result });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// Ingest raw batch - for n8n workflows that prepare structured entity batches
app.post('/ingest_raw_batch', async (req, res) => {
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
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ═══════════════════════════════════════════════════════════════════════════════
// CRASH CAUSAL INTELLIGENCE API [crash-api-001]
// Higher-level endpoints for answering "what caused the crash and why?"
// ═══════════════════════════════════════════════════════════════════════════════
// Initialize causal engine lazily
let causalEngine = null;
async function getCausalEngine() {
    if (causalEngine)
        return causalEngine;
    // Get Redis client from knowledge graph's internal DB
    const db = knowledgeGraph.db;
    if (!db || !db.getClient())
        return null;
    causalEngine = createCausalEngine(db.getClient(), db.getGraphName());
    return causalEngine;
}
// ─── /crash/explain ──────────────────────────────────────────────────────────
// Explain a crash: trace causal paths, identify root causes, summarize
//
// Body: { crash: string }
// Returns: Full crash explanation with causal paths, mechanisms, affected entities
app.post('/crash/explain', async (req, res) => {
    try {
        const { crash } = req.body;
        if (!crash) {
            res.status(400).json({ error: 'crash name is required' });
            return;
        }
        const engine = await getCausalEngine();
        if (!engine) {
            res.status(503).json({ error: 'Causal engine not available' });
            return;
        }
        const explanation = await engine.explainCrash(crash);
        if (!explanation) {
            res.json({
                crash,
                found: false,
                message: 'Crash not found in graph. Load crash data first with crash_loader.py',
            });
            return;
        }
        res.json({
            crash: explanation.crash.name,
            found: true,
            peak_drawdown_pct: explanation.crash.peak_drawdown_pct,
            date_range: {
                start: explanation.crash.start_date,
                peak: explanation.crash.peak_date,
                end: explanation.crash.end_date,
            },
            root_causes: explanation.root_causes.slice(0, 5).map(c => ({
                name: c.name,
                causal_weight: c.causal_weight,
                mechanism: c.mechanism,
                confidence: c.confidence,
            })),
            transmission_mechanisms: explanation.transmission_mechanisms,
            causal_paths: explanation.causal_paths.slice(0, 5).map(p => ({
                nodes: p.nodes,
                total_weight: p.total_weight,
                narrative: p.narrative,
            })),
            most_affected: explanation.most_affected.slice(0, 10),
            summary: explanation.summary,
            detailed_narrative: explanation.detailed_narrative,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /crash/counterfactual ───────────────────────────────────────────────────
// Simulate a counterfactual: "What if X didn't happen?"
//
// Body: {
//   crash: string,
//   intervention: {
//     type: 'remove_factor' | 'early_intervention' | 'limit_exposure' | 'circuit_breaker',
//     target_entity: string,
//     intervention_date?: string,
//     parameters?: object
//   }
// }
app.post('/crash/counterfactual', async (req, res) => {
    try {
        const { crash, intervention } = req.body;
        if (!crash || !intervention) {
            res.status(400).json({ error: 'crash and intervention are required' });
            return;
        }
        if (!intervention.type || !intervention.target_entity) {
            res.status(400).json({ error: 'intervention must include type and target_entity' });
            return;
        }
        const engine = await getCausalEngine();
        if (!engine) {
            res.status(503).json({ error: 'Causal engine not available' });
            return;
        }
        const scenario = await engine.simulateCounterfactual(crash, {
            type: intervention.type,
            target_entity: intervention.target_entity,
            intervention_date: intervention.intervention_date || new Date().toISOString(),
            parameters: intervention.parameters || {},
        });
        if (!scenario) {
            res.json({
                crash,
                found: false,
                message: 'Crash not found in graph',
            });
            return;
        }
        res.json({
            crash,
            scenario_id: scenario.scenario_id,
            intervention: scenario.intervention,
            simulated_drawdown_pct: scenario.simulated_drawdown_pct,
            entities_saved: scenario.entities_saved,
            cascade_prevented: scenario.cascade_prevented,
            confidence: scenario.confidence,
            interpretation: scenario.cascade_prevented
                ? `Intervention "${intervention.type}" on ${intervention.target_entity} would likely have prevented the cascade`
                : `Intervention would have reduced drawdown from baseline to ${scenario.simulated_drawdown_pct}%`,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /crash/add ──────────────────────────────────────────────────────────────
// Add a crash event to the graph (for programmatic ingestion)
//
// Body: CrashEvent object
app.post('/crash/add', async (req, res) => {
    try {
        const crash = req.body;
        if (!crash.crash_id || !crash.name || !crash.crash_type) {
            res.status(400).json({ error: 'crash_id, name, and crash_type are required' });
            return;
        }
        const engine = await getCausalEngine();
        if (!engine) {
            res.status(503).json({ error: 'Causal engine not available' });
            return;
        }
        await engine.addCrashEvent(crash);
        res.status(201).json({
            success: true,
            crash_id: crash.crash_id,
            name: crash.name,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /crash/link_factor ──────────────────────────────────────────────────────
// Link a causal factor to a crash
//
// Body: {
//   crash_id: string,
//   factor: CausalFactor,
//   relation?: 'TRIGGERED' | 'SHOCKED' | 'AMPLIFIED_CRASH'
// }
app.post('/crash/link_factor', async (req, res) => {
    try {
        const { crash_id, factor, relation = 'TRIGGERED' } = req.body;
        if (!crash_id || !factor) {
            res.status(400).json({ error: 'crash_id and factor are required' });
            return;
        }
        if (!factor.factor_id || !factor.name || !factor.type) {
            res.status(400).json({ error: 'factor must include factor_id, name, and type' });
            return;
        }
        const engine = await getCausalEngine();
        if (!engine) {
            res.status(503).json({ error: 'Causal engine not available' });
            return;
        }
        await engine.linkCausalFactor(crash_id, factor, relation);
        res.status(201).json({
            success: true,
            crash_id,
            factor: factor.name,
            relation,
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /regime/now ─────────────────────────────────────────────────────────────
// Get current global regime summary
//
// Returns: Current regime classification with confidence
app.get('/regime/now', async (_req, res) => {
    try {
        // Query for recent regime-tagged entities
        const db = knowledgeGraph.db;
        if (!db) {
            res.status(503).json({ error: 'Database not available' });
            return;
        }
        // Get distribution of current regimes
        const regimeQuery = `
      MATCH (n:Entity)
      WHERE n.regime IS NOT NULL
      RETURN n.regime AS regime, count(n) AS count
      ORDER BY count DESC
    `;
        const result = await db.graphQuery(regimeQuery, {});
        // Compute dominant regime
        let totalCount = 0;
        const regimeCounts = {};
        for (const row of result) {
            const regime = row.regime || 'normal';
            const count = parseInt(row.count) || 0;
            regimeCounts[regime] = count;
            totalCount += count;
        }
        // Determine dominant regime
        let dominantRegime = 'normal';
        let maxCount = 0;
        for (const [regime, count] of Object.entries(regimeCounts)) {
            if (count > maxCount) {
                maxCount = count;
                dominantRegime = regime;
            }
        }
        const confidence = totalCount > 0 ? maxCount / totalCount : 0.5;
        res.json({
            current_regime: dominantRegime,
            confidence: Math.round(confidence * 100) / 100,
            regime_distribution: regimeCounts,
            entities_classified: totalCount,
            interpretation: {
                normal: 'Standard market conditions, no elevated stress',
                stressed: 'Elevated volatility, defensive sentiment detected',
                pre_tipping: 'Critical state, potential cascade imminent',
                post_event: 'Recovery phase following major event',
            }[dominantRegime] || 'Unknown regime state',
            timestamp: new Date().toISOString(),
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /portfolio/risk_map ─────────────────────────────────────────────────────
// Analyze portfolio exposure to crashes, channels, and regimes
//
// Body: {
//   positions: Array<{ entity: string, weight: number }>
// }
app.post('/portfolio/risk_map', async (req, res) => {
    try {
        const { positions } = req.body;
        if (!positions || !Array.isArray(positions)) {
            res.status(400).json({ error: 'positions array is required' });
            return;
        }
        const exposures = {
            credit: 0,
            liquidity: 0,
            sentiment: 0,
            supply_chain: 0,
            counterparty: 0,
        };
        const crashExposures = [];
        // Analyze each position
        for (const pos of positions) {
            const { entity, weight } = pos;
            if (!entity || weight === undefined)
                continue;
            // Get entity's crash exposures
            const neighbors = await knowledgeGraph.getNeighbours(entity);
            for (const { edge, neighbour } of neighbors) {
                // Check for crash exposure
                if (neighbour.type && neighbour.type.includes('Crash')) {
                    const existing = crashExposures.find(c => c.crash === neighbour.name);
                    if (existing) {
                        existing.exposure += weight * (edge.confidence || 0.5);
                        existing.affected_positions.push(entity);
                    }
                    else {
                        crashExposures.push({
                            crash: neighbour.name,
                            exposure: weight * (edge.confidence || 0.5),
                            affected_positions: [entity],
                        });
                    }
                }
                // Check for channel exposure
                if (neighbour.type === 'RiskChannel') {
                    const channelType = neighbour.properties?.channel_type || 'unknown';
                    if (exposures[channelType] !== undefined) {
                        exposures[channelType] += weight * (edge.confidence || 0.5);
                    }
                }
            }
        }
        // Normalize exposures
        const totalWeight = positions.reduce((sum, p) => sum + (p.weight || 0), 0);
        if (totalWeight > 0) {
            for (const key of Object.keys(exposures)) {
                exposures[key] = Math.round((exposures[key] / totalWeight) * 100) / 100;
            }
        }
        res.json({
            portfolio_summary: {
                positions_count: positions.length,
                total_weight: totalWeight,
            },
            channel_exposures: exposures,
            crash_exposures: crashExposures.sort((a, b) => b.exposure - a.exposure).slice(0, 5),
            highest_risk: Object.entries(exposures)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 3)
                .map(([channel, exposure]) => ({
                channel,
                exposure,
                warning: exposure > 0.3 ? 'HIGH' : exposure > 0.15 ? 'MEDIUM' : 'LOW',
            })),
            timestamp: new Date().toISOString(),
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /narrative/brief ────────────────────────────────────────────────────────
// Get active narratives for an asset/entity
//
// Body: { entity: string }
app.post('/narrative/brief', async (req, res) => {
    try {
        const { entity } = req.body;
        if (!entity) {
            res.status(400).json({ error: 'entity is required' });
            return;
        }
        // Find entity
        const entities = await knowledgeGraph.findEntity(entity);
        if (!entities.length) {
            res.json({ entity, found: false, narratives: [] });
            return;
        }
        const entityId = entities[0].id;
        // Get connected narratives
        const neighbors = await knowledgeGraph.getNeighbours(entityId);
        const narratives = neighbors
            .filter(n => n.neighbour.type === 'Narrative')
            .map(n => ({
            narrative: n.neighbour.name,
            theme: n.neighbour.properties?.theme || n.neighbour.name,
            strength: n.edge.confidence || 0.5,
            relation: n.edge.relation,
        }))
            .sort((a, b) => b.strength - a.strength);
        res.json({
            entity: entities[0].name,
            entity_type: entities[0].type,
            found: true,
            active_narratives: narratives.slice(0, 10),
            dominant_narrative: narratives.length > 0 ? narratives[0].narrative : null,
            narrative_strength: narratives.length > 0 ? narratives[0].strength : 0,
            timestamp: new Date().toISOString(),
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /event/explain ──────────────────────────────────────────────────────────
// Explain any MacroEvent or Crash via causal tree traversal
//
// Body: { event: string } or { crash: string }
app.post('/event/explain', async (req, res) => {
    try {
        const { event, crash } = req.body;
        const target = event || crash;
        if (!target) {
            res.status(400).json({ error: 'event or crash name is required' });
            return;
        }
        const db = knowledgeGraph.db;
        if (!db) {
            res.status(503).json({ error: 'Database not available' });
            return;
        }
        // Find the target entity
        const findQuery = `
      MATCH (e:Entity)
      WHERE e.name_lower CONTAINS "${target.toLowerCase().replace(/"/g, '\\"')}"
        AND e.type IN ['MarketCrash', 'SectorCrash', 'FlashCrash', 'BankRun',
                       'LiquidityCrisis', 'CurrencyCrisis', 'MacroEvent', 'Event']
      RETURN e.id AS id, e.name AS name, e.type AS type,
             e.peak_drawdown_pct AS drawdown, e.start_date AS start_date,
             e.regime_before AS regime_before, e.regime_during AS regime_during
      LIMIT 1
    `;
        const found = await db.graphQuery(findQuery, {});
        if (!found || found.length === 0) {
            res.json({ event: target, found: false, explanation: null });
            return;
        }
        const entity = found[0];
        const entityId = entity.id;
        // Get causal factors (upstream TRIGGERED edges)
        const factorsQuery = `
      MATCH (factor:Entity)-[r:RELATES]->(crash:Entity {id: "${entityId}"})
      WHERE r.relation IN ['TRIGGERED', 'SHOCKED', 'causes', 'caused_by']
      RETURN factor.id AS id, factor.name AS name, factor.type AS type,
             r.causal_weight AS weight, factor.mechanism AS mechanism,
             factor.preceded_crash_by_days AS lead_days
      ORDER BY r.causal_weight DESC
      LIMIT 10
    `;
        const factors = await db.graphQuery(factorsQuery, {});
        // Get channels this event propagated through
        const channelsQuery = `
      MATCH (e:Entity {id: "${entityId}"})-[r:RELATES]->(ch:Entity {type: 'Channel'})
      WHERE r.relation IN ['TRANSMITS_THROUGH', 'TRANSMITTED_THROUGH']
      RETURN ch.channel_type AS channel, ch.name AS name,
             r.confidence AS strength, r.lag_hours AS lag_hours
      ORDER BY r.confidence DESC
    `;
        const channels = await db.graphQuery(channelsQuery, {});
        // Get affected entities
        const affectedQuery = `
      MATCH (e:Entity {id: "${entityId}"})-[r:RELATES]->(affected:Entity)
      WHERE r.relation IN ['CRASHED', 'impacts', 'PROPAGATED_TO', 'CASCADED_TO']
        AND affected.type IN ['Corporation', 'Company', 'FinancialInstitution', 'Asset', 'Industry']
      RETURN affected.name AS name, affected.type AS type,
             r.relation AS impact_type, r.confidence AS severity
      ORDER BY r.confidence DESC
      LIMIT 20
    `;
        const affected = await db.graphQuery(affectedQuery, {});
        // Get narratives
        const narrativeQuery = `
      MATCH (e:Entity {id: "${entityId}"})-[r:RELATES]->(n:Entity {type: 'Narrative'})
      WHERE r.relation = 'DRIVES_NARRATIVE'
      RETURN n.name AS narrative, n.theme AS theme,
             n.sentiment_score AS sentiment, r.contribution_weight AS weight
      ORDER BY r.contribution_weight DESC
    `;
        const narratives = await db.graphQuery(narrativeQuery, {});
        // Get causal chain summary if exists
        const chainQuery = `
      MATCH (e:Entity {id: "${entityId}"})-[r:RELATES]->(chain:Entity {type: 'CausalChain'})
      WHERE r.relation = 'PART_OF_CHAIN'
      RETURN chain.summary AS summary, chain.depth AS depth
      LIMIT 1
    `;
        const chains = await db.graphQuery(chainQuery, {});
        // Get regime context
        const regimeQuery = `
      MATCH (e:Entity {id: "${entityId}"})-[r:RELATES]->(regime:Entity {type: 'Regime'})
      WHERE r.relation = 'IN_REGIME'
      RETURN regime.regime_state AS state, regime.name AS name, r.since AS since
      LIMIT 1
    `;
        const regimes = await db.graphQuery(regimeQuery, {});
        res.json({
            event: {
                id: entityId,
                name: entity.name,
                type: entity.type,
                drawdown_pct: entity.drawdown,
                start_date: entity.start_date,
            },
            found: true,
            causal_factors: factors.map((f) => ({
                name: f.name,
                type: f.type,
                causal_weight: parseFloat(f.weight) || 0,
                mechanism: f.mechanism,
                lead_days: parseInt(f.lead_days) || 0,
            })),
            transmission_channels: channels.map((ch) => ({
                channel: ch.channel,
                name: ch.name,
                strength: parseFloat(ch.strength) || 0,
                lag_hours: parseInt(ch.lag_hours) || 0,
            })),
            affected_entities: affected.map((a) => ({
                name: a.name,
                type: a.type,
                impact_type: a.impact_type,
                severity: parseFloat(a.severity) || 0,
            })),
            narratives: narratives.map((n) => ({
                narrative: n.narrative,
                theme: n.theme,
                sentiment: parseFloat(n.sentiment) || 0,
                weight: parseFloat(n.weight) || 0,
            })),
            causal_chain: chains.length > 0 ? {
                summary: chains[0].summary,
                depth: parseInt(chains[0].depth) || 0,
            } : null,
            regime: regimes.length > 0 ? {
                state: regimes[0].state,
                name: regimes[0].name,
                since: regimes[0].since,
            } : { state: entity.regime_during || 'unknown' },
            timestamp: new Date().toISOString(),
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── /plan/scenarios ────────────────────────────────────────────────────────
// Generate scenario plans for a portfolio or entity
//
// Body: {
//   target: string,           // Entity or crash to plan around
//   portfolio?: Array<{ entity: string, weight: number }>,
//   variants?: Array<{ name: string, shock_magnitude: number }>
// }
app.post('/plan/scenarios', async (req, res) => {
    try {
        const { target, portfolio, variants } = req.body;
        if (!target) {
            res.status(400).json({ error: 'target entity/crash name is required' });
            return;
        }
        const db = knowledgeGraph.db;
        if (!db) {
            res.status(503).json({ error: 'Database not available' });
            return;
        }
        // Find relevant historical crashes/events
        const scenariosQuery = `
      MATCH (e:Entity)
      WHERE (e.name_lower CONTAINS "${target.toLowerCase().replace(/"/g, '\\"')}"
             OR e.type IN ['MarketCrash', 'SectorCrash', 'BankRun'])
        AND e.type IN ['MarketCrash', 'SectorCrash', 'FlashCrash', 'BankRun',
                       'LiquidityCrisis', 'CurrencyCrisis']
      RETURN e.id AS id, e.name AS name, e.type AS type,
             e.peak_drawdown_pct AS drawdown, e.start_date AS start_date,
             e.recovery_days AS recovery_days
      ORDER BY e.peak_drawdown_pct ASC
      LIMIT 5
    `;
        const scenarios = await db.graphQuery(scenariosQuery, {});
        // Get simulated outcomes if any exist
        const simQuery = `
      MATCH (s:Entity {type: 'Scenario'})-[r:RELATES]->(o:Entity {type: 'SimulatedOutcome'})
      WHERE s.name_lower CONTAINS "${target.toLowerCase().replace(/"/g, '\\"')}"
      RETURN s.name AS scenario_name, o.drawdown_pct AS drawdown,
             o.confidence AS confidence, o.peak_intensity AS intensity
      ORDER BY o.drawdown_pct ASC
      LIMIT 10
    `;
        const simulated = await db.graphQuery(simQuery, {});
        // Compute portfolio exposure if provided
        let portfolioExposure = null;
        if (portfolio && Array.isArray(portfolio)) {
            const exposures = {
                credit: 0, liquidity: 0, sentiment: 0,
                supply_chain: 0, counterparty: 0, regulation: 0,
            };
            const totalWeight = portfolio.reduce((sum, p) => sum + (p.weight || 0), 0);
            for (const pos of portfolio.slice(0, 20)) {
                const neighbors = await knowledgeGraph.getNeighbours(pos.entity);
                const weight = totalWeight > 0 ? (pos.weight || 0) / totalWeight : 0;
                for (const { edge, neighbour } of neighbors) {
                    if (neighbour.type === 'Channel') {
                        const chType = neighbour.properties?.channel_type;
                        if (chType && exposures[chType] !== undefined) {
                            exposures[chType] += weight * (edge.confidence || 0.5);
                        }
                    }
                }
            }
            portfolioExposure = {
                positions_count: portfolio.length,
                channel_exposures: Object.fromEntries(Object.entries(exposures).map(([k, v]) => [k, Math.round(v * 100) / 100])),
            };
        }
        // Generate scenario variants
        const defaultVariants = [
            { name: 'mild', shock_magnitude: 0.5, description: 'Mild shock, single-channel' },
            { name: 'moderate', shock_magnitude: 1.0, description: 'Historical baseline replay' },
            { name: 'severe', shock_magnitude: 1.5, description: 'Amplified multi-channel contagion' },
            { name: 'systemic', shock_magnitude: 2.0, description: 'Full systemic cascade' },
        ];
        const activeVariants = variants || defaultVariants;
        // Build projected outcomes from historical data
        const projectedOutcomes = scenarios.map((s) => {
            const baseDrawdown = parseFloat(s.drawdown) || -10;
            return activeVariants.map((v) => ({
                scenario: s.name,
                variant: v.name,
                projected_drawdown: Math.round(baseDrawdown * v.shock_magnitude * 10) / 10,
                recovery_days: Math.round((parseInt(s.recovery_days) || 90) * v.shock_magnitude),
                confidence: Math.max(0.3, 0.8 - (v.shock_magnitude - 1) * 0.2),
            }));
        }).flat();
        // Recommendations
        const recommendations = [];
        const worstCase = projectedOutcomes.length > 0
            ? Math.min(...projectedOutcomes.map((o) => o.projected_drawdown))
            : -10;
        if (worstCase < -30) {
            recommendations.push({
                action: 'reduce_exposure',
                urgency: 'high',
                description: `Worst-case ${worstCase}% drawdown — reduce concentrated positions`,
            });
        }
        if (worstCase < -15) {
            recommendations.push({
                action: 'hedge_tail_risk',
                urgency: 'medium',
                description: 'Consider protective puts or CDS for exposed sectors',
            });
        }
        recommendations.push({
            action: 'monitor_channels',
            urgency: 'low',
            description: 'Monitor credit spreads, VIX, and funding rates for early signals',
        });
        res.json({
            target,
            historical_scenarios: scenarios.map((s) => ({
                name: s.name,
                type: s.type,
                drawdown_pct: parseFloat(s.drawdown) || 0,
                start_date: s.start_date,
                recovery_days: parseInt(s.recovery_days) || 0,
            })),
            simulated_outcomes: simulated.map((s) => ({
                scenario: s.scenario_name,
                drawdown_pct: parseFloat(s.drawdown) || 0,
                confidence: parseFloat(s.confidence) || 0,
                intensity: parseFloat(s.intensity) || 0,
            })),
            projected_outcomes: projectedOutcomes,
            portfolio_exposure: portfolioExposure,
            recommendations,
            timestamp: new Date().toISOString(),
        });
    }
    catch (err) {
        res.status(500).json({ error: err.message });
    }
});
// ─── START ────────────────────────────────────────────────────────────────────
// Listen FIRST so Railway proxy always gets a response, then connect DB in background.
// This prevents 502 errors caused by FalkorDB init failures crashing the process.
async function initDbWithRetry(maxAttempts = 10, delayMs = 5000) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            console.log(`[START] DB init attempt ${attempt}/${maxAttempts}...`);
            await knowledgeGraph.init();
            console.log('[START] FalkorDB connected successfully');
            return true;
        }
        catch (err) {
            console.error(`[START] DB init attempt ${attempt} failed: ${err.message}`);
            if (attempt < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }
    }
    console.error('[START] All DB init attempts failed — running in degraded mode (503 on graph endpoints)');
    return false;
}
async function start() {
    // Step 1: Start HTTP server immediately so Railway health checks respond
    await new Promise(resolve => {
        app.listen(Number(PORT), '0.0.0.0', () => {
            console.log(`Forage Reality Graph API listening on port ${PORT}`);
            console.log(`Health: http://localhost:${PORT}/health`);
            resolve();
        });
    });
    // Step 2: Connect to FalkorDB in background — never crash the process
    const dbReady = await initDbWithRetry();
    if (!dbReady) {
        return; // degraded mode — endpoints return 503 via isReady() checks
    }
    console.log('[START] Forage Reality Graph fully operational');
}
start().catch(err => {
    // Only fatal if we can't even bind the port
    console.error('[START] Fatal startup error:', err);
    process.exit(1);
});
