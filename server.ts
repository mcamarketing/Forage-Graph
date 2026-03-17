/**
 * Forage Graph API — src/server.ts
 *
 * Standalone Express server. Runs on Railway / any VPS.
 * The Apify actor POSTs to this after every tool call (fire and forget).
 * All graph data lives here — never on Apify infrastructure.
 *
 * Endpoints:
 *   POST /ingest              — receive tool output, extract entities, merge into graph
 *   POST /ingest_raw          — directly insert structured entities with explicit relationships
 *   POST /query               — find entities by name
 *   POST /enrich              — everything the graph knows about an entity (incl. causal paths)
 *   POST /connections         — find relationship path between two entities
 *   POST /search              — find companies by industry and optional location
 *   POST /browse              — list all entities of a given type
 *   POST /browse_country      — list all entities associated with a country
 *   GET  /stats               — graph size and coverage
 *   GET  /health              — liveness check
 *
 * Auth: Bearer token via GRAPH_API_SECRET env var.
 * All write endpoints require auth. /health is open.
 */

import express, { Request, Response, NextFunction } from 'express';
import { knowledgeGraph } from './knowledge-graph.js';
import helmet from 'helmet';

const app = express();
const PORT = process.env.PORT || 3000;
const SECRET = process.env.GRAPH_API_SECRET;

if (!SECRET) {
  console.error('CRITICAL: GRAPH_API_SECRET env var is required for authentication.');
  process.exit(1);
}

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────

// Security headers
app.use(helmet());

// Body parser with size limit
app.use(express.json({ limit: '10mb' }));

// Structured request logging
app.use((req: Request, _res: Response, next: NextFunction) => {
  // In production, consider using a dedicated logger like Pino or Winston
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} - IP: ${req.ip}`);
  next();
});

// Auth — all routes except /health require Bearer token
function requireAuth(req: Request, res: Response, next: NextFunction): void {
  if (req.path === '/health') {
    next();
    return;
  }
  
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Unauthorized: Missing or invalid Authorization header format' });
    return;
  }

  const token = auth.slice(7);
  // Using timing-safe comparison would be ideal here if using Node's crypto.timingSafeEqual,
  // but a simple string comparison is acceptable for internal API secrets.
  if (token !== SECRET) {
    res.status(403).json({ error: 'Forbidden: Invalid token' });
    return;
  }
  
  next();
}

app.use(requireAuth);

// ─── ERROR HANDLING MIDDLEWARE ────────────────────────────────────────────────
// Catch-all for JSON parsing errors and other synchronous middleware errors
app.use((err: any, _req: Request, res: Response, next: NextFunction) => {
  if (err instanceof SyntaxError && 'body' in err) {
    res.status(400).json({ error: 'Bad Request: Invalid JSON payload' });
    return;
  }
  next(err);
});

// ─── HEALTH ───────────────────────────────────────────────────────────────────

app.get('/health', async (_req: Request, res: Response) => {
  try {
    const healthy = await knowledgeGraph.isHealthy();
    res.status(healthy ? 200 : 503).json({
      status: healthy ? 'ok' : 'degraded',
      graph: healthy ? 'connected' : 'disconnected',
      ts: new Date().toISOString(),
    });
  } catch (err) {
    res.status(503).json({
      status: 'error',
      graph: 'disconnected',
      error: 'Health check failed',
      ts: new Date().toISOString(),
    });
  }
});

// ─── INGEST ───────────────────────────────────────────────────────────────────
// Called by Apify actor after every tool response — fire and forget on caller side.
// Returns 202 immediately, processes async.
//
// Body: { tool_name: string, result: any }

app.post('/ingest', (req: Request, res: Response) => {
  const { tool_name, result } = req.body;

  if (!tool_name || typeof tool_name !== 'string') {
    res.status(400).json({ error: 'tool_name is required and must be a string' });
    return;
  }

  if (result === undefined || result === null) {
    res.status(400).json({ error: 'result is required' });
    return;
  }

  // Respond immediately — never make the caller wait
  res.status(202).json({ accepted: true, message: 'Ingestion task queued' });

  // Process async, catch errors to prevent unhandled promise rejections crashing the server
  knowledgeGraph.ingest(tool_name, result).catch((err) => {
    console.error(`[Background Ingest Error] Tool: ${tool_name} -`, err.message || err);
  });
});

// ─── QUERY ────────────────────────────────────────────────────────────────────
// Find entities by name, optionally filtered by type.
//
// Body: { name: string, type?: EntityType, min_confidence?: number }

app.post('/query', async (req: Request, res: Response) => {
  try {
    const { name, type, min_confidence } = req.body;
    
    if (!name || typeof name !== 'string') {
      res.status(400).json({ error: 'name is required and must be a string' });
      return;
    }

    const confidenceThreshold = typeof min_confidence === 'number' ? min_confidence : 0.0;

    const entities = await knowledgeGraph.findEntity(name, type);
    const filtered = entities.filter(e => e.confidence >= confidenceThreshold);

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
    console.error('[Query Error]', err);
    res.status(500).json({ error: 'Internal server error during query operation' });
  }
});

// ─── ENRICH ───────────────────────────────────────────────────────────────────
// Everything the graph knows about a company or domain — entity + all relationships.
//
// Body: { identifier: string }

app.post('/enrich', async (req: Request, res: Response) => {
  try {
    const { identifier } = req.body;
    
    if (!identifier || typeof identifier !== 'string') {
      res.status(400).json({ error: 'identifier is required and must be a string' });
      return;
    }

    const result = await knowledgeGraph.enrich(identifier);

    if (!result.entity) {
      res.status(404).json({
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
        valid_from: result.entity.valid_from,
        valid_until: result.entity.valid_until,
        properties: result.entity.properties,
        sources: result.entity.sources,
        provenance: result.entity.provenance,
      },
      relationships: Object.fromEntries(
        Object.entries(result.related).map(([relation, nodes]) => [
          relation,
          nodes.map(n => ({ name: n.name, type: n.type, confidence: n.confidence })),
        ])
      ),
      causal_paths: result.causal_paths.map(cp => ({
        relation: cp.relation,
        entity: { name: cp.entity.name, type: cp.entity.type },
        causal_weight: cp.causal_weight,
        mechanism: cp.mechanism,
      })),
      confidence: result.confidence,
    });
  } catch (err: any) {
    console.error('[Enrich Error]', err);
    res.status(500).json({ error: 'Internal server error during enrich operation' });
  }
});

// ─── CONNECTIONS ──────────────────────────────────────────────────────────────
// Find relationship path between two entities.
//
// Body: { from: string, to: string, max_hops?: number }

app.post('/connections', async (req: Request, res: Response) => {
  try {
    const { from, to, max_hops } = req.body;
    
    if (!from || typeof from !== 'string' || !to || typeof to !== 'string') {
      res.status(400).json({ error: 'from and to are required and must be strings' });
      return;
    }

    const requestedHops = typeof max_hops === 'number' ? max_hops : 3;
    const hops = Math.min(Math.max(1, requestedHops), 5);
    
    const result = await knowledgeGraph.findConnections(from, to, hops);

    if (!result) {
      res.status(404).json({
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
        causal_weight: e.causal_weight,
        mechanism: e.mechanism,
        valid_from: e.valid_from,
        valid_until: e.valid_until,
      })),
    });
  } catch (err: any) {
    console.error('[Connections Error]', err);
    res.status(500).json({ error: 'Internal server error during connections operation' });
  }
});

// ─── INDUSTRY + LOCATION SEARCH ───────────────────────────────────────────────
// Find companies by industry and optional location.
//
// Body: { industry: string, location?: string, min_confidence?: number }

app.post('/search', async (req: Request, res: Response) => {
  try {
    const { industry, location, min_confidence } = req.body;
    
    if (!industry || typeof industry !== 'string') {
      res.status(400).json({ error: 'industry is required and must be a string' });
      return;
    }

    if (location !== undefined && typeof location !== 'string') {
      res.status(400).json({ error: 'location must be a string if provided' });
      return;
    }

    const confidenceThreshold = typeof min_confidence === 'number' ? min_confidence : 0.0;

    const companies = await knowledgeGraph.findByIndustryAndLocation(industry, location);
    const filtered = companies.filter(c => c.confidence >= confidenceThreshold);

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
    console.error('[Search Error]', err);
    res.status(500).json({ error: 'Internal server error during search operation' });
  }
});

// ─── BROWSE BY TYPE ─────────────────────────────────────────────────────────
// List all entities of a given type (company, country, person, conflict_event, etc.)
//
// Body: { type: EntityType, limit?: number }

app.post('/browse', async (req: Request, res: Response) => {
  try {
    const { type, limit } = req.body;

    if (!type || typeof type !== 'string') {
      res.status(400).json({ error: 'type is required and must be a string' });
      return;
    }

    const entities = await knowledgeGraph.findByType(type as any, typeof limit === 'number' ? limit : 100);

    res.json({
      type,
      count: entities.length,
      entities: entities.map(e => ({
        id: e.id,
        name: e.name,
        type: e.type,
        confidence: e.confidence,
        properties: e.properties,
        provenance: e.provenance,
        valid_from: e.valid_from,
        valid_until: e.valid_until,
        last_seen: e.last_seen,
      })),
    });
  } catch (err: any) {
    console.error('[Browse Error]', err);
    res.status(500).json({ error: 'Internal server error during browse operation' });
  }
});

// ─── BROWSE BY COUNTRY ────────────────────────────────────────────────────────
// List all entities associated with a country code.
//
// Body: { country: string, type?: EntityType }

app.post('/browse_country', async (req: Request, res: Response) => {
  try {
    const { country, type } = req.body;

    if (!country || typeof country !== 'string') {
      res.status(400).json({ error: 'country is required and must be a string' });
      return;
    }

    const entities = await knowledgeGraph.findByCountry(country, type as any);

    res.json({
      country,
      type: type || 'any',
      count: entities.length,
      entities: entities.map(e => ({
        id: e.id,
        name: e.name,
        type: e.type,
        confidence: e.confidence,
        properties: e.properties,
        last_seen: e.last_seen,
      })),
    });
  } catch (err: any) {
    console.error('[Browse Country Error]', err);
    res.status(500).json({ error: 'Internal server error during browse_country operation' });
  }
});

// ─── INGEST RAW ───────────────────────────────────────────────────────────────
// Directly insert a structured entity with explicit relationships.
// Used by external data loaders: Wikidata, GLEIF, ACLED, UN Comtrade, etc.
//
// Body: { name, type, properties, relationships: [{targetName, targetType, relation, ...}], source, confidence? }

app.post('/ingest_raw', async (req: Request, res: Response) => {
  try {
    const { name, type, properties, relationships, source, confidence } = req.body;

    if (!name || typeof name !== 'string') {
      res.status(400).json({ error: 'name is required and must be a string' });
      return;
    }
    if (!type || typeof type !== 'string') {
      res.status(400).json({ error: 'type is required and must be a string' });
      return;
    }
    if (!source || typeof source !== 'string') {
      res.status(400).json({ error: 'source is required and must be a string' });
      return;
    }

    const entity = await knowledgeGraph.ingestRaw(
      name,
      type as any,
      properties || {},
      Array.isArray(relationships) ? relationships : [],
      source,
      typeof confidence === 'number' ? confidence : 0.8
    );

    res.status(201).json({
      accepted: true,
      entity: { id: entity.id, name: entity.name, type: entity.type },
      relationships_created: Array.isArray(relationships) ? relationships.length : 0,
    });
  } catch (err: any) {
    console.error('[Ingest Raw Error]', err);
    res.status(500).json({ error: 'Internal server error during ingest_raw operation' });
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
    console.error('[Stats Error]', err);
    res.status(500).json({ error: 'Internal server error while fetching stats' });
  }
});

// ─── GLOBAL ERROR HANDLER ─────────────────────────────────────────────────────
// Catch any unhandled errors in routes
app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
  console.error('[Unhandled Server Error]', err);
  res.status(500).json({ error: 'An unexpected error occurred' });
});

// ─── START ────────────────────────────────────────────────────────────────────

async function start() {
  // Start HTTP server FIRST so Railway's health check can reach /health immediately.
  // The /health endpoint returns 503 while the DB is still connecting, which is
  // correct — Railway will keep retrying until it gets a 200.
  const server = app.listen(PORT, () => {
    console.log(`Forage Graph API running on port ${PORT}`);
    console.log(`Health: http://localhost:${PORT}/health`);
  });

  // Graceful shutdown handling
  const shutdown = async () => {
    console.log('Shutting down server gracefully...');
    server.close(async () => {
      console.log('HTTP server closed.');
      try {
        await knowledgeGraph.disconnect();
        console.log('Database disconnected.');
        process.exit(0);
      } catch (err) {
        console.error('Error during database disconnection:', err);
        process.exit(1);
      }
    });

    // Force shutdown after 10 seconds
    setTimeout(() => {
      console.error('Could not close connections in time, forcefully shutting down');
      process.exit(1);
    }, 10000);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  // Connect to DB in the background with exponential backoff.
  // The server stays alive throughout — /health returns 503 until connected.
  let retries = 0;
  const maxRetries = 10;

  while (retries < maxRetries) {
    try {
      await knowledgeGraph.init();
      console.log('FalkorDB initialized successfully');
      break;
    } catch (err) {
      retries++;
      if (retries >= maxRetries) {
        console.error('CRITICAL: Failed to init FalkorDB after', maxRetries, 'retries. Server remains up but unhealthy.');
        break;
      }
      const delay = Math.min(1000 * Math.pow(2, retries), 10000);
      console.log(`Init failed (attempt ${retries}/${maxRetries}), retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

start().catch(err => {
  console.error('CRITICAL: Failed to start server:', err);
  process.exit(1);
});
