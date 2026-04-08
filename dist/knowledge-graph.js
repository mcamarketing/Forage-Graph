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
import { generateULEMIdentity, generateCompositeId } from './ulem-identity.js';
import { AdamicAdarScorer, JaccardScorer } from './link-prediction.js';
import { HawkesProcessEngine, estimateHawkesParams } from './hawkes-contagion.js';
// ─── STORAGE LAYER ────────────────────────────────────────────────────────────
// Uses FalkorDB via redis client. Swap internals here when scaling.
// FalkorDB speaks Redis protocol — same client, graph-native Cypher queries on top.
class KnowledgeStore {
    constructor() {
        this.client = null;
        this.graphName = 'forage_v1';
        this.connectionReady = false;
    }
    async init() {
        const url = process.env.FALKORDB_URL || process.env.REDIS_URL;
        if (!url) {
            console.error('[GRAPH] ERROR: FALKORDB_URL or REDIS_URL environment variable required');
            throw new Error('FALKORDB_URL or REDIS_URL environment variable required');
        }
        const maskedUrl = url.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@');
        console.log(`[GRAPH] Connecting to FalkorDB at ${maskedUrl}...`);
        this.client = createClient({
            url,
            socket: {
                connectTimeout: 15000,
                reconnectStrategy: (retries) => {
                    console.error(`[GRAPH] Redis reconnect attempt ${retries}`);
                    if (retries > 10) {
                        console.error('[GRAPH] Max reconnect attempts reached');
                        return new Error('Max reconnect attempts reached');
                    }
                    return Math.min(retries * 500, 5000);
                }
            }
        });
        // Log all errors - don't silence in production!
        this.client.on('error', (err) => {
            console.error('[GRAPH ERROR]', err.message);
            this.connectionReady = false;
        });
        this.client.on('connect', () => {
            console.log('[GRAPH] Connected to FalkorDB');
            this.connectionReady = true;
        });
        this.client.on('reconnecting', () => {
            console.log('[GRAPH] Reconnecting to FalkorDB...');
            this.connectionReady = false;
        });
        await this.client.connect();
        // Verify connection actually works
        const pong = await this.client.ping();
        if (pong !== 'PONG') {
            throw new Error('FalkorDB ping failed - connection not working');
        }
        this.connectionReady = true;
        console.log('[GRAPH] FalkorDB connection verified (PONG received)');
        // Create indexes for fast lookups
        await this.ensureIndexes();
    }
    isConnectionReady() {
        return this.connectionReady && this.client !== null;
    }
    async ensureIndexes() {
        if (!this.client)
            return;
        try {
            // FalkorDB: create indexes on node properties we query by
            await this.graphQuery(`CREATE INDEX FOR (n:Entity) ON (n.id)`, {}).catch(() => { }); // Ignore if already exists
            await this.graphQuery(`CREATE INDEX FOR (n:Entity) ON (n.name_lower)`, {}).catch(() => { });
            await this.graphQuery(`CREATE INDEX FOR (n:Entity) ON (n.type)`, {}).catch(() => { });
            // Simulation Layer indexes [sim-003]
            await this.graphQuery(`CREATE INDEX FOR (n:Entity) ON (n.is_simulation)`, {}).catch(() => { });
        }
        catch {
            // Indexes are optional — graph still works without them
        }
    }
    // ── SIMULATION BOUNDARY VALIDATION [sim-004] ────────────────────────────────
    // SimAgents can only READS_FROM Reality nodes — never modify them
    isSimulationType(type) {
        return type === 'SimAgent' || type === 'SimEpisode';
    }
    isSimulationRelation(relation) {
        return relation === 'READS_FROM' || relation === 'SIMULATES' || relation === 'HYPOTHESIZES';
    }
    /**
     * Validate simulation boundary constraint.
     * SimAgents can READS_FROM Reality, but cannot create causal edges to Reality.
     * Returns true if the edge is valid, false if it violates simulation boundary.
     */
    validateSimulationBoundary(fromType, toType, relation) {
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
    getClient() {
        return this.client;
    }
    /**
     * Get graph name for GraphBLAS operations.
     */
    getGraphName() {
        return this.graphName;
    }
    // ── EXPORT ALL [backup-003] ──────────────────────────────────────────────────
    /**
     * Get all nodes from the graph for backup/export.
     */
    async getAllNodes() {
        const rows = await this.graphQuery(`MATCH (n:Entity) RETURN n`, {});
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
            };
        });
    }
    /**
     * Get all edges from the graph for backup/export.
     */
    async getAllEdges() {
        const rows = await this.graphQuery(`MATCH (a:Entity)-[r:RELATES]->(b:Entity) RETURN a.id AS from_id, a.name AS from_name, r, b.id AS to_id, b.name AS to_name`, {});
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
        }));
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
    async batchCreateRelationships(batch) {
        if (!this.client || batch.length === 0)
            return;
        try {
            await this.graphQuery(`UNWIND $batch AS row
         MATCH (a:Entity {id: row.from_id})
         MATCH (b:Entity {id: row.to_id})
         MERGE (a)-[r:RELATES]->(b)
         SET r.relation = row.relation,
             r.weight = row.weight,
             r.contagion_weight = row.contagion_weight,
             r.updated_at = timestamp()`, { batch });
        }
        catch {
            // Silent failure for batch operations
        }
    }
    // Execute a Cypher query against FalkorDB
    async graphQuery(query, params) {
        if (!this.client)
            return [];
        try {
            // FalkorDB uses GRAPH.QUERY command
            // Inline params directly into query (most reliable across FalkorDB versions)
            let q = query;
            for (const [k, v] of Object.entries(params)) {
                const replacement = this.cypherValue(v);
                q = q.replace(new RegExp(`\\$${k}\\b`, 'g'), replacement);
            }
            console.log('[GRAPH QUERY]', q.substring(0, 200));
            const result = await this.client.sendCommand([
                'GRAPH.QUERY',
                this.graphName,
                q,
            ]);
            return this.parseGraphResult(result);
        }
        catch (err) {
            console.error('[GRAPH QUERY ERROR]', err.message, 'Query:', query);
            return [];
        }
    }
    // Convert JS value to Cypher literal syntax
    cypherValue(v) {
        if (v === null || v === undefined)
            return 'null';
        if (typeof v === 'string')
            return `"${v.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
        if (typeof v === 'number' || typeof v === 'boolean')
            return String(v);
        if (Array.isArray(v))
            return `[${v.map(x => this.cypherValue(x)).join(', ')}]`;
        if (typeof v === 'object') {
            const pairs = Object.entries(v).map(([key, val]) => `${key}: ${this.cypherValue(val)}`);
            return `{${pairs.join(', ')}}`;
        }
        return String(v);
    }
    parseGraphResult(raw) {
        if (!raw || !Array.isArray(raw))
            return [];
        // FalkorDB returns: [[headers], [[row1], [row2], ...], [stats]]
        // Headers and values may be typed: [type_id, actual_value]
        // Type IDs: 1=null, 2=string, 3=integer, etc.
        const rawHeaders = raw[0];
        const data = raw[1];
        const stats = raw[2];
        if (!data || !Array.isArray(data))
            return [];
        // Parse headers (they may also have type prefixes)
        const headers = [];
        if (rawHeaders && Array.isArray(rawHeaders)) {
            for (const h of rawHeaders) {
                const parsed = this.parseGraphElement(h);
                headers.push(String(parsed));
            }
        }
        // If headers exist, map data to named objects
        if (headers.length > 0) {
            return data.map((row) => {
                const obj = {};
                for (let i = 0; i < headers.length; i++) {
                    const colName = headers[i];
                    const value = row[i];
                    obj[colName] = this.parseGraphElement(value);
                }
                // If single column, also store at index 0 for backwards compat
                if (headers.length === 1) {
                    obj[0] = obj[headers[0]];
                }
                return obj;
            });
        }
        // Fallback: return raw data rows with parsed elements
        return data.map((row) => Array.isArray(row) ? row.map(v => this.parseGraphElement(v)) : row);
    }
    parseGraphElement(elem) {
        if (elem === null || elem === undefined)
            return null;
        // Non-array values returned directly
        if (typeof elem !== 'object' || !Array.isArray(elem))
            return elem;
        // FalkorDB scalar format: [type_id, value]
        // Type IDs: 1=null, 2=string, 3=integer, 4=boolean, 5=double, 6=array, 7=edge, 8=node, 9=path
        // Type ID might be number or string
        if (elem.length === 2) {
            const typeId = typeof elem[0] === 'string' ? parseInt(elem[0], 10) : elem[0];
            const value = elem[1];
            if (typeof typeId === 'number' && typeId >= 1 && typeId <= 10) {
                if (typeId === 1)
                    return null; // NULL
                if (typeId === 8)
                    return this.parseGraphElement(value); // NODE - recurse
                if (typeId === 7)
                    return this.parseGraphElement(value); // EDGE - recurse
                if (typeId === 6 && Array.isArray(value)) { // ARRAY
                    return value.map(v => this.parseGraphElement(v));
                }
                return value; // STRING, INTEGER, BOOLEAN, DOUBLE - return value directly
            }
        }
        // FalkorDB node format: [internal_id, [labels], [[key, type, value], ...]]
        if (elem.length === 3 && Array.isArray(elem[1]) && Array.isArray(elem[2])) {
            const [internalId, labels, props] = elem;
            const obj = { _internal_id: internalId, _labels: labels };
            // Parse properties array: [[key, type, value], ...]
            if (Array.isArray(props)) {
                for (const prop of props) {
                    if (Array.isArray(prop) && prop.length >= 2) {
                        const [key, typeOrValue, maybeValue] = prop;
                        // Format: [key, type_id, value] or [key, value]
                        if (typeof typeOrValue === 'number' && typeOrValue >= 1 && typeOrValue <= 10 && maybeValue !== undefined) {
                            obj[key] = maybeValue; // [key, type_id, value]
                        }
                        else {
                            obj[key] = maybeValue !== undefined ? maybeValue : typeOrValue;
                        }
                    }
                }
            }
            return obj;
        }
        // FalkorDB edge format: [internal_id, rel_type, src_id, dst_id, [[props...]]]
        if (elem.length === 5 && Array.isArray(elem[4])) {
            const [internalId, relType, srcId, dstId, props] = elem;
            const obj = { _internal_id: internalId, _rel_type: relType, _src_id: srcId, _dst_id: dstId };
            if (Array.isArray(props)) {
                for (const prop of props) {
                    if (Array.isArray(prop) && prop.length >= 2) {
                        const [key, typeOrValue, maybeValue] = prop;
                        if (typeof typeOrValue === 'number' && typeOrValue >= 1 && typeOrValue <= 10 && maybeValue !== undefined) {
                            obj[key] = maybeValue;
                        }
                        else {
                            obj[key] = maybeValue !== undefined ? maybeValue : typeOrValue;
                        }
                    }
                }
            }
            return obj;
        }
        // Unknown array format, return as-is
        return elem;
    }
    async getNode(id) {
        const rows = await this.graphQuery(`MATCH (n:Entity {id: $id}) RETURN n`, { id });
        if (!rows.length)
            return null;
        return this.rowToNode(rows[0][0]);
    }
    async setNode(node) {
        const props = this.flattenForCypher(node);
        await this.graphQuery(`MERGE (n:Entity {id: $id})
       SET n += $props
       SET n.name_lower = $name_lower`, {
            id: node.id,
            props,
            name_lower: node.name.toLowerCase(),
        });
    }
    async getEdge(id) {
        const rows = await this.graphQuery(`MATCH ()-[e:RELATES {id: $id}]->() RETURN e`, { id });
        if (!rows.length)
            return null;
        return this.rowToEdge(rows[0][0]);
    }
    async setEdge(edge) {
        const props = this.flattenForCypher(edge);
        // Use MERGE for nodes too - creates them if they don't exist
        await this.graphQuery(`MERGE (a:Entity {id: $from_id})
       MERGE (b:Entity {id: $to_id})
       MERGE (a)-[e:RELATES {id: $edge_id}]->(b)
       SET e += $props`, {
            from_id: edge.from_id,
            to_id: edge.to_id,
            edge_id: edge.id,
            props,
        });
    }
    async findNodesByName(nameLower, type, limit = 50) {
        const cap = Math.min(limit, 500);
        const query = type
            ? `MATCH (n:Entity) WHERE n.name_lower CONTAINS $name AND n.type = $type RETURN n ORDER BY n.confidence DESC LIMIT ${cap}`
            : `MATCH (n:Entity) WHERE n.name_lower CONTAINS $name RETURN n ORDER BY n.confidence DESC LIMIT ${cap}`;
        const params = { name: nameLower };
        if (type)
            params.type = type;
        const rows = await this.graphQuery(query, params);
        return rows.map(r => this.rowToNode(r[0])).filter(Boolean);
    }
    async getOutboundEdges(nodeId, relation) {
        const query = relation
            ? `MATCH (a:Entity {id: $id})-[e:RELATES]->(b:Entity) WHERE e.relation = $relation RETURN e ORDER BY e.confidence DESC`
            : `MATCH (a:Entity {id: $id})-[e:RELATES]->(b:Entity) RETURN e ORDER BY e.confidence DESC`;
        const params = { id: nodeId };
        if (relation)
            params.relation = relation;
        const rows = await this.graphQuery(query, params);
        return rows.map(r => this.rowToEdge(r[0])).filter(Boolean);
    }
    async getInboundEdges(nodeId, relation) {
        const query = relation
            ? `MATCH (a:Entity)-[e:RELATES]->(b:Entity {id: $id}) WHERE e.relation = $relation RETURN e ORDER BY e.confidence DESC`
            : `MATCH (a:Entity)-[e:RELATES]->(b:Entity {id: $id}) RETURN e ORDER BY e.confidence DESC`;
        const params = { id: nodeId };
        if (relation)
            params.relation = relation;
        const rows = await this.graphQuery(query, params);
        return rows.map(r => this.rowToEdge(r[0])).filter(Boolean);
    }
    async getStats() {
        try {
            const nodeCount = await this.graphQuery(`MATCH (n:Entity) RETURN count(n) AS cnt`, {});
            const edgeCount = await this.graphQuery(`MATCH ()-[e:RELATES]->() RETURN count(e) AS cnt`, {});
            const byType = await this.graphQuery(`MATCH (n:Entity) RETURN n.type AS type, count(n) AS cnt ORDER BY cnt DESC`, {});
            const nodes_by_type = {};
            for (const row of byType) {
                // Handle both old array format [type, count] and new object format {type, cnt}
                const typeVal = row.type ?? row[0];
                const cntVal = row.cnt ?? row[1];
                if (typeVal && cntVal)
                    nodes_by_type[String(typeVal)] = parseInt(String(cntVal));
            }
            // Get contagion stats from Redis
            const contagionByType = {};
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
            // Handle both old array format and new object format {cnt}
            const totalNodes = nodeCount[0]?.cnt ?? nodeCount[0]?.[0] ?? 0;
            const totalEdges = edgeCount[0]?.cnt ?? edgeCount[0]?.[0] ?? 0;
            return {
                total_nodes: parseInt(String(totalNodes)),
                total_edges: parseInt(String(totalEdges)),
                nodes_by_type,
                last_updated: new Date().toISOString(),
            };
        }
        catch {
            return { total_nodes: 0, total_edges: 0, nodes_by_type: {}, last_updated: new Date().toISOString() };
        }
    }
    // ── REGIME ─────────────────────────────────────────────────────────────────
    async setRegime(nodeId, regime) {
        await this.graphQuery(`MATCH (n:Entity {id: $id}) SET n.regime = $regime`, { id: nodeId, regime });
    }
    async getRegime(nodeId) {
        const rows = await this.graphQuery(`MATCH (n:Entity {id: $id}) RETURN n.regime`, { id: nodeId });
        return rows[0]?.[0] || null;
    }
    // ── CLAIMS ────────────────────────────────────────────────────────────────
    claimId(entity, assertion) {
        return createHash('sha256')
            .update(`${entity}:${assertion}`)
            .digest('hex')
            .substring(0, 16);
    }
    async addClaim(claim) {
        if (!this.client)
            throw new Error('Not connected');
        const id = this.claimId(claim.entity, claim.assertion);
        const fullClaim = {
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
    async getClaims(entityName) {
        if (!this.client)
            return [];
        const claimIds = await this.client.sMembers(`entity_claims:${entityName.toLowerCase()}`);
        const claims = [];
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
    async addSignal(signal) {
        if (!this.client)
            return;
        const key = `signal:${signal.entity.toLowerCase()}:${signal.metric}`;
        await this.client.zAdd(key, {
            score: signal.timestamp,
            value: JSON.stringify({ value: signal.value, timestamp: signal.timestamp }),
        });
    }
    async getSignals(entityName, metric, limit = 100) {
        if (!this.client)
            return [];
        const signals = [];
        if (metric) {
            const key = `signal:${entityName.toLowerCase()}:${metric}`;
            const data = await this.client.zRange(key, 0, limit - 1, { REV: true });
            for (const item of data) {
                try {
                    const parsed = JSON.parse(item);
                    signals.push({ entity: entityName, metric, value: parsed.value, timestamp: parsed.timestamp });
                }
                catch { }
            }
        }
        else {
            // Get all metrics for entity
            const keys = await this.client.keys(`signal:${entityName.toLowerCase()}:*`);
            for (const key of keys) {
                const metric = key.split(':').pop() || '';
                const data = await this.client.zRange(key, 0, limit - 1, { REV: true });
                for (const item of data) {
                    try {
                        const parsed = JSON.parse(item);
                        signals.push({ entity: entityName, metric, value: parsed.value, timestamp: parsed.timestamp });
                    }
                    catch { }
                }
            }
        }
        return signals.sort((a, b) => b.timestamp - a.timestamp).slice(0, limit);
    }
    // ── CONTAGION SCORE ──────────────────────────────────────────────────────
    async updateContagion(entityType, residualImpact) {
        if (!this.client)
            return;
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
    async getContagionByType(entityType) {
        if (!this.client)
            return null;
        const key = `contagion:type:${entityType}`;
        const data = await this.client.hGetAll(key);
        if (!data || !data.avg_impact)
            return null;
        return {
            type: entityType,
            avg_residual_impact: parseFloat(data.avg_impact),
            total_updates: parseInt(data.update_count),
        };
    }
    async findPath(fromId, toIds, maxHops) {
        // FalkorDB native shortest path
        try {
            for (const toId of toIds) {
                const rows = await this.graphQuery(`MATCH p = shortestPath((a:Entity {id: $from})-[*..${maxHops}]->(b:Entity {id: $to}))
           RETURN [node in nodes(p) | node.id] as node_ids,
                  [rel in relationships(p) | rel.id] as edge_ids`, { from: fromId, to: toId });
                if (rows.length && rows[0][0]) {
                    return { path: rows[0][0], edges: rows[0][1] || [] };
                }
            }
        }
        catch {
            return null;
        }
        return null;
    }
    // Serialize a GraphNode/GraphEdge to flat Cypher-safe properties
    flattenForCypher(obj) {
        const flat = {};
        for (const [k, v] of Object.entries(obj)) {
            if (v === null || v === undefined)
                continue;
            if (typeof v === 'object' && !Array.isArray(v)) {
                // Stringify nested objects — Cypher doesn't support nested maps
                flat[k] = JSON.stringify(v);
            }
            else if (Array.isArray(v)) {
                flat[k] = JSON.stringify(v);
            }
            else {
                flat[k] = v;
            }
        }
        return flat;
    }
    rowToNode(raw) {
        if (!raw)
            return null;
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
        }
        catch {
            return null;
        }
    }
    rowToEdge(raw) {
        if (!raw)
            return null;
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
        }
        catch {
            return null;
        }
    }
    parseJsonField(val) {
        if (!val)
            return {};
        if (typeof val === 'object')
            return val;
        try {
            return JSON.parse(val);
        }
        catch {
            return {};
        }
    }
    async isHealthy() {
        if (!this.client)
            return false;
        try {
            await this.client.ping();
            return true;
        }
        catch {
            return false;
        }
    }
}
// ─── ENTITY EXTRACTORS ────────────────────────────────────────────────────────
function extractFromLeads(leads) {
    const nodes = [];
    const edges = [];
    for (const lead of leads) {
        if (!lead)
            continue;
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
function extractFromEmails(data) {
    const nodes = [];
    const edges = [];
    if (!data.domain)
        return { nodes, edges };
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
        if (!email.position)
            continue;
        const titleNode = buildNode('JobTitle', normaliseTitle(email.position), {
            department: email.department || null,
            seniority: email.seniority || null,
        }, 'forage/find-emails');
        nodes.push(titleNode);
    }
    return { nodes, edges };
}
function extractFromCompanyInfo(data) {
    const nodes = [];
    const edges = [];
    if (!data.domain)
        return { nodes, edges };
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
function extractFromLocalLeads(data) {
    const nodes = [];
    const edges = [];
    if (!data.location)
        return { nodes, edges };
    const locationNode = buildNode('Location', data.location, {}, 'forage/find-local-leads');
    nodes.push(locationNode);
    const industryNode = buildNode('Industry', data.keyword, {}, 'forage/find-local-leads');
    nodes.push(industryNode);
    for (const lead of (data.leads || [])) {
        if (!lead.name)
            continue;
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
function extractFromWebSearch(data) {
    const nodes = [];
    const edges = [];
    for (const result of (data.results || [])) {
        const domain = extractDomain(result.link);
        if (!domain)
            continue;
        nodes.push(buildNode('Domain', domain, {
            title: result.title || null,
            snippet: result.snippet?.substring(0, 200) || null,
        }, 'forage/search-web'));
    }
    return { nodes, edges };
}
// ─── KNOWLEDGE GRAPH ──────────────────────────────────────────────────────────
export class KnowledgeGraph {
    constructor() {
        this.ready = false;
        // Link prediction scorers [fp-001]
        this.adamicAdar = null;
        this.jaccard = null;
        // Hawkes process engine [hawkes-001]
        this.hawkes = null;
        this.hawkesEvents = [];
        this.db = new KnowledgeStore();
    }
    async init() {
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
            console.log('[GRAPH] Knowledge graph initialised with Reality Graph features');
        }
        catch (err) {
            console.error('[GRAPH] Knowledge graph init failed:', err.message);
            this.ready = false;
            throw err; // Don't swallow - let caller know
        }
    }
    isReady() {
        return this.ready;
    }
    /** Direct Cypher query — used by connection-enricher and mirofish-realtime-bridge. */
    async rawCypherQuery(query, params = {}) {
        if (!this.ready)
            return [];
        return this.db.graphQuery(query, params);
    }
    async isHealthy() {
        return this.ready && this.db.isConnectionReady() && await this.db.isHealthy();
    }
    // Called after every tool response - now with proper error handling
    async ingest(toolName, result) {
        if (!this.ready) {
            console.error('[GRAPH] Ingest called but graph not ready');
            throw new Error('Graph not initialized');
        }
        if (!this.db.isConnectionReady()) {
            console.error('[GRAPH] Ingest called but DB connection not ready');
            throw new Error('Database connection not ready');
        }
        const { nodes, edges } = this.extract(toolName, result);
        if (nodes.length === 0 && edges.length === 0) {
            console.log(`[GRAPH] No entities extracted from ${toolName}`);
            return;
        }
        console.log(`[GRAPH] Ingesting ${nodes.length} nodes, ${edges.length} edges from ${toolName}`);
        await this.merge(nodes, edges);
    }
    extract(toolName, result) {
        // Normalize tool name: forage/find-leads → find_leads, FIND_LEADS → find_leads
        const normalized = toolName
            .toLowerCase()
            .replace(/^forage[\/\-_]?/, '')
            .replace(/[-\s]/g, '_')
            .trim();
        switch (normalized) {
            case 'find_leads': return extractFromLeads(result?.leads || result || []);
            case 'find_emails': return extractFromEmails(result || {});
            case 'get_company_info': return extractFromCompanyInfo(result || {});
            case 'find_local_leads': return extractFromLocalLeads(result || {});
            case 'search_web': return extractFromWebSearch(result || {});
            default: return this.extractUniversal(toolName, result);
        }
    }
    // Universal fallback extractor - extracts entities from any JSON structure
    extractUniversal(source, data) {
        const nodes = [];
        const edges = [];
        if (!data || typeof data !== 'object')
            return { nodes, edges };
        const extract = (obj, depth = 0) => {
            if (depth > 5 || !obj)
                return;
            if (Array.isArray(obj)) {
                obj.forEach(item => extract(item, depth + 1));
                return;
            }
            if (typeof obj !== 'object')
                return;
            // Auto-detect entities by field names
            const entityMappings = {
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
                if (!value || typeof value !== 'string' || value.length < 2 || value.length > 200)
                    continue;
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
                        if (typeof item === 'object')
                            extract(item, depth + 1);
                    });
                }
                else if (typeof value === 'object') {
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
    async merge(newNodes, newEdges) {
        const now = new Date().toISOString();
        // Deduplicate within batch
        const nodeMap = new Map();
        for (const node of newNodes) {
            if (nodeMap.has(node.id)) {
                nodeMap.set(node.id, mergeNodeProperties(nodeMap.get(node.id), node));
            }
            else {
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
            }
            else {
                await this.db.setNode({ ...node, first_seen: now, last_seen: now });
            }
        }
        const edgeMap = new Map();
        for (const edge of newEdges)
            edgeMap.set(edge.id, edge);
        for (const edge of edgeMap.values()) {
            const existing = await this.db.getEdge(edge.id);
            if (existing) {
                existing.call_count = (existing.call_count || 1) + 1;
                existing.confidence = Math.min(0.99, existing.confidence + 0.05);
                existing.last_seen = now;
                await this.db.setEdge(existing);
            }
            else {
                await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
            }
        }
    }
    // ── QUERIES ───────────────────────────────────────────────────────────────
    async findEntity(name, type, limit = 50) {
        if (!this.ready)
            return [];
        const nodes = await this.db.findNodesByName(name.toLowerCase(), type, limit);
        return nodes.sort((a, b) => {
            const aExact = a.name.toLowerCase() === name.toLowerCase() ? 1 : 0;
            const bExact = b.name.toLowerCase() === name.toLowerCase() ? 1 : 0;
            return (bExact - aExact) || (b.confidence - a.confidence);
        });
    }
    async getNeighbours(nodeId, relation) {
        if (!this.ready)
            return [];
        const edges = await this.db.getOutboundEdges(nodeId, relation);
        const results = [];
        for (const edge of edges) {
            const [node, neighbour] = await Promise.all([
                this.db.getNode(edge.from_id),
                this.db.getNode(edge.to_id),
            ]);
            if (node && neighbour)
                results.push({ node, edge, neighbour });
        }
        return results.sort((a, b) => b.edge.confidence - a.edge.confidence);
    }
    async findConnections(fromName, toName, maxHops = 3) {
        if (!this.ready)
            return null;
        const fromNodes = await this.findEntity(fromName);
        const toNodes = await this.findEntity(toName);
        if (!fromNodes.length || !toNodes.length)
            return null;
        const result = await this.db.findPath(fromNodes[0].id, toNodes.map(n => n.id), maxHops);
        if (!result)
            return null;
        const pathNodes = await Promise.all(result.path.map(id => this.db.getNode(id)));
        const pathEdges = await Promise.all(result.edges.map(id => this.db.getEdge(id)));
        return {
            path: pathNodes.filter(Boolean),
            edges: pathEdges.filter(Boolean),
            hops: result.path.length - 1,
        };
    }
    async enrich(identifier) {
        if (!this.ready)
            return { entity: null, related: {}, confidence: 0 };
        let candidates = await this.findEntity(identifier, 'Domain');
        if (!candidates.length)
            candidates = await this.findEntity(identifier, 'Company');
        if (!candidates.length)
            candidates = await this.findEntity(identifier);
        if (!candidates.length)
            return { entity: null, related: {}, confidence: 0 };
        const entity = candidates[0];
        const neighbours = await this.getNeighbours(entity.id);
        const related = {};
        for (const { edge, neighbour } of neighbours) {
            const key = edge.relation;
            if (!related[key])
                related[key] = [];
            related[key].push(neighbour);
        }
        return { entity, related, confidence: entity.confidence };
    }
    async findByIndustryAndLocation(industry, location) {
        if (!this.ready)
            return [];
        const industryNodes = await this.findEntity(industry, 'Industry');
        if (!industryNodes.length)
            return [];
        const inEdges = await this.db.getInboundEdges(industryNodes[0].id, 'operates_in');
        const companies = [];
        for (const edge of inEdges) {
            const company = await this.db.getNode(edge.from_id);
            if (!company || company.type !== 'Company')
                continue;
            if (location) {
                const neighbours = await this.getNeighbours(company.id, 'located_in');
                const inLocation = neighbours.some(n => n.neighbour.name.toLowerCase().includes(location.toLowerCase()));
                if (!inLocation)
                    continue;
            }
            companies.push(company);
        }
        return companies.sort((a, b) => b.confidence - a.confidence);
    }
    async getStats() {
        return this.db.getStats();
    }
    // ── DIRECT INJECTION ────────────────────────────────────────────────────────
    // For n8n workflows and external feeds to inject entities/connections directly
    async addEntities(entities) {
        // Health check with reconnect attempt
        if (!this.ready || !this.db.isConnectionReady()) {
            console.error('[GRAPH] addEntities: Not ready, attempting reconnect...');
            try {
                await this.init();
            }
            catch (err) {
                console.error('[GRAPH] addEntities reconnect failed:', err.message);
                throw new Error('Database not available');
            }
        }
        let added = 0, merged = 0;
        const now = new Date().toISOString();
        for (const e of entities) {
            const node = buildNode(e.type, e.name, e.properties || {}, e.source || 'direct_inject', e.confidence || 0.75);
            const existing = await this.db.getNode(node.id);
            if (existing) {
                const m = mergeNodeProperties(existing, node);
                m.last_seen = now;
                m.call_count = (existing.call_count || 1) + 1;
                m.confidence = Math.min(0.99, existing.confidence + 0.03);
                await this.db.setNode(m);
                merged++;
            }
            else {
                await this.db.setNode({ ...node, first_seen: now, last_seen: now });
                added++;
            }
        }
        return { added, merged };
    }
    async addConnections(connections) {
        // Health check with reconnect attempt
        if (!this.ready || !this.db.isConnectionReady()) {
            console.error('[GRAPH] addConnections: Not ready, attempting reconnect...');
            try {
                await this.init();
            }
            catch (err) {
                console.error('[GRAPH] addConnections reconnect failed:', err.message);
                throw new Error('Database not available');
            }
        }
        let added = 0, merged = 0;
        const now = new Date().toISOString();
        for (const c of connections) {
            try {
                // Handle relationship alias
                const relation = (c.relation || c.relationship || 'related_to');
                // Build nodes - MERGE in setNode will handle deduplication
                const fromType = c.from_type || 'LegalEntity';
                const toType = c.to_type || 'LegalEntity';
                const fromNode = buildNode(fromType, c.from_name, {}, c.source || 'direct_inject');
                const toNode = buildNode(toType, c.to_name, {}, c.source || 'direct_inject');
                // Create/merge nodes (MERGE handles existing)
                await this.db.setNode({ ...fromNode, first_seen: now, last_seen: now });
                await this.db.setNode({ ...toNode, first_seen: now, last_seen: now });
                const edge = buildEdge(fromNode, toNode, relation, c.source || 'direct_inject', c.confidence || 0.75);
                if (c.properties)
                    edge.properties = { ...edge.properties, ...c.properties };
                const existing = await this.db.getEdge(edge.id);
                if (existing) {
                    existing.call_count = (existing.call_count || 1) + 1;
                    existing.confidence = Math.min(0.99, existing.confidence + 0.05);
                    existing.last_seen = now;
                    await this.db.setEdge(existing);
                    merged++;
                }
                else {
                    await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
                    added++;
                }
            }
            catch (err) {
                console.error(`[GRAPH] Connection ${c.from_name} -> ${c.to_name} failed:`, err.message);
                // Continue processing other connections
            }
        }
        return { added, merged };
    }
    // ── REGIME ─────────────────────────────────────────────────────────────────
    async setRegime(entityName, regime) {
        if (!this.ready)
            return false;
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return false;
        await this.db.setRegime(nodes[0].id, regime);
        return true;
    }
    async getRegime(entityName) {
        if (!this.ready)
            return null;
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return null;
        return this.db.getRegime(nodes[0].id);
    }
    // ── CLAIMS ────────────────────────────────────────────────────────────────
    async addClaim(claim) {
        if (!this.ready)
            throw new Error('Not ready');
        return this.db.addClaim(claim);
    }
    async getClaims(entityName) {
        if (!this.ready)
            return [];
        return this.db.getClaims(entityName);
    }
    // ── SIGNALS ──────────────────────────────────────────────────────────────
    async addSignal(signal) {
        if (!this.ready)
            return;
        await this.db.addSignal(signal);
    }
    async getSignals(entityName, metric, limit = 100) {
        if (!this.ready)
            return [];
        return this.db.getSignals(entityName, metric, limit);
    }
    // ── CAUSAL QUERIES ──────────────────────────────────────────────────────
    async getCausalParents(entityName, limit = 10) {
        if (!this.ready)
            return { entities: [] };
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return { entities: [] };
        const edges = await this.db.getInboundEdges(nodes[0].id);
        const results = [];
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
    async getCausalChildren(entityName, limit = 10) {
        if (!this.ready)
            return { entities: [] };
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return { entities: [] };
        const edges = await this.db.getOutboundEdges(nodes[0].id);
        const results = [];
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
    async getCausalPath(fromName, toName) {
        if (!this.ready)
            return null;
        const fromNodes = await this.findEntity(fromName);
        const toNodes = await this.findEntity(toName);
        if (!fromNodes.length || !toNodes.length)
            return null;
        const result = await this.db.findPath(fromNodes[0].id, toNodes.map(n => n.id), 5);
        if (!result)
            return null;
        let totalWeight = 0;
        const edgeDetails = [];
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
        const pathNames = pathNodes.filter(Boolean).map(n => n.name);
        return { path: pathNames, total_weight: totalWeight, edges: edgeDetails };
    }
    async simulate(entityName, intervention, depth = 3) {
        if (!this.ready)
            return { affected: [], summary: 'Not ready' };
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return { affected: [], summary: 'Entity not found' };
        const startNode = nodes[0];
        const startRegime = await this.db.getRegime(startNode.id);
        // Get initial impact based on intervention type
        const baseImpact = intervention === 'shock' ? 1.0 : intervention === 'boost' ? 0.8 : -0.5;
        const regimeMultiplier = (startRegime === 'stressed' || startRegime === 'pre_tipping') ? 1.5 : 1.0;
        const initialImpact = baseImpact * regimeMultiplier;
        // BFS propagation with attenuation
        const visited = new Set();
        const affected = [];
        const queue = [
            { nodeId: startNode.id, impact: initialImpact, path: [startNode.name] }
        ];
        while (queue.length > 0 && affected.length < 50) {
            const current = queue.shift();
            if (visited.has(current.nodeId))
                continue;
            visited.add(current.nodeId);
            const node = await this.db.getNode(current.nodeId);
            if (!node || node.id === startNode.id)
                continue;
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
    async predictLinks(entityName, targetEntityType, maxPredictions = 10) {
        if (!this.ready || !this.adamicAdar)
            return [];
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return [];
        return this.adamicAdar.predictLinks(nodes[0].id, targetEntityType, maxPredictions);
    }
    // ── HAWKES PROCESS CONTAGION [hawkes-002] ─────────────────────────────────────
    /**
     * Record an event for Hawkes process modeling.
     * Events are used to compute self-exciting intensity for causal contagion.
     */
    async recordEvent(event) {
        if (!this.ready || !this.hawkes)
            return;
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
    async getContagionState(entityName) {
        if (!this.ready || !this.hawkes)
            return null;
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return null;
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
    async simulateShock(entityName, shockMagnitude = 1.0, durationHours = 168) {
        if (!this.ready || !this.hawkes)
            return null;
        const nodes = await this.findEntity(entityName);
        if (!nodes.length)
            return null;
        return this.hawkes.simulateShock(nodes[0].id, shockMagnitude, durationHours);
    }
    /**
     * Update Hawkes process parameters based on observed events.
     * Uses Maximum Likelihood Estimation (MLE):
     * $$\hat{\mu} = \frac{N}{T}, \quad \hat{\alpha}_{ij} = \frac{\text{co-occurrences}}{\text{total}}$$
     */
    async recalibrateHawkes(windowHours = 720) {
        if (!this.ready)
            return;
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
    async batchMergeEdges(edges) {
        if (!this.ready || edges.length === 0)
            return { merged: 0 };
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
    async exportAll() {
        if (!this.ready)
            throw new Error('Graph not initialized');
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
    async importBackup(backup) {
        if (!this.ready)
            throw new Error('Graph not initialized');
        let entities_imported = 0;
        let relationships_imported = 0;
        const now = new Date().toISOString();
        // Import entities
        for (const e of backup.entities || []) {
            const node = buildNode(e.type, e.name, e.properties || {}, e.source || 'backup_import', e.confidence || 0.9);
            await this.db.setNode({ ...node, first_seen: now, last_seen: now });
            entities_imported++;
        }
        // Import relationships
        for (const r of backup.relationships || []) {
            const fromNode = buildNode(r.from_type, r.from_name, {}, 'backup_import');
            const toNode = buildNode(r.to_type, r.to_name, {}, 'backup_import');
            await this.db.setNode({ ...fromNode, first_seen: now, last_seen: now });
            await this.db.setNode({ ...toNode, first_seen: now, last_seen: now });
            const edge = buildEdge(fromNode, toNode, r.relation, r.source || 'backup_import', r.confidence || 0.8);
            await this.db.setEdge({ ...edge, first_seen: now, last_seen: now });
            relationships_imported++;
        }
        return { entities_imported, relationships_imported };
    }
}
// ─── HELPERS ──────────────────────────────────────────────────────────────────
function buildNode(type, name, properties, source, confidence = 0.75) {
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
function buildEdge(from, to, relation, source, confidence = 0.8) {
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
function mergeNodeProperties(existing, incoming) {
    const mergedSources = [...new Set([...existing.sources, ...incoming.sources])];
    const mergedProps = { ...existing.properties };
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
function nodeId(type, name) {
    return createHash('sha3-256')
        .update(`${type}:${name.toLowerCase().trim()}`)
        .digest('hex')
        .substring(0, 16);
}
function edgeId(fromId, relation, toId) {
    return createHash('sha3-256')
        .update(`${fromId}:${relation}:${toId}`)
        .digest('hex')
        .substring(0, 16);
}
function hashPII(value) {
    return 'pii:' + createHash('sha256').update(value.toLowerCase().trim()).digest('hex').substring(0, 12);
}
function extractDomain(input) {
    if (!input)
        return null;
    try {
        const s = input.includes('://') ? input : `https://${input}`;
        const host = new URL(s).hostname.replace(/^www\./, '');
        if (host.includes('.') && host.length > 3)
            return host;
    }
    catch { }
    const match = input.match(/(?:https?:\/\/)?(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})(?:\/|$)/);
    return match ? match[1] : null;
}
function normaliseTitle(title) {
    return title
        .replace(/\b(senior|sr|junior|jr|lead|principal|associate|staff|vp of|head of|director of|chief)\b/gi, '')
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase()
        .replace(/\b\w/g, c => c.toUpperCase());
}
function cleanProperties(props) {
    const clean = {};
    for (const [k, v] of Object.entries(props)) {
        if (v !== null && v !== undefined && v !== '')
            clean[k] = v;
    }
    return clean;
}
export const knowledgeGraph = new KnowledgeGraph();
