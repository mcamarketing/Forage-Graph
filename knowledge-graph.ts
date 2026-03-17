/**
 * Knowledge Graph Service — knowledge-graph.ts
 *
 * Universal Causal Intelligence Graph backed by FalkorDB/Redis.
 *
 * Models the full web of global relationships: companies, people, governments,
 * infrastructure, commodities, financial instruments, conflicts, technologies,
 * and the causal edges that connect them across domains and time.
 *
 * Every fact carries: confidence, provenance, valid_from, valid_until.
 * Every relationship carries: causal_weight, mechanism, lag_days, effect_size.
 *
 * Designed for Railway / VPS deployment. All data persists across restarts.
 */

import { createClient, RedisClientType } from 'redis';

// ─── ENTITY TYPES ─────────────────────────────────────────────────────────────

export type EntityType =
  // Economic
  | 'company' | 'subsidiary' | 'spv' | 'financial_instrument' | 'central_bank' | 'commodity_flow'
  // Political
  | 'country' | 'government' | 'government_body' | 'political_party' | 'international_agreement' | 'regional_bloc'
  // People & Orgs
  | 'person' | 'ngo' | 'university' | 'religious_institution' | 'social_movement'
  // Physical & Infrastructure
  | 'location' | 'port' | 'pipeline' | 'submarine_cable' | 'power_plant' | 'mine' | 'farmland'
  // Technical
  | 'technology' | 'oss_library' | 'supply_chain' | 'dependency'
  // Events
  | 'natural_event' | 'conflict_event' | 'political_event' | 'economic_event'
  // Legacy / B2B
  | 'industry' | 'product';

// ─── INTERFACES ───────────────────────────────────────────────────────────────

export interface Entity {
  id: string;
  name: string;
  type: EntityType;
  confidence: number;
  call_count: number;
  properties: Record<string, any>;
  sources: string[];
  provenance: string[];
  first_seen: string;
  last_seen: string;
  valid_from?: string;
  valid_until?: string;
}

export interface Relationship {
  id: string;
  from_id: string;
  to_id: string;
  from_name: string;
  to_name: string;
  relation: string;
  confidence: number;
  causal_weight?: number;
  mechanism?: string;
  lag_days?: number;
  effect_size?: number;
  valid_from?: string;
  valid_until?: string;
  provenance: string[];
  first_seen: string;
  last_seen: string;
}

// ─── KNOWLEDGE GRAPH CLASS ─────────────────────────────────────────────────────

class KnowledgeGraph {
  private client: RedisClientType | null = null;
  private ready: boolean = false;

  // ─── LIFECYCLE ────────────────────────────────────────────────────────────────

  async init(): Promise<void> {
    const url = process.env.FALKORDB_URL || process.env.REDIS_URL || 'redis://localhost:6379';
    console.log('Connecting to FalkorDB at:', url);

    try {
      const parsed = new URL(url);
      console.log('URL parsed - Protocol:', parsed.protocol, 'Host:', parsed.hostname, 'Port:', parsed.port);
    } catch (e) {
      console.error('Invalid URL format:', url);
    }

    this.client = createClient({
      url,
      socket: {
        reconnectStrategy: (retries) => {
          console.log(`Reconnect attempt ${retries}`);
          return Math.min(retries * 50, 500);
        },
        connectTimeout: 10000,
      }
    });

    this.client.on('error', (err) => console.error('KV error:', err.message || err));
    this.client.on('connect', () => console.log('Redis client connected'));
    this.client.on('ready', () => { console.log('Redis client ready'); this.ready = true; });
    this.client.on('end', () => { console.log('Redis client disconnected'); this.ready = false; });

    try {
      await this.client.connect();
      console.log('FalkorDB connected successfully');
      const ping = await this.client.ping();
      console.log('Ping result:', ping);
    } catch (err: any) {
      console.error('Failed to connect to FalkorDB:', err.message || err);
      throw err;
    }

    await this.ensureIndexes();
    console.log('Knowledge Graph initialized');
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.quit();
      this.ready = false;
      this.client = null;
    }
  }

  isHealthy(): boolean {
    return this.ready && (this.client?.isReady ?? false);
  }

  // ─── INDEXES ──────────────────────────────────────────────────────────────────

  private async ensureIndexes(): Promise<void> {
    // Index structure:
    //   index:type:{type}       -> Set of entity IDs
    //   index:name:{name}       -> Set of entity IDs
    //   index:country:{code}    -> Set of entity IDs
    //   rel:from:{id}           -> Set of relationship IDs
    //   rel:to:{id}             -> Set of relationship IDs
    //   rel:type:{relation}     -> Set of relationship IDs
    console.log('Indexes ensured');
  }

  // ─── ENTITY OPERATIONS ───────────────────────────────────────────────────────

  async upsertEntity(
    name: string,
    type: EntityType,
    properties: Record<string, any> = {},
    source: string,
    confidence: number = 0.8,
    options: {
      provenance?: string[];
      valid_from?: string;
      valid_until?: string;
    } = {}
  ): Promise<Entity> {
    this.assertInitialized();

    const id = this.entityId(name, type);
    const now = new Date().toISOString();

    const exists = await this.client!.hGetAll(`entity:${id}`);
    const isNew = Object.keys(exists).length === 0;

    const existingProvenance: string[] = isNew ? [] : this.safeParseJSON(exists.provenance, []);
    const newProvenance = options.provenance || [source];
    const mergedProvenance = Array.from(new Set([...existingProvenance, ...newProvenance]));

    const entity: Entity = {
      id,
      name,
      type,
      confidence: isNew ? confidence : Math.max(parseFloat(exists.confidence || '0'), confidence),
      call_count: isNew ? 1 : parseInt(exists.call_count || '0', 10) + 1,
      properties: isNew ? properties : { ...this.safeParseJSON(exists.properties, {}), ...properties },
      sources: isNew ? [source] : Array.from(new Set([...this.safeParseJSON(exists.sources, []), source])),
      provenance: mergedProvenance,
      first_seen: isNew ? now : exists.first_seen || now,
      last_seen: now,
      valid_from: options.valid_from,
      valid_until: options.valid_until,
    };

    const multi = this.client!.multi();

    multi.hSet(`entity:${id}`, {
      name: entity.name,
      type: entity.type,
      confidence: entity.confidence.toString(),
      call_count: entity.call_count.toString(),
      properties: JSON.stringify(entity.properties),
      sources: JSON.stringify(entity.sources),
      provenance: JSON.stringify(entity.provenance),
      first_seen: entity.first_seen,
      last_seen: entity.last_seen,
      ...(entity.valid_from ? { valid_from: entity.valid_from } : {}),
      ...(entity.valid_until ? { valid_until: entity.valid_until } : {}),
    });

    multi.sAdd(`index:type:${type}`, id);
    multi.sAdd(`index:name:${name.toLowerCase()}`, id);

    const country = properties.country || properties.iso_code;
    if (country) {
      multi.sAdd(`index:country:${String(country).toLowerCase()}`, id);
    }

    await multi.exec();

    console.log(`Upserted ${type}: ${name} (calls: ${entity.call_count})`);
    return entity;
  }

  async findEntity(name: string, type?: EntityType): Promise<Entity[]> {
    this.assertInitialized();

    const nameIds = await this.client!.sMembers(`index:name:${name.toLowerCase()}`);
    let ids = new Set(nameIds);

    if (type) {
      const typeIds = await this.client!.sMembers(`index:type:${type}`);
      const typeSet = new Set(typeIds);
      ids = new Set([...ids].filter(id => typeSet.has(id)));
    }

    if (ids.size === 0) return [];

    const multi = this.client!.multi();
    for (const id of ids) multi.hGetAll(`entity:${id}`);

    const results = await multi.exec() as Record<string, string>[];
    const idArray = Array.from(ids);
    const entities: Entity[] = [];

    for (let i = 0; i < results.length; i++) {
      const data = results[i];
      if (data && Object.keys(data).length > 0) {
        entities.push(this.hydrateEntity(idArray[i], data));
      }
    }

    return entities.sort((a, b) => b.confidence - a.confidence);
  }

  async findByType(type: EntityType, limit: number = 100): Promise<Entity[]> {
    this.assertInitialized();

    const ids = await this.client!.sMembers(`index:type:${type}`);
    if (ids.length === 0) return [];

    const slice = ids.slice(0, limit);
    const multi = this.client!.multi();
    for (const id of slice) multi.hGetAll(`entity:${id}`);

    const results = await multi.exec() as Record<string, string>[];
    const entities: Entity[] = [];

    for (let i = 0; i < results.length; i++) {
      const data = results[i];
      if (data && Object.keys(data).length > 0) {
        entities.push(this.hydrateEntity(slice[i], data));
      }
    }

    return entities.sort((a, b) => b.confidence - a.confidence);
  }

  async findByCountry(countryCode: string, type?: EntityType): Promise<Entity[]> {
    this.assertInitialized();

    const countryIds = await this.client!.sMembers(`index:country:${countryCode.toLowerCase()}`);
    let ids = new Set(countryIds);

    if (type) {
      const typeIds = await this.client!.sMembers(`index:type:${type}`);
      const typeSet = new Set(typeIds);
      ids = new Set([...ids].filter(id => typeSet.has(id)));
    }

    if (ids.size === 0) return [];

    const multi = this.client!.multi();
    const idArray = Array.from(ids);
    for (const id of idArray) multi.hGetAll(`entity:${id}`);

    const results = await multi.exec() as Record<string, string>[];
    const entities: Entity[] = [];

    for (let i = 0; i < results.length; i++) {
      const data = results[i];
      if (data && Object.keys(data).length > 0) {
        entities.push(this.hydrateEntity(idArray[i], data));
      }
    }

    return entities.sort((a, b) => b.confidence - a.confidence);
  }

  async enrich(identifier: string): Promise<{
    entity: Entity | null;
    related: Record<string, Entity[]>;
    confidence: number;
    causal_paths: Array<{ relation: string; entity: Entity; causal_weight: number; mechanism?: string }>;
  }> {
    this.assertInitialized();

    let entities = await this.findEntity(identifier);

    if (entities.length === 0 && identifier.includes('.')) {
      const domain = identifier.toLowerCase().replace(/^www\./, '').split('/')[0];
      entities = await this.findEntity(domain);
    }

    if (entities.length === 0) {
      return { entity: null, related: {}, confidence: 0, causal_paths: [] };
    }

    const main = entities[0];
    const related: Record<string, Entity[]> = {};
    const causal_paths: Array<{ relation: string; entity: Entity; causal_weight: number; mechanism?: string }> = [];

    const [relIdsFrom, relIdsTo] = await Promise.all([
      this.client!.sMembers(`rel:from:${main.id}`),
      this.client!.sMembers(`rel:to:${main.id}`)
    ]);

    const allRelIds = Array.from(new Set([...relIdsFrom, ...relIdsTo]));

    if (allRelIds.length > 0) {
      const relMulti = this.client!.multi();
      for (const relId of allRelIds) relMulti.hGetAll(`relationship:${relId}`);
      const relResults = await relMulti.exec() as Record<string, string>[];

      const entityIdsToFetch = new Set<string>();
      const validRelations: Array<{ otherId: string; relData: Record<string, string> }> = [];

      for (const relData of relResults) {
        if (!relData || Object.keys(relData).length === 0) continue;
        const isFrom = relData.from_id === main.id;
        const otherId = isFrom ? relData.to_id : relData.from_id;
        entityIdsToFetch.add(otherId);
        validRelations.push({ otherId, relData });
      }

      if (entityIdsToFetch.size > 0) {
        const entityMulti = this.client!.multi();
        const entityIdsArray = Array.from(entityIdsToFetch);
        for (const id of entityIdsArray) entityMulti.hGetAll(`entity:${id}`);

        const entityResults = await entityMulti.exec() as Record<string, string>[];
        const entityMap = new Map<string, Entity>();

        for (let i = 0; i < entityResults.length; i++) {
          const data = entityResults[i];
          if (data && Object.keys(data).length > 0) {
            entityMap.set(entityIdsArray[i], this.hydrateEntity(entityIdsArray[i], data));
          }
        }

        for (const { otherId, relData } of validRelations) {
          const otherEntity = entityMap.get(otherId);
          if (!otherEntity) continue;

          const relation = relData.relation;
          if (!related[relation]) related[relation] = [];
          related[relation].push(otherEntity);

          const causalWeight = parseFloat(relData.causal_weight || '0');
          if (causalWeight > 0 || relData.mechanism) {
            causal_paths.push({
              relation,
              entity: otherEntity,
              causal_weight: causalWeight,
              mechanism: relData.mechanism,
            });
          }
        }
      }
    }

    const relCount = allRelIds.length;
    const confidence = Math.min(0.3 + relCount * 0.1, 1.0);

    return { entity: main, related, confidence, causal_paths };
  }

  async findConnections(
    from: string,
    to: string,
    maxHops: number = 3
  ): Promise<{ hops: number; path: Entity[]; edges: Relationship[] } | null> {
    this.assertInitialized();

    const fromEntities = await this.findEntity(from);
    if (fromEntities.length === 0) return null;

    const targetEntities = await this.findEntity(to);
    if (targetEntities.length === 0) return null;

    const startId = fromEntities[0].id;
    const targetId = targetEntities[0].id;

    if (startId === targetId) {
      return { hops: 0, path: [fromEntities[0]], edges: [] };
    }

    const queue: Array<{ id: string; hops: number; path: string[]; edges: string[] }> = [];
    const visited = new Set<string>();

    queue.push({ id: startId, hops: 0, path: [startId], edges: [] });
    visited.add(startId);

    while (queue.length > 0) {
      const current = queue.shift()!;

      if (current.id === targetId) {
        return this.hydratePathAndEdges(current.path, current.edges, current.hops);
      }

      if (current.hops >= maxHops) continue;

      // Traverse both directions for undirected-style search
      const [outbound, inbound] = await Promise.all([
        this.client!.sMembers(`rel:from:${current.id}`),
        this.client!.sMembers(`rel:to:${current.id}`)
      ]);

      const allNeighborRelIds = Array.from(new Set([...outbound, ...inbound]));
      if (allNeighborRelIds.length === 0) continue;

      const multi = this.client!.multi();
      for (const relId of allNeighborRelIds) multi.hGetAll(`relationship:${relId}`);
      const relResults = await multi.exec() as Record<string, string>[];

      for (let i = 0; i < relResults.length; i++) {
        const relData = relResults[i];
        if (!relData || Object.keys(relData).length === 0) continue;

        const isFrom = relData.from_id === current.id;
        const nextId = isFrom ? relData.to_id : relData.from_id;
        const relId = allNeighborRelIds[i];

        if (!visited.has(nextId)) {
          visited.add(nextId);
          queue.push({
            id: nextId,
            hops: current.hops + 1,
            path: [...current.path, nextId],
            edges: [...current.edges, relId]
          });
        }
      }
    }

    return null;
  }

  private async hydratePathAndEdges(
    pathIds: string[],
    edgeIds: string[],
    hops: number
  ): Promise<{ hops: number; path: Entity[]; edges: Relationship[] }> {
    const path: Entity[] = [];
    const edges: Relationship[] = [];

    if (pathIds.length > 0) {
      const multi = this.client!.multi();
      for (const id of pathIds) multi.hGetAll(`entity:${id}`);
      const results = await multi.exec() as Record<string, string>[];
      for (let i = 0; i < results.length; i++) {
        const data = results[i];
        if (data && Object.keys(data).length > 0) {
          path.push(this.hydrateEntity(pathIds[i], data));
        }
      }
    }

    if (edgeIds.length > 0) {
      const multi = this.client!.multi();
      for (const id of edgeIds) multi.hGetAll(`relationship:${id}`);
      const results = await multi.exec() as Record<string, string>[];
      for (let i = 0; i < results.length; i++) {
        const data = results[i];
        if (data && Object.keys(data).length > 0) {
          edges.push(this.hydrateRelationship(edgeIds[i], data));
        }
      }
    }

    return { hops, path, edges };
  }

  async addRelationship(
    from: Entity,
    to: Entity,
    relation: string,
    confidence: number = 0.8,
    options: {
      causal_weight?: number;
      mechanism?: string;
      lag_days?: number;
      effect_size?: number;
      valid_from?: string;
      valid_until?: string;
      provenance?: string[];
    } = {}
  ): Promise<Relationship> {
    this.assertInitialized();

    const id = `${from.id}-${relation}-${to.id}`;
    const now = new Date().toISOString();

    const exists = await this.client!.hGetAll(`relationship:${id}`);
    const isNew = Object.keys(exists).length === 0;

    const existingProvenance: string[] = isNew ? [] : this.safeParseJSON(exists.provenance, []);
    const mergedProvenance = Array.from(new Set([...existingProvenance, ...(options.provenance || [])]));

    const rel: Relationship = {
      id,
      from_id: from.id,
      to_id: to.id,
      from_name: from.name,
      to_name: to.name,
      relation,
      confidence: isNew ? confidence : Math.max(parseFloat(exists.confidence || '0'), confidence),
      causal_weight: options.causal_weight,
      mechanism: options.mechanism,
      lag_days: options.lag_days,
      effect_size: options.effect_size,
      valid_from: options.valid_from,
      valid_until: options.valid_until,
      provenance: mergedProvenance,
      first_seen: isNew ? now : exists.first_seen || now,
      last_seen: now,
    };

    const multi = this.client!.multi();

    const relHash: Record<string, string> = {
      from_id: rel.from_id,
      to_id: rel.to_id,
      from_name: rel.from_name,
      to_name: rel.to_name,
      relation: rel.relation,
      confidence: rel.confidence.toString(),
      provenance: JSON.stringify(rel.provenance),
      first_seen: rel.first_seen,
      last_seen: rel.last_seen,
    };

    if (rel.causal_weight !== undefined) relHash.causal_weight = rel.causal_weight.toString();
    if (rel.mechanism) relHash.mechanism = rel.mechanism;
    if (rel.lag_days !== undefined) relHash.lag_days = rel.lag_days.toString();
    if (rel.effect_size !== undefined) relHash.effect_size = rel.effect_size.toString();
    if (rel.valid_from) relHash.valid_from = rel.valid_from;
    if (rel.valid_until) relHash.valid_until = rel.valid_until;

    multi.hSet(`relationship:${id}`, relHash);
    multi.sAdd(`rel:from:${from.id}`, id);
    multi.sAdd(`rel:to:${to.id}`, id);
    multi.sAdd(`rel:type:${relation}`, id);

    await multi.exec();

    console.log(`Relationship: ${from.name} -[${relation}]-> ${to.name}`);
    return rel;
  }

  // ─── INGEST ───────────────────────────────────────────────────────────────────

  async ingest(toolName: string, result: any): Promise<void> {
    if (!this.client) {
      console.error('Cannot ingest — KnowledgeGraph not initialized');
      return;
    }

    console.log(`Ingesting from ${toolName}...`);

    try {
      const entities = this.extractEntities(toolName, result);

      const savedEntities: Entity[] = [];
      for (const e of entities) {
        const saved = await this.upsertEntity(
          e.name, e.type, e.properties, toolName, e.confidence,
          { provenance: [toolName] }
        );
        savedEntities.push(saved);
      }

      for (let i = 0; i < savedEntities.length; i++) {
        for (let j = i + 1; j < savedEntities.length; j++) {
          const a = savedEntities[i];
          const b = savedEntities[j];
          const { relation, causal_weight, mechanism } = this.inferRelation(a, b);
          await this.addRelationship(a, b, relation, 0.7, {
            causal_weight,
            mechanism,
            provenance: [toolName]
          });
        }
      }

      console.log(`Ingested ${entities.length} entities from ${toolName}`);
    } catch (err: any) {
      console.error('Ingest error:', err.message || err);
      throw err;
    }
  }

  /**
   * Ingest a pre-structured entity with explicit relationships.
   * Used by external data loaders: Wikidata, GLEIF, ACLED, UN Comtrade, etc.
   */
  async ingestRaw(
    name: string,
    type: EntityType,
    properties: Record<string, any>,
    relationships: Array<{
      targetName: string;
      targetType: EntityType;
      targetProperties?: Record<string, any>;
      relation: string;
      confidence?: number;
      causal_weight?: number;
      mechanism?: string;
      valid_from?: string;
      valid_until?: string;
    }>,
    source: string,
    confidence: number = 0.8
  ): Promise<Entity> {
    const entity = await this.upsertEntity(name, type, properties, source, confidence, {
      provenance: [source]
    });

    for (const rel of relationships) {
      const target = await this.upsertEntity(
        rel.targetName,
        rel.targetType,
        rel.targetProperties || {},
        source,
        rel.confidence || 0.7,
        { provenance: [source] }
      );

      await this.addRelationship(entity, target, rel.relation, rel.confidence || 0.7, {
        causal_weight: rel.causal_weight,
        mechanism: rel.mechanism,
        valid_from: rel.valid_from,
        valid_until: rel.valid_until,
        provenance: [source]
      });
    }

    return entity;
  }

  // ─── ENTITY EXTRACTION ────────────────────────────────────────────────────────

  private extractEntities(
    toolName: string,
    result: any
  ): Array<{ name: string; type: EntityType; confidence: number; properties: Record<string, any> }> {
    const entities: Array<{ name: string; type: EntityType; confidence: number; properties: Record<string, any> }> = [];

    if (!result) return entities;

    if (toolName === 'find_leads' || toolName === 'get_company_info') {
      if (result.company || result.name) {
        const company = result.company || result;
        const name = company.name || company.company;
        if (name && name !== 'Unknown') {
          entities.push({
            name,
            type: 'company',
            confidence: 0.9,
            properties: {
              domain: company.domain || result.domain,
              industry: company.industry,
              size: company.size || company.employees,
              location: company.location || company.city,
              country: company.country,
              description: company.description,
            }
          });

          if (company.industry) {
            entities.push({ name: company.industry, type: 'industry', confidence: 0.85, properties: {} });
          }

          const locationName = company.location || company.city;
          if (locationName) {
            entities.push({
              name: locationName,
              type: 'location',
              confidence: 0.7,
              properties: { country: company.country }
            });
          }

          if (company.country) {
            entities.push({
              name: company.country,
              type: 'country',
              confidence: 0.9,
              properties: { iso_code: company.country }
            });
          }
        }
      }

      if (Array.isArray(result.technologies)) {
        for (const tech of result.technologies) {
          const techName = typeof tech === 'string' ? tech : tech.name;
          if (techName) {
            entities.push({
              name: techName,
              type: 'technology',
              confidence: 0.7,
              properties: typeof tech === 'object' ? tech : {}
            });
          }
        }
      }
    }

    if (toolName === 'find_emails') {
      if (Array.isArray(result.emails)) {
        for (const email of result.emails) {
          const personName = email.name || email.person;
          if (personName) {
            entities.push({
              name: personName,
              type: 'person',
              confidence: 0.8,
              properties: {
                email: email.email,
                title: email.title || email.position,
                source: email.source
              }
            });
          }
        }
      }
      if (result.domain) {
        entities.push({
          name: result.domain,
          type: 'company',
          confidence: 0.7,
          properties: { domain: result.domain }
        });
      }
    }

    // Fallback keyword-based industry extraction
    const industries = this.extractIndustries(result);
    for (const ind of industries) {
      if (!entities.find(e => e.type === 'industry' && e.name === ind)) {
        entities.push({ name: ind, type: 'industry', confidence: 0.5, properties: {} });
      }
    }

    return entities;
  }

  private extractIndustries(result: any): string[] {
    const text = JSON.stringify(result).toLowerCase();
    const industries = [
      'software', 'saas', 'fintech', 'healthcare', 'biotech', 'ecommerce',
      'retail', 'manufacturing', 'consulting', 'marketing', 'education',
      'real estate', 'construction', 'transportation', 'energy', 'media',
      'telecommunications', 'agriculture', 'mining', 'defence', 'aerospace',
      'pharmaceuticals', 'logistics', 'insurance', 'banking', 'automotive',
      'semiconductors', 'oil and gas', 'renewable energy', 'food and beverage',
    ];
    return Array.from(new Set(industries.filter(ind => text.includes(ind))));
  }

  private inferRelation(
    a: Entity,
    b: Entity
  ): { relation: string; causal_weight?: number; mechanism?: string } {
    const t = (x: Entity) => x.type;

    if (t(a) === 'company' && t(b) === 'person') return { relation: 'employs' };
    if (t(a) === 'person' && t(b) === 'company') return { relation: 'works_at' };
    if (t(a) === 'company' && t(b) === 'industry') return { relation: 'operates_in' };
    if (t(a) === 'industry' && t(b) === 'company') return { relation: 'operates_in' };
    if (t(a) === 'company' && t(b) === 'technology') return { relation: 'uses' };
    if (t(a) === 'technology' && t(b) === 'company') return { relation: 'uses' };
    if (t(a) === 'company' && t(b) === 'location') return { relation: 'located_in' };
    if (t(a) === 'company' && t(b) === 'country') return { relation: 'located_in' };
    if (t(a) === 'person' && t(b) === 'country') return { relation: 'located_in' };
    if (t(a) === 'country' && t(b) === 'country') return { relation: 'related_to', causal_weight: 0.3, mechanism: 'geopolitical_proximity' };
    if (t(a) === 'country' && t(b) === 'commodity_flow') return { relation: 'exports_to', causal_weight: 0.6, mechanism: 'trade_dependency' };
    if (t(a) === 'government' && t(b) === 'company') return { relation: 'regulated_by' };
    if (t(a) === 'conflict_event' && t(b) === 'country') return { relation: 'causes', causal_weight: 0.7, mechanism: 'conflict_spillover' };
    if (t(a) === 'economic_event' && t(b) === 'company') return { relation: 'causes', causal_weight: 0.5, mechanism: 'market_shock' };

    return { relation: 'related_to' };
  }

  // ─── SEARCH ───────────────────────────────────────────────────────────────────

  async findByIndustryAndLocation(industry: string, location?: string): Promise<Entity[]> {
    this.assertInitialized();

    const industryEntities = await this.findEntity(industry, 'industry');
    if (industryEntities.length === 0) return [];

    const results: Entity[] = [];
    const locationLower = location?.toLowerCase();

    for (const ind of industryEntities) {
      const relIds = await this.client!.sMembers(`rel:from:${ind.id}`);
      if (relIds.length === 0) continue;

      const multi = this.client!.multi();
      for (const relId of relIds) multi.hGetAll(`relationship:${relId}`);
      const relResults = await multi.exec() as Record<string, string>[];

      const validCompanyIds = new Set<string>();
      for (const relData of relResults) {
        if (!relData || Object.keys(relData).length === 0) continue;
        if (['operates_in', 'industry', 'related_to'].includes(relData.relation)) {
          validCompanyIds.add(relData.to_id);
        }
      }

      if (validCompanyIds.size === 0) continue;

      const companyIdsArray = Array.from(validCompanyIds);
      const companyMulti = this.client!.multi();
      for (const id of companyIdsArray) companyMulti.hGetAll(`entity:${id}`);
      const companyResults = await companyMulti.exec() as Record<string, string>[];

      for (let i = 0; i < companyResults.length; i++) {
        const companyData = companyResults[i];
        if (!companyData || Object.keys(companyData).length === 0) continue;

        const company = this.hydrateEntity(companyIdsArray[i], companyData);

        if (locationLower) {
          const props = company.properties;
          const loc = String(props.location || props.city || props.country || '').toLowerCase();
          if (loc.includes(locationLower)) results.push(company);
        } else {
          results.push(company);
        }
      }
    }

    return results.sort((a, b) => b.confidence - a.confidence);
  }

  // ─── STATS ────────────────────────────────────────────────────────────────────

  async getStats(): Promise<{
    total_nodes: number;
    total_edges: number;
    nodes_by_type: Record<string, number>;
    last_updated: string;
  }> {
    this.assertInitialized();

    const keys = await this.client!.keys('entity:*');
    const relKeys = await this.client!.keys('relationship:*');

    const nodesByType: Record<string, number> = {};

    const chunkSize = 500;
    for (let i = 0; i < keys.length; i += chunkSize) {
      const chunk = keys.slice(i, i + chunkSize);
      const multi = this.client!.multi();
      for (const key of chunk) multi.hGet(key, 'type');
      const types = await multi.exec() as (string | null)[];
      for (const type of types) {
        if (type) nodesByType[type] = (nodesByType[type] || 0) + 1;
      }
    }

    return {
      total_nodes: keys.length,
      total_edges: relKeys.length,
      nodes_by_type: nodesByType,
      last_updated: new Date().toISOString(),
    };
  }

  // ─── PRIVATE HELPERS ──────────────────────────────────────────────────────────

  private assertInitialized(): void {
    if (!this.client) {
      throw new Error('KnowledgeGraph not initialized. Call init() first.');
    }
  }

  private entityId(name: string, type: EntityType): string {
    return `${type}:${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}`;
  }

  private hydrateEntity(id: string, data: Record<string, string>): Entity {
    return {
      id,
      name: data.name || 'Unknown',
      type: (data.type as EntityType) || 'company',
      confidence: parseFloat(data.confidence || '0'),
      call_count: parseInt(data.call_count || '0', 10),
      properties: this.safeParseJSON(data.properties, {}),
      sources: this.safeParseJSON(data.sources, []),
      provenance: this.safeParseJSON(data.provenance, []),
      first_seen: data.first_seen || new Date().toISOString(),
      last_seen: data.last_seen || new Date().toISOString(),
      valid_from: data.valid_from,
      valid_until: data.valid_until,
    };
  }

  private hydrateRelationship(id: string, data: Record<string, string>): Relationship {
    return {
      id,
      from_id: data.from_id || '',
      to_id: data.to_id || '',
      from_name: data.from_name || '',
      to_name: data.to_name || '',
      relation: data.relation || 'related_to',
      confidence: parseFloat(data.confidence || '0'),
      causal_weight: data.causal_weight ? parseFloat(data.causal_weight) : undefined,
      mechanism: data.mechanism,
      lag_days: data.lag_days ? parseInt(data.lag_days, 10) : undefined,
      effect_size: data.effect_size ? parseFloat(data.effect_size) : undefined,
      valid_from: data.valid_from,
      valid_until: data.valid_until,
      provenance: this.safeParseJSON(data.provenance, []),
      first_seen: data.first_seen || new Date().toISOString(),
      last_seen: data.last_seen || new Date().toISOString(),
    };
  }

  private safeParseJSON<T>(jsonStr: string | undefined, fallback: T): T {
    if (!jsonStr) return fallback;
    try {
      return JSON.parse(jsonStr) as T;
    } catch {
      return fallback;
    }
  }
}

export const knowledgeGraph = new KnowledgeGraph();
