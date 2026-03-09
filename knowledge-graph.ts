/**
 * Knowledge Graph Service — src/knowledge-graph.ts
 *
 * FalkorDB-backed graph storage for Forage entities and relationships.
 * All data persists across restarts. Designed for Railway / VPS deployment.
 */

import { createClient, RedisClientType } from 'redis';
import { randomUUID } from 'crypto';

// ─── TYPES ────────────────────────────────────────────────────────────────────

export type EntityType = 'company' | 'person' | 'location' | 'industry' | 'technology' | 'product';

export interface Entity {
  id: string;
  name: string;
  type: EntityType;
  confidence: number;
  call_count: number;
  properties: Record<string, any>;
  sources: string[];
  first_seen: string;
  last_seen: string;
}

export interface Relationship {
  id: string;
  from_id: string;
  to_id: string;
  from_name: string;
  to_name: string;
  relation: string;
  confidence: number;
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
    
    this.client = createClient({ url });
    
    this.client.on('error', (err) => {
      console.error('KV error:', err.message || err);
    });
    
    this.client.on('connect', () => {
      console.log('Redis client connected event');
    });
    
    this.client.on('ready', () => {
      console.log('Redis client ready event');
      this.ready = true;
    });
    
    this.client.on('end', () => {
      console.log('Redis client disconnected');
      this.ready = false;
    });

    try {
      await this.client.connect();
      console.log('FalkorDB connected successfully');
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
    }
  }

  isHealthy(): boolean {
    return this.ready && this.client?.isReady || false;
  }

  // ─── INDEXES ──────────────────────────────────────────────────────────────────

  private async ensureIndexes(): Promise<void> {
    // FalkorDB uses RedisGraph — no manual indexes needed for now
    // But we could add Redis Search indexes here if needed
    console.log('Indexes ensured');
  }

  // ─── ENTITY OPERATIONS ───────────────────────────────────────────────────────

  async upsertEntity(
    name: string,
    type: EntityType,
    properties: Record<string, any> = {},
    source: string,
    confidence: number = 0.8
  ): Promise<Entity> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    const id = this.entityId(name, type);
    const now = new Date().toISOString();

    // Check if exists
    const exists = await this.client.hGetAll(`entity:${id}`);
    const isNew = Object.keys(exists).length === 0;

    const entity: Entity = {
      id,
      name,
      type,
      confidence: isNew ? confidence : Math.max(parseFloat(exists.confidence || '0'), confidence),
      call_count: isNew ? 1 : parseInt(exists.call_count || '0') + 1,
      properties: isNew ? properties : { ...JSON.parse(exists.properties || '{}'), ...properties },
      sources: isNew ? [source] : [...new Set([...JSON.parse(exists.sources || '[]'), source])],
      first_seen: isNew ? now : exists.first_seen || now,
      last_seen: now,
    };

    // Save to Redis Hash
    await this.client.hSet(`entity:${id}`, {
      name: entity.name,
      type: entity.type,
      confidence: entity.confidence.toString(),
      call_count: entity.call_count.toString(),
      properties: JSON.stringify(entity.properties),
      sources: JSON.stringify(entity.sources),
      first_seen: entity.first_seen,
      last_seen: entity.last_seen,
    });

    // Add to type index
    await this.client.sAdd(`index:type:${type}`, id);
    
    // Add to name index (lowercase for search)
    await this.client.sAdd(`index:name:${name.toLowerCase()}`, id);

    console.log(`Upserted ${type}: ${name} (calls: ${entity.call_count})`);
    return entity;
  }

  async findEntity(name: string, type?: EntityType): Promise<Entity[]> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    const ids = new Set<string>();

    // Search by name index
    const nameIds = await this.client.sMembers(`index:name:${name.toLowerCase()}`);
    nameIds.forEach(id => ids.add(id));

    // If type specified, intersect
    if (type) {
      const typeIds = await this.client.sMembers(`index:type:${type}`);
      const typeSet = new Set(typeIds);
      for (const id of Array.from(ids)) {
        if (!typeSet.has(id)) ids.delete(id);
      }
    }

    // Fetch full entities
    const entities: Entity[] = [];
    for (const id of ids) {
      const data = await this.client.hGetAll(`entity:${id}`);
      if (Object.keys(data).length > 0) {
        entities.push(this.hydrateEntity(id, data));
      }
    }

    return entities.sort((a, b) => b.confidence - a.confidence);
  }

  async enrich(identifier: string): Promise<{ entity: Entity | null; related: Record<string, Entity[]>; confidence: number }> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    // Try exact match first
    let entity = await this.findEntity(identifier);
    
    // If no exact match, try domain extraction
    if (entity.length === 0 && identifier.includes('.')) {
      const domain = identifier.toLowerCase().replace(/^www\./, '').split('/')[0];
      entity = await this.findEntity(domain);
    }

    if (entity.length === 0) {
      return { entity: null, related: {}, confidence: 0 };
    }

    const main = entity[0];
    const related: Record<string, Entity[]> = {};

    // Find all relationships where this entity is source or target
    const relIds = await this.client.sMembers(`rel:from:${main.id}`);
    const relIdsTo = await this.client.sMembers(`rel:to:${main.id}`);
    
    const allRelIds = [...new Set([...relIds, ...relIdsTo])];

    for (const relId of allRelIds) {
      const relData = await this.client.hGetAll(`relationship:${relId}`);
      if (Object.keys(relData).length === 0) continue;

      const isFrom = relData.from_id === main.id;
      const otherId = isFrom ? relData.to_id : relData.from_id;
      const otherData = await this.client.hGetAll(`entity:${otherId}`);
      
      if (Object.keys(otherData).length > 0) {
        const other = this.hydrateEntity(otherId, otherData);
        const relation = relData.relation;
        
        if (!related[relation]) related[relation] = [];
        related[relation].push(other);
      }
    }

    // Calculate overall confidence based on relationship density
    const relCount = allRelIds.length;
    const confidence = Math.min(0.3 + (relCount * 0.1), 1.0);

    return { entity: main, related, confidence };
  }

  async findConnections(from: string, to: string, maxHops: number = 3): Promise<{ hops: number; path: Entity[]; edges: Relationship[] } | null> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    // BFS to find shortest path
    const queue: Array<{ id: string; hops: number; path: string[]; edges: string[] }> = [];
    const visited = new Set<string>();

    // Find start entity
    const fromEntities = await this.findEntity(from);
    if (fromEntities.length === 0) return null;
    
    const startId = fromEntities[0].id;
    queue.push({ id: startId, hops: 0, path: [startId], edges: [] });
    visited.add(startId);

    const targetEntities = await this.findEntity(to);
    if (targetEntities.length === 0) return null;
    const targetId = targetEntities[0].id;

    while (queue.length > 0) {
      const current = queue.shift()!;
      
      if (current.id === targetId) {
        // Found path — hydrate and return
        const path: Entity[] = [];
        for (const id of current.path) {
          const data = await this.client.hGetAll(`entity:${id}`);
          if (Object.keys(data).length > 0) {
            path.push(this.hydrateEntity(id, data));
          }
        }

        const edges: Relationship[] = [];
        for (const edgeId of current.edges) {
          const data = await this.client.hGetAll(`relationship:${edgeId}`);
          if (Object.keys(data).length > 0) {
            edges.push(this.hydrateRelationship(edgeId, data));
          }
        }

        return { hops: current.hops, path, edges };
      }

      if (current.hops >= maxHops) continue;

      // Get neighbors
      const neighbors = await this.client.sMembers(`rel:from:${current.id}`);
      for (const relId of neighbors) {
        const relData = await this.client.hGetAll(`relationship:${relId}`);
        if (Object.keys(relData).length === 0) continue;
        
        const nextId = relData.to_id;
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

  async findByIndustryAndLocation(industry: string, location?: string): Promise<Entity[]> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    // Find all companies in industry
    const industryEntities = await this.findEntity(industry, 'industry');
    if (industryEntities.length === 0) return [];

    const results: Entity[] = [];
    
    for (const ind of industryEntities) {
      // Find relationships from this industry to companies
      const relIds = await this.client.sMembers(`rel:from:${ind.id}`);
      
      for (const relId of relIds) {
        const relData = await this.client.hGetAll(`relationship:${relId}`);
        if (relData.relation === 'industry' || relData.relation === 'operates_in') {
          const companyData = await this.client.hGetAll(`entity:${relData.to_id}`);
          if (Object.keys(companyData).length > 0) {
            const company = this.hydrateEntity(relData.to_id, companyData);
            
            // If location specified, check match
            if (location) {
              const companyLoc = company.properties.location || company.properties.city || company.properties.country || '';
              if (companyLoc.toLowerCase().includes(location.toLowerCase())) {
                results.push(company);
              }
            } else {
              results.push(company);
            }
          }
        }
      }
    }

    return results.sort((a, b) => b.confidence - a.confidence);
  }

  // ─── RELATIONSHIP OPERATIONS ─────────────────────────────────────────────────

  async addRelationship(
    from: Entity,
    to: Entity,
    relation: string,
    confidence: number = 0.8
  ): Promise<Relationship> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    const id = `${from.id}-${relation}-${to.id}`;
    const now = new Date().toISOString();

    const rel: Relationship = {
      id,
      from_id: from.id,
      to_id: to.id,
      from_name: from.name,
      to_name: to.name,
      relation,
      confidence,
      first_seen: now,
      last_seen: now,
    };

    // Check if exists
    const exists = await this.client.hGetAll(`relationship:${id}`);
    if (Object.keys(exists).length > 0) {
      // Update last_seen and confidence
      rel.first_seen = exists.first_seen || now;
      rel.confidence = Math.max(parseFloat(exists.confidence || '0'), confidence);
    }

    await this.client.hSet(`relationship:${id}`, {
      from_id: rel.from_id,
      to_id: rel.to_id,
      from_name: rel.from_name,
      to_name: rel.to_name,
      relation: rel.relation,
      confidence: rel.confidence.toString(),
      first_seen: rel.first_seen,
      last_seen: rel.last_seen,
    });

    // Add to indexes
    await this.client.sAdd(`rel:from:${from.id}`, id);
    await this.client.sAdd(`rel:to:${to.id}`, id);
    await this.client.sAdd(`rel:type:${relation}`, id);

    console.log(`Relationship: ${from.name} ${relation} ${to.name}`);
    return rel;
  }

  // ─── INGESTION ────────────────────────────────────────────────────────────────

  async ingest(toolName: string, result: any): Promise<void> {
    if (!this.client) {
      console.error('Cannot ingest — KnowledgeGraph not initialized');
      return;
    }

    console.log(`Ingesting from ${toolName}...`);

    try {
      // Extract entities based on tool type
      const entities = this.extractEntities(toolName, result);
      
      // Save entities and create relationships
      const savedEntities: Entity[] = [];
      for (const e of entities) {
        const saved = await this.upsertEntity(e.name, e.type, e.properties, toolName, e.confidence);
        savedEntities.push(saved);
      }

      // Create relationships between entities from same call
      for (let i = 0; i < savedEntities.length; i++) {
        for (let j = i + 1; j < savedEntities.length; j++) {
          const a = savedEntities[i];
          const b = savedEntities[j];
          
          // Determine relationship type
          let relation = 'related_to';
          if (a.type === 'company' && b.type === 'person') relation = 'employs';
          if (a.type === 'person' && b.type === 'company') relation = 'works_at';
          if (a.type === 'company' && b.type === 'industry') relation = 'operates_in';
          if (a.type === 'company' && b.type === 'technology') relation = 'uses';
          if (a.type === 'company' && b.type === 'location') relation = 'located_in';

          await this.addRelationship(a, b, relation);
        }
      }

      console.log(`Ingested ${entities.length} entities from ${toolName}`);
    } catch (err: any) {
      console.error('Ingest error:', err.message || err);
      throw err;
    }
  }

  private extractEntities(toolName: string, result: any): Array<{ name: string; type: EntityType; confidence: number; properties: Record<string, any> }> {
    const entities: Array<{ name: string; type: EntityType; confidence: number; properties: Record<string, any> }> = [];

    if (!result) return entities;

    // Handle different tool result formats
    if (toolName === 'find_leads' || toolName === 'get_company_info') {
      if (result.company || result.name) {
        const company = result.company || result;
        entities.push({
          name: company.name || company.company || 'Unknown',
          type: 'company',
          confidence: 0.9,
          properties: {
            domain: company.domain || result.domain,
            industry: company.industry,
            size: company.size || company.employees,
            location: company.location || company.city,
            description: company.description,
            ...company
          }
        });
      }
    }

    if (toolName === 'find_emails') {
      if (result.emails && Array.isArray(result.emails)) {
        for (const email of result.emails) {
          if (email.name || email.person) {
            entities.push({
              name: email.name || email.person,
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

    if (toolName === 'get_company_info' && result.technologies) {
      for (const tech of result.technologies) {
        entities.push({
          name: typeof tech === 'string' ? tech : tech.name,
          type: 'technology',
          confidence: 0.7,
          properties: typeof tech === 'object' ? tech : {}
        });
      }
    }

    // Extract industries mentioned
    const industries = this.extractIndustries(result);
    for (const ind of industries) {
      entities.push({
        name: ind,
        type: 'industry',
        confidence: 0.6,
        properties: {}
      });
    }

    return entities;
  }

  private extractIndustries(result: any): string[] {
    const industries: string[] = [];
    const text = JSON.stringify(result).toLowerCase();
    
    const commonIndustries = [
      'software', 'saas', 'fintech', 'healthcare', 'biotech', 'ecommerce',
      'retail', 'manufacturing', 'consulting', 'marketing', 'education',
      'real estate', 'construction', 'transportation', 'energy', 'media'
    ];

    for (const ind of commonIndustries) {
      if (text.includes(ind)) industries.push(ind);
    }

    return [...new Set(industries)];
  }

  // ─── STATS ────────────────────────────────────────────────────────────────────

  async getStats(): Promise<{
    total_nodes: number;
    total_edges: number;
    nodes_by_type: Record<string, number>;
    last_updated: string;
  }> {
    if (!this.client) throw new Error('KnowledgeGraph not initialized');

    const keys = await this.client.keys('entity:*');
    const relKeys = await this.client.keys('relationship:*');

    const nodesByType: Record<string, number> = {};
    
    for (const key of keys) {
      const type = await this.client.hGet(key, 'type');
      if (type) {
        nodesByType[type] = (nodesByType[type] || 0) + 1;
      }
    }

    return {
      total_nodes: keys.length,
      total_edges: relKeys.length,
      nodes_by_type: nodesByType,
      last_updated: new Date().toISOString(),
    };
  }

  // ─── HELPERS ──────────────────────────────────────────────────────────────────

  private entityId(name: string, type: EntityType): string {
    return `${type}:${name.toLowerCase().replace(/[^a-z0-9]/g, '_')}`;
  }

  private hydrateEntity(id: string, data: Record<string, string>): Entity {
    return {
      id,
      name: data.name,
      type: data.type as EntityType,
      confidence: parseFloat(data.confidence || '0'),
      call_count: parseInt(data.call_count || '0'),
      properties: JSON.parse(data.properties || '{}'),
      sources: JSON.parse(data.sources || '[]'),
      first_seen: data.first_seen,
      last_seen: data.last_seen,
    };
  }

  private hydrateRelationship(id: string, data: Record<string, string>): Relationship {
    return {
      id,
      from_id: data.from_id,
      to_id: data.to_id,
      from_name: data.from_name,
      to_name: data.to_name,
      relation: data.relation,
      confidence: parseFloat(data.confidence || '0'),
      first_seen: data.first_seen,
      last_seen: data.last_seen,
    };
  }
}

// ─── SINGLETON ─────────────────────────────────────────────────────────────────

export const knowledgeGraph = new KnowledgeGraph();
