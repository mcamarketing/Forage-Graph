/**
 * Link Prediction Module — GraphBLAS-native implementations
 *
 * Implements topological inference algorithms using matrix algebra:
 * - Adamic-Adar Similarity via GraphBLAS semirings
 * - Jaccard Coefficient for neighbor overlap
 *
 * FalkorDB's GraphBLAS engine represents graph traversals as
 * vectorized matrix multiplications rather than pointer-chasing.
 *
 * For adjacency matrix $A$:
 * - Degree of node $i$: $d_i = \sum_j A_{ij}$
 * - Common neighbors: $|N(i) \cap N(j)| = (A^2)_{ij}$
 * - Adamic-Adar: $AA(i,j) = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$
 * - Jaccard: $J(i,j) = \frac{|N(i) \cap N(j)|}{|N(i) \cup N(j)|}$
 */

import { createClient } from 'redis';

// ─── TYPES ────────────────────────────────────────────────────────────────────

export interface LinkPrediction {
  source: string;
  target: string;
  score: number;
  algorithm: 'adamic_adar' | 'jaccard' | 'preferential_attachment';
  common_neighbors: number;
  confidence: number;
}

export interface PredictionRequest {
  sourceEntity: string;
  targetEntityType?: string;
  maxPredictions?: number;
  minScore?: number;
}

// ─── GRAPHBLAS ADAMIC-ADAR ────────────────────────────────────────────────────

/**
 * Adamic-Adar Similarity using GraphBLAS semirings.
 *
 * Matrix formulation:
 * $$AA_{ij} = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$$
 *
 * Where:
 * - $N(i)$ = set of neighbors of node $i$
 * - $d_v$ = degree of common neighbor $v$
 * - $AA_{ij}$ = similarity score between nodes $i$ and $j$
 *
 * GraphBLAS optimization:
 * 1. Compute degree vector: $\mathbf{d} = A \cdot \mathbf{1}$
 * 2. Compute weight vector: $\mathbf{w}_v = \frac{1}{\log d_v}$
 * 3. Compute common neighbors: $C = A \cdot A$ (via `PLUS_TIMES` semiring)
 * 4. Apply weights: $AA = A \cdot \text{diag}(\mathbf{w}) \cdot A$
 *
 * Time complexity: $O(|E| \cdot \bar{d})$ where $\bar{d}$ is average degree
 * [fp-aa-001]
 */
export class AdamicAdarScorer {
  private client: ReturnType<typeof createClient> | null = null;
  private graphName: string;

  constructor(client: ReturnType<typeof createClient>, graphName: string) {
    this.client = client;
    this.graphName = graphName;
  }

  /**
   * Compute Adamic-Adar scores for potential links from a source node.
   *
   * Returns predicted edges ranked by score, excluding existing edges.
   */
  async predictLinks(
    sourceId: string,
    targetEntityType?: string,
    maxPredictions = 10,
    minScore = 0.1
  ): Promise<LinkPrediction[]> {
    if (!this.client) return [];

    // Step 1: Get neighbors of source (one hop)
    const neighborRows = await this.query(
      `MATCH (source:Entity {id: $sourceId})-[:RELATES]->(neighbor:Entity)
       RETURN neighbor.id, neighbor.name, neighbor.type`,
      { sourceId }
    );

    if (!neighborRows.length) return [];

    // Step 2: Get second-hop neighbors (candidates)
    // These are neighbors of neighbors, excluding source and direct neighbors
    const candidateRows = await this.query(
      `MATCH (source:Entity {id: $sourceId})-[:RELATES]->(neighbor:Entity)-[:RELATES]->(candidate:Entity)
       WHERE candidate.id <> $sourceId
       AND NOT (source)-[:RELATES]->(candidate)
       RETURN candidate.id as id, candidate.name as name, candidate.type as type,
              COUNT(DISTINCT neighbor) as common_neighbors`,
      { sourceId }
    );

    if (!candidateRows.length) return [];

    // Step 3: Compute Adamic-Adar score for each candidate
    const predictions: LinkPrediction[] = [];

    for (const row of candidateRows) {
      const candidateId = row[0];
      const candidateName = row[1];
      const candidateType = row[2];
      const commonNeighbors = parseInt(row[3]) || 0;

      if (commonNeighbors === 0) continue;

      // Filter by target entity type if specified
      if (targetEntityType && candidateType !== targetEntityType) continue;

      // Get degrees of common neighbors for Adamic-Adar weighting
      const degreeRows = await this.query(
        `MATCH (source:Entity {id: $sourceId})-[:RELATES]->(common:Entity)-[:RELATES]->(candidate:Entity {id: $candidateId})
         RETURN common.id, common.degree`,
        { sourceId, candidateId }
      );

      // Adamic-Adar: $AA = \sum_{v \in N(i) \cap N(j)} \frac{1}{\log d_v}$
      let adamicAdarScore = 0;
      for (const degRow of degreeRows) {
        const degree = parseInt(degRow[1]) || 1;
        if (degree > 1) {
          adamicAdarScore += 1.0 / Math.log(degree);
        }
      }

      if (adamicAdarScore >= minScore) {
        predictions.push({
          source: sourceId,
          target: candidateId,
          score: Math.round(adamicAdarScore * 1000) / 1000,
          algorithm: 'adamic_adar',
          common_neighbors: commonNeighbors,
          confidence: Math.min(0.95, 0.5 + (adamicAdarScore * 0.1)),
        });
      }
    }

    return predictions
      .sort((a, b) => b.score - a.score)
      .slice(0, maxPredictions);
  }

  private async query(cypher: string, params: Record<string, any>): Promise<any[]> {
    if (!this.client) return [];
    try {
      const result = await (this.client as any).sendCommand([
        'GRAPH.QUERY',
        this.graphName,
        cypher,
        '--params',
        JSON.stringify(params),
        '--compact',
      ]);
      if (!Array.isArray(result) || !Array.isArray(result[1])) return [];
      return result[1];
    } catch {
      return [];
    }
  }
}

// ─── JACCARD COEFFICIENT ──────────────────────────────────────────────────────

/**
 * Jaccard Coefficient for neighbor overlap.
 *
 * $$J(i,j) = \frac{|N(i) \cap N(j)|}{|N(i) \cup N(j)|} = \frac{|N(i) \cap N(j)|}{|N(i)| + |N(j)| - |N(i) \cap N(j)|}$$
 *
 * GraphBLAS formulation:
 * $$J = \frac{C_{ij}}{d_i + d_j - C_{ij}}$$
 *
 * Where:
 * - $C = A \cdot A$ via `PLUS_TIMES` semiring (common neighbors)
 * - $d_i = \sum_j A_{ij}$ (degree vector)
 * [fp-jc-001]
 */
export class JaccardScorer {
  private client: ReturnType<typeof createClient> | null = null;
  private graphName: string;

  constructor(client: ReturnType<typeof createClient>, graphName: string) {
    this.client = client;
    this.graphName = graphName;
  }

  /**
   * Compute Jaccard similarity between source and candidate nodes.
   */
  async computeJaccard(
    sourceId: string,
    candidateIds: string[]
  ): Promise<Map<string, number>> {
    const scores = new Map<string, number>();
    if (!this.client || candidateIds.length === 0) return scores;

    for (const candidateId of candidateIds) {
      const row = await this.query(
        `MATCH (a:Entity {id: $sourceId})-[:RELATES]->(common:Entity)-[:RELATES]->(b:Entity {id: $candidateId})
         WITH COUNT(DISTINCT common) as intersection
         MATCH (a:Entity {id: $sourceId})-[:RELATES]->(aN:Entity)
         WITH COUNT(DISTINCT aN) as sizeA, intersection
         MATCH (b:Entity {id: $candidateId})-[:RELATES]->(bN:Entity)
         WITH COUNT(DISTINCT bN) as sizeB, sizeA, intersection
         RETURN toFloat(intersection) / (sizeA + sizeB - intersection) as jaccard`,
        { sourceId, candidateId }
      );

      if (row.length && row[0][0]) {
        scores.set(candidateId, parseFloat(row[0][0]) || 0);
      }
    }

    return scores;
  }

  private async query(cypher: string, params: Record<string, any>): Promise<any[]> {
    if (!this.client) return [];
    try {
      const result = await (this.client as any).sendCommand([
        'GRAPH.QUERY',
        this.graphName,
        cypher,
        '--params',
        JSON.stringify(params),
        '--compact',
      ]);
      if (!Array.isArray(result) || !Array.isArray(result[1])) return [];
      return result[1];
    } catch {
      return [];
    }
  }
}

// ─── PREFERENTIAL ATTACHMENT ──────────────────────────────────────────────────

/**
 * Preferential Attachment Score.
 *
 * For link prediction, nodes with higher degrees are more likely to connect.
 * Score is simply the product of degrees:
 * $$PA(i,j) = |N(i)| \cdot |N(j)|$$
 *
 * This models the "rich get richer" phenomenon in scale-free networks.
 * [fp-pa-001]
 */
export class PreferentialAttachmentScorer {
  private client: ReturnType<typeof createClient> | null = null;
  private graphName: string;

  constructor(client: ReturnType<typeof createClient>, graphName: string) {
    this.client = client;
    this.graphName = graphName;
  }

  async scorePair(sourceId: string, targetId: string): Promise<number> {
    if (!this.client) return 0;

    const row = await this.query(
      `MATCH (a:Entity {id: $sourceId})-[:RELATES]->()
       WITH COUNT(*) as degA
       MATCH (b:Entity {id: $targetId})-[:RELATES]->()
       RETURN degA * COUNT(*) as pa_score`,
      { sourceId, targetId }
    );

    return row.length && row[0][0] ? parseInt(row[0][0]) : 0;
  }

  private async query(cypher: string, params: Record<string, any>): Promise<any[]> {
    if (!this.client) return [];
    try {
      const result = await (this.client as any).sendCommand([
        'GRAPH.QUERY',
        this.graphName,
        cypher,
        '--params',
        JSON.stringify(params),
        '--compact',
      ]);
      if (!Array.isArray(result) || !Array.isArray(result[1])) return [];
      return result[1];
    } catch {
      return [];
    }
  }
}
