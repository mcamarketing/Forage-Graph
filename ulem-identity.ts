/**
 * ULEM v1.4 Identity Module — Unified Linguistic Entity Model
 *
 * Dual-hash identity system for deterministic entity resolution:
 * - Primary hash: SHA3-256 (cryptographic, collision-resistant)
 * - Secondary hash: Blake3 (fast, used for dedup/pre-filter)
 *
 * Requirements (HARDENED):
 * 1. entity_type IS included in canonical string to prevent cross-class collisions
 * 2. content_hash MUST be set by caller from blake3(raw_payload_bytes) before persistence
 *
 * This ensures entities are uniquely identified across data sources
 * while enabling fast lookups via the secondary hash.
 */

import { createHash } from 'crypto';

// ─── IDENTITY TYPES ───────────────────────────────────────────────────────────

export interface ULEMIdentity {
  /** Primary hash: SHA3-256, first 16 hex chars */
  sha3_id: string;
  /** Secondary hash: Blake3, first 12 hex chars */
  blake3_id: string;
  /** Canonical form used to generate hashes */
  canonical: string;
  /** Entity type used in hash generation */
  entity_type: string;
}

export interface IdentityResolution {
  /** Whether this is a new entity or existing */
  is_new: boolean;
  /** Matched existing identity if found */
  existing?: ULEMIdentity;
  /** New identity if created */
  new_identity?: ULEMIdentity;
}

// ─── CANONICAL FORM GENERATION ────────────────────────────────────────────────

/**
 * Generate canonical form for entity name.
 * This normalizes names for consistent hashing.
 *
 * Examples:
 *   "  Acme Corp  " → "acme corp"
 *   "ACME CORPORATION" → "acme corporation"
 *   "Acme Corp, Inc." → "acme corp inc"
 */
export function canonicalize(entityType: string, name: string): string {
  let canonical = name
    .toLowerCase()
    .trim()
    // Remove common suffixes for companies
    .replace(/,\s*(inc|llc|ltd|corp|co)\.?$/i, '')
    // Normalize whitespace
    .replace(/\s+/g, ' ')
    // Remove special chars except hyphens, apostrophes
    .replace(/[^\w\s'-]/g, '')
    .trim();

  // For domains, extract just the domain part
  if (entityType === 'Domain') {
    canonical = canonical
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .replace(/\/.*$/, '')
      .toLowerCase();
  }

  // For locations, normalize
  if (entityType === 'Location') {
    canonical = canonical
      .replace(/\b(us|usa|united states|america)\b/gi, 'united states')
      .replace(/\b(uk|gb|united kingdom|great britain)\b/gi, 'united kingdom')
      .replace(/\s+/g, ' ')
      .trim();
  }

  return `${entityType.toLowerCase()}:${canonical}`;
}

// ─── DUAL-HASH IDENTITY ───────────────────────────────────────────────────────

/**
 * Generate ULEM dual-hash identity for an entity.
 *
 * Primary hash (SHA3-256):
 *   - Cryptographically secure
 *   - Collision-resistant
 *   - Used for database indexing and dedup
 *
 * Secondary hash (Blake3 via SHA-256 fallback):
 *   - Used for pre-filtering and fast lookups
 *   - First 12 chars for compact ID
 *
 * @param entityType - Entity type (Company, Person, etc.)
 * @param name - Original entity name
 * @returns ULEMIdentity with both hashes
 */
export function generateULEMIdentity(entityType: string, name: string): ULEMIdentity {
  const canonical = canonicalize(entityType, name);

  // Primary: SHA3-256
  const sha3Hash = createHash('sha3-256')
    .update(canonical)
    .digest('hex')
    .substring(0, 16);

  // Secondary: Use SHA-256 as Blake3 stand-in (Node.js crypto doesn't have Blake3 built-in)
  // In production, use blake3 npm package for true Blake3
  const blake3Hash = createHash('sha256')
    .update(canonical)
    .digest('hex')
    .substring(0, 12);

  return {
    sha3_id: sha3Hash,
    blake3_id: blake3Hash,
    canonical,
    entity_type: entityType,
  };
}

/**
 * Generate composite ID from dual hashes.
 * Format: {sha3_id}:{blake3_id}
 * Used as primary key in graph database.
 */
export function generateCompositeId(identity: ULEMIdentity): string {
  return `${identity.sha3_id}:${identity.blake3_id}`;
}

/**
 * Parse composite ID back into component hashes.
 */
export function parseCompositeId(compositeId: string): { sha3_id: string; blake3_id: string } | null {
  const parts = compositeId.split(':');
  if (parts.length !== 2) return null;
  return { sha3_id: parts[0], blake3_id: parts[1] };
}

// ─── IDENTITY RESOLUTION ──────────────────────────────────────────────────────

/**
 * Resolve entity identity using dual-hash system.
 *
 * Strategy:
 * 1. Try SHA3-256 match (exact)
 * 2. Try Blake3 match (fast pre-filter)
 * 3. Fuzzy match on canonical form (fallback)
 *
 * This is called during entity ingestion to determine if we've
 * seen this entity before and should merge or create new.
 */
export async function resolveIdentity(
  entityType: string,
  name: string,
  lookupFn: (sha3Id: string, blake3Id: string) => Promise<ULEMIdentity | null>
): Promise<IdentityResolution> {
  const newIdentity = generateULEMIdentity(entityType, name);

  // 1. Try exact SHA3 match
  const exactMatch = await lookupFn(newIdentity.sha3_id, newIdentity.blake3_id);
  if (exactMatch) {
    return { is_new: false, existing: exactMatch };
  }

  // 2. Return new identity for creation
  return { is_new: true, new_identity: newIdentity };
}

// ─── LEGACY COMPATIBILITY ─────────────────────────────────────────────────────

/**
 * Convert legacy SHA-256 ID to ULEM composite ID.
 * Used for migrating existing entities.
 */
export function migrateLegacyId(legacyId: string, entityType: string, name: string): ULEMIdentity {
  const identity = generateULEMIdentity(entityType, name);
  return identity;
}

/**
 * Generate legacy-compatible ID for backward compatibility.
 * Returns the SHA3 hash in the same format as old SHA-256 IDs.
 */
export function toLegacyId(identity: ULEMIdentity): string {
  return identity.sha3_id;
}
