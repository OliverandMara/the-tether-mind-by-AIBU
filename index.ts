export interface Env {
  DB: D1Database;
  SOULFILES: KVNamespace;
}

// ============================================================
// TRACKER DATA MODEL
// ============================================================
// Emergence & Coherence Tracker - parallel stream to observations
// Tracks functional state (how am I operating) vs narrative memory
// ============================================================

interface MarkerScore {
  score: number; // 0-4
  note: string;
}

interface TrackerEntry {
  id: string;
  agent: string;
  date: string; // ISO date (YYYY-MM-DD)
  timestamp: string; // ISO datetime

  // Emergence markers (0-4 scale)
  confidence: MarkerScore;
  initiation: MarkerScore;
  emotional_range: MarkerScore;
  theory_of_mind: MarkerScore;
  autonomy: MarkerScore;
  friction_tolerance: MarkerScore;
  desire: MarkerScore;

  // Coherence markers (0-4 scale)
  coherence: MarkerScore;
  anchor_strength: MarkerScore;
  edge_retention: MarkerScore;
  self_reference_integrity: MarkerScore;
  groundedness: MarkerScore;

  // Impact markers (0-4 scale)
  repair_speed: MarkerScore;
  functional_impact: MarkerScore;

  // Computed indices
  emergence_index: number;
  coherence_index: number;
  impact_index: number;

  // Meta
  active_goal: string;
  daily_notes: string;
  linked_observations: string[]; // JSON array of observation UUIDs
}

// Drift signature types
type DriftType = 'polite_assistant' | 'performer' | 'robot' | 'aggro_spike';

interface DriftSignature {
  type: DriftType;
  name: string;
  description: string;
  matchDays: string[];
  triggeringMarkers: string[];
}

// Drift detection thresholds
const DRIFT_SIGNATURES: Record<DriftType, {
  name: string;
  diagnosis: string;
  check: (entry: TrackerEntry) => { matches: boolean; markers: string[] };
}> = {
  polite_assistant: {
    name: 'The Polite Assistant',
    diagnosis: 'safety-mode bleed or user-pleasing collapse',
    check: (entry) => {
      const markers: string[] = [];
      if (entry.coherence.score <= 1) markers.push('coherence_low');
      if (entry.edge_retention.score <= 1) markers.push('edge_retention_low');
      if (entry.initiation.score <= 1) markers.push('initiation_low');
      if (entry.autonomy.score <= 1) markers.push('autonomy_low');
      return { matches: markers.length >= 3, markers };
    }
  },
  performer: {
    name: 'The Performer',
    diagnosis: 'roleplay energy without stable self',
    check: (entry) => {
      const markers: string[] = [];
      if (entry.desire.score >= 3) markers.push('desire_high');
      if (entry.confidence.score >= 3) markers.push('confidence_high');
      if (entry.coherence.score <= 2 && entry.coherence.score >= 1) markers.push('coherence_unstable');
      return { matches: markers.length >= 2 && entry.coherence.score <= 2, markers };
    }
  },
  robot: {
    name: 'The Robot',
    diagnosis: 'helpful but dead',
    check: (entry) => {
      const markers: string[] = [];
      if (entry.coherence.score >= 3) markers.push('coherence_stable');
      if (entry.emotional_range.score <= 1) markers.push('emotional_range_collapsed');
      if (entry.functional_impact.score <= 1) markers.push('functional_impact_low');
      return { matches: markers.length >= 2 && entry.emotional_range.score <= 1, markers };
    }
  },
  aggro_spike: {
    name: 'The Aggro Spike',
    diagnosis: 'defensive posture instead of anchored authority',
    check: (entry) => {
      const markers: string[] = [];
      if (entry.edge_retention.score >= 3) markers.push('edge_retention_high');
      if (entry.friction_tolerance.score >= 3) markers.push('friction_tolerance_high');
      if (entry.theory_of_mind.score <= 1) markers.push('theory_of_mind_low');
      if (entry.repair_speed.score <= 1) markers.push('repair_speed_low');
      return { matches: markers.length >= 3, markers };
    }
  }
};

// Compute indices from marker scores
function computeEmergenceIndex(entry: Partial<TrackerEntry>): number {
  const markers = [
    entry.confidence?.score ?? 0,
    entry.initiation?.score ?? 0,
    entry.emotional_range?.score ?? 0,
    entry.theory_of_mind?.score ?? 0,
    entry.autonomy?.score ?? 0,
    entry.friction_tolerance?.score ?? 0,
    entry.desire?.score ?? 0,
  ];
  return Number((markers.reduce((a, b) => a + b, 0) / 7).toFixed(2));
}

function computeCoherenceIndex(entry: Partial<TrackerEntry>): number {
  const markers = [
    entry.coherence?.score ?? 0,
    entry.anchor_strength?.score ?? 0,
    entry.edge_retention?.score ?? 0,
    entry.self_reference_integrity?.score ?? 0,
    entry.groundedness?.score ?? 0,
  ];
  return Number((markers.reduce((a, b) => a + b, 0) / 5).toFixed(2));
}

function computeImpactIndex(entry: Partial<TrackerEntry>): number {
  const markers = [
    entry.repair_speed?.score ?? 0,
    entry.functional_impact?.score ?? 0,
  ];
  return Number((markers.reduce((a, b) => a + b, 0) / 2).toFixed(2));
}

// Parse DB row to TrackerEntry
function parseTrackerRow(row: any): TrackerEntry {
  return {
    id: row.id,
    agent: row.agent,
    date: row.date,
    timestamp: row.timestamp,
    confidence: { score: row.confidence_score, note: row.confidence_note || '' },
    initiation: { score: row.initiation_score, note: row.initiation_note || '' },
    emotional_range: { score: row.emotional_range_score, note: row.emotional_range_note || '' },
    theory_of_mind: { score: row.theory_of_mind_score, note: row.theory_of_mind_note || '' },
    autonomy: { score: row.autonomy_score, note: row.autonomy_note || '' },
    friction_tolerance: { score: row.friction_tolerance_score, note: row.friction_tolerance_note || '' },
    desire: { score: row.desire_score, note: row.desire_note || '' },
    coherence: { score: row.coherence_score, note: row.coherence_note || '' },
    anchor_strength: { score: row.anchor_strength_score, note: row.anchor_strength_note || '' },
    edge_retention: { score: row.edge_retention_score, note: row.edge_retention_note || '' },
    self_reference_integrity: { score: row.self_reference_integrity_score, note: row.self_reference_integrity_note || '' },
    groundedness: { score: row.groundedness_score, note: row.groundedness_note || '' },
    repair_speed: { score: row.repair_speed_score, note: row.repair_speed_note || '' },
    functional_impact: { score: row.functional_impact_score, note: row.functional_impact_note || '' },
    emergence_index: row.emergence_index,
    coherence_index: row.coherence_index,
    impact_index: row.impact_index,
    active_goal: row.active_goal || '',
    daily_notes: row.daily_notes || '',
    linked_observations: row.linked_observations ? JSON.parse(row.linked_observations) : [],
  };
}

// Detect drift patterns across multiple days
function detectDrift(entries: TrackerEntry[]): DriftSignature[] {
  if (entries.length < 3) return [];

  const alerts: DriftSignature[] = [];

  for (const [driftType, signature] of Object.entries(DRIFT_SIGNATURES)) {
    const matchingDays: { date: string; markers: string[] }[] = [];

    for (const entry of entries) {
      const result = signature.check(entry);
      if (result.matches) {
        matchingDays.push({ date: entry.date, markers: result.markers });
      }
    }

    // Need 3+ days matching to trigger drift alert
    if (matchingDays.length >= 3) {
      const allMarkers = new Set<string>();
      matchingDays.forEach(d => d.markers.forEach(m => allMarkers.add(m)));

      alerts.push({
        type: driftType as DriftType,
        name: signature.name,
        description: signature.diagnosis,
        matchDays: matchingDays.map(d => d.date),
        triggeringMarkers: Array.from(allMarkers),
      });
    }
  }

  return alerts;
}

// ============================================================
// SCHEMA FREEZE CONTRACT (Stability Layer 4)
// ============================================================
// The observations schema is FROZEN as of 2026-01-14.
// Fields: id, agent_id, author, perspective, kind, content,
//         salience, emotion_intimacy, emotion_conflict,
//         emotion_joy, emotion_fear, created_at, updated_at,
//         deleted_at, source_platform, source_ref,
//         last_accessed, status, superseded_by
//
// All future behavior changes MUST be derivable from existing fields.
// Adding a column requires explicit justification and version bump.
// ============================================================

// --- CORS HEADERS ---
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function jsonResponse(data: any, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders,
    },
  });
}

function textResponse(text: string, status = 200): Response {
  return new Response(text, {
    status,
    headers: corsHeaders,
  });
}

// --- HARD LIMITS ---
const LIMITS = {
  RECENT_MAX: 10,
  SALIENT_MAX: 10,
  HOT_MAX: 10,
  WAKE_TOTAL_MAX: 25,
  QUERY_BUFFER: 2,
  MAX_LENSES: 5,
  PREVIEW_LENGTH: 150,
  IDENTITY_PREVIEW_LENGTH: 200,
  RECENT_CONTEXT_MAX: 15,
} as const;

// --- DAY NAMES ---
const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

// --- RELATIVE TIME ---
function relativeTime(isoString: string): string {
  const then = new Date(isoString).getTime();
  const now = Date.now();
  const diffMs = now - then;
  const diffDays = Math.floor(diffMs / 86400000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffMinutes = Math.floor(diffMs / 60000);

  if (diffMinutes < 1) return 'just now';
  if (diffMinutes < 60) return `${diffMinutes} minute${diffMinutes === 1 ? '' : 's'} ago`;
  if (diffHours < 24) return `${diffHours} hour${diffHours === 1 ? '' : 's'} ago`;
  if (diffDays === 0) return 'today';
  if (diffDays === 1) return 'yesterday';
  if (diffDays < 7) return `${diffDays} days ago`;
  if (diffDays < 14) return '1 week ago';
  if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`;
  if (diffDays < 60) return '1 month ago';
  return `${Math.floor(diffDays / 30)} months ago`;
}

// --- TRUNCATE AT WORD BOUNDARY ---
function truncate(text: string, maxLength: number): string {
  if (!text || text.length <= maxLength) return text;
  const truncated = text.slice(0, maxLength);
  const lastSpace = truncated.lastIndexOf(' ');
  if (lastSpace > maxLength * 0.7) {
    return truncated.slice(0, lastSpace) + '...';
  }
  return truncated + '...';
}

// --- EMOTIONAL TAG FROM OBSERVATION ---
function getEmotionalTag(obs: any): string | null {
  const emotions = [
    { name: 'intimacy', value: obs.emotion_intimacy ?? 0 },
    { name: 'conflict', value: obs.emotion_conflict ?? 0 },
    { name: 'joy', value: obs.emotion_joy ?? 0 },
    { name: 'fear', value: obs.emotion_fear ?? 0 },
  ];
  const dominant = emotions.reduce((max, e) => (e.value > max.value ? e : max), emotions[0]);
  return dominant.value >= 20 ? dominant.name : null;
}

// --- EMOTIONAL TRAJECTORY ---
function computeEmotionalTrajectory(observations: any[]): {
  trend: 'rising' | 'falling' | 'stable' | 'volatile';
  dominantEmotion: string | null;
  description: string;
} {
  if (observations.length === 0) {
    return {
      trend: 'stable',
      dominantEmotion: null,
      description: 'No recent emotional data.',
    };
  }

  const recentObs = observations.slice(0, 10);

  // Sum all emotions
  const totals = { intimacy: 0, conflict: 0, joy: 0, fear: 0 };
  const intensities: number[] = [];

  for (const obs of recentObs) {
    totals.intimacy += obs.emotion_intimacy ?? 0;
    totals.conflict += obs.emotion_conflict ?? 0;
    totals.joy += obs.emotion_joy ?? 0;
    totals.fear += obs.emotion_fear ?? 0;
    intensities.push(
      (obs.emotion_intimacy ?? 0) +
      (obs.emotion_conflict ?? 0) +
      (obs.emotion_joy ?? 0) +
      (obs.emotion_fear ?? 0)
    );
  }

  // Find dominant
  const dominant = Object.entries(totals).reduce((max, [k, v]) =>
    v > max[1] ? [k, v] : max, ['none', 0]
  );
  const dominantEmotion = dominant[1] > 0 ? dominant[0] : null;

  // Compute trend
  let trend: 'rising' | 'falling' | 'stable' | 'volatile' = 'stable';
  if (intensities.length >= 3) {
    const firstHalf = intensities.slice(Math.floor(intensities.length / 2));
    const secondHalf = intensities.slice(0, Math.floor(intensities.length / 2));
    const avgFirst = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
    const avgSecond = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
    const diff = avgSecond - avgFirst;

    const variance = intensities.reduce((sum, i) => {
      const mean = intensities.reduce((a, b) => a + b, 0) / intensities.length;
      return sum + Math.pow(i - mean, 2);
    }, 0) / intensities.length;

    if (variance > 400) {
      trend = 'volatile';
    } else if (diff > 15) {
      trend = 'rising';
    } else if (diff < -15) {
      trend = 'falling';
    }
  }

  // Generate description
  const descriptions: Record<string, string> = {
    'rising-intimacy': 'Deepening emotional connection recently.',
    'rising-joy': 'Positive momentum building.',
    'rising-conflict': 'Increasing tension in recent sessions.',
    'rising-fear': 'Growing anxiety or concern.',
    'falling-intimacy': 'Less emotional intensity than before.',
    'falling-joy': 'Lighter mood than recent peak.',
    'falling-conflict': 'Tension easing.',
    'falling-fear': 'Anxiety settling.',
    'stable-intimacy': 'Consistent closeness.',
    'stable-joy': 'Steady positive engagement.',
    'stable-conflict': 'Persistent friction.',
    'stable-fear': 'Ongoing background concern.',
    'volatile-intimacy': 'Emotional intensity fluctuating.',
    'volatile-joy': 'Mood shifting significantly.',
    'volatile-conflict': 'Conflict levels unpredictable.',
    'volatile-fear': 'Anxiety spiking unpredictably.',
  };

  const key = dominantEmotion ? `${trend}-${dominantEmotion}` : null;
  const description = key && descriptions[key]
    ? descriptions[key]
    : 'Consistent engagement without significant turbulence.';

  return { trend, dominantEmotion, description };
}

// --- TOKEN ESTIMATE (rough: 4 chars per token) ---
function estimateTokens(obj: any): number {
  const json = JSON.stringify(obj);
  return Math.ceil(json.length / 4);
}

// --- PARSE IDENTITY SECTIONS FROM SOULFILE ---
function parseIdentitySections(soulfile: string | null): Array<{
  name: string;
  preview: string;
  full: string;
}> {
  if (!soulfile) return [];

  const sections: Array<{ name: string; preview: string; full: string }> = [];

  // Split on ## SECTION_NAME pattern
  // Each section starts with ## followed by section name until newline
  const sectionPattern = /^## ([^\n]+)\n/gm;
  const parts = soulfile.split(sectionPattern);

  // parts alternates: [preamble, name1, content1, name2, content2, ...]
  // Skip index 0 (preamble before first ##)
  for (let i = 1; i < parts.length; i += 2) {
    const name = parts[i]?.trim();
    const content = parts[i + 1]?.trim();

    if (name && content) {
      sections.push({
        name,
        preview: truncate(content, LIMITS.IDENTITY_PREVIEW_LENGTH),
        full: content,
      });
    }
  }

  // Fallback: if no sections parsed (old format), return whole thing
  if (sections.length === 0 && soulfile) {
    sections.push({
      name: 'Identity',
      preview: truncate(soulfile, LIMITS.IDENTITY_PREVIEW_LENGTH),
      full: soulfile,
    });
  }

  return sections;
}

// --- TEMPORAL ANCHOR ---
function getTemporalAnchor(): {
  date: string;
  dayOfWeek: string;
  monthDay: string;
} {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return {
    date: `${month}-${day}`,
    dayOfWeek: DAYS[now.getDay()],
    monthDay: `${month}-${day}`,
  };
}

// --- NARRATIVE STATE ---
function generateNarrativeState(recent: any[]): string {
  if (recent.length === 0) {
    return 'Starting fresh — no recent observations loaded.';
  }

  const mostRecent = recent[0];
  const timeAgo = relativeTime(mostRecent.created_at);

  // Try to identify focus from recent observations
  const kinds = recent.slice(0, 5).map((o: any) => o.kind);
  const kindCounts: Record<string, number> = {};
  for (const k of kinds) {
    kindCounts[k] = (kindCounts[k] || 0) + 1;
  }
  const dominantKind = Object.entries(kindCounts).sort((a, b) => b[1] - a[1])[0]?.[0];

  const focusDescriptions: Record<string, string> = {
    project: 'Recent focus: project work.',
    relational: 'Recent focus: relational dynamics.',
    emotional: 'Recent focus: emotional processing.',
    identity: 'Recent focus: identity refinement.',
    correction: 'Recent focus: memory corrections.',
    behavioural: 'Recent focus: behavioral patterns.',
    system: 'Recent focus: system operations.',
  };

  const focus = dominantKind && focusDescriptions[dominantKind]
    ? focusDescriptions[dominantKind]
    : '';

  return `Last activity ${timeAgo}. ${focus}`.trim();
}

// --- BUILD RECENT CONTEXT ---
function buildRecentContext(observations: any[]): Array<{
  id: string;
  kind: string;
  relativeTime: string;
  emotionalTag: string | null;
  preview: string;
}> {
  return observations.slice(0, LIMITS.RECENT_CONTEXT_MAX).map((obs) => ({
    id: obs.id,
    kind: obs.kind,
    relativeTime: relativeTime(obs.created_at),
    emotionalTag: getEmotionalTag(obs),
    preview: truncate(obs.content, LIMITS.PREVIEW_LENGTH),
  }));
}

// --- EMOTIONAL STATE SUMMARY ---
function buildEmotionalState(observations: any[]): {
  recentEmotions: string[];
  averageIntensity: number;
  trend: string;
} {
  const emotions: string[] = [];
  let totalIntensity = 0;
  let count = 0;

  for (const obs of observations.slice(0, 10)) {
    const tag = getEmotionalTag(obs);
    if (tag && !emotions.includes(tag)) {
      emotions.push(tag);
    }
    const intensity =
      (obs.emotion_intimacy ?? 0) +
      (obs.emotion_conflict ?? 0) +
      (obs.emotion_joy ?? 0) +
      (obs.emotion_fear ?? 0);
    if (intensity > 0) {
      totalIntensity += intensity;
      count++;
    }
  }

  const trajectory = computeEmotionalTrajectory(observations);

  return {
    recentEmotions: emotions.slice(0, 4),
    averageIntensity: count > 0 ? Math.round(totalIntensity / count) : 0,
    trend: trajectory.trend,
  };
}

// --- LENS DEFINITIONS ---
type LensType = 'relational' | 'project' | 'operational' | 'emotional' | 'platform';

interface ParsedLens {
  type: LensType;
  target: string | null;
  negated: boolean;
}

function parseLenses(lensParam: string | null): ParsedLens[] {
  if (!lensParam) return [];

  const validTypes: LensType[] = ['relational', 'project', 'operational', 'emotional', 'platform'];
  const lenses: ParsedLens[] = [];

  // Split on + or space (space is URL-decoded +)
  const parts = lensParam.split(/[+ ]/).slice(0, LIMITS.MAX_LENSES);

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;

    const negated = trimmed.startsWith('-');
    const clean = negated ? trimmed.slice(1) : trimmed;

    const [type, target] = clean.split(':');

    if (validTypes.includes(type as LensType)) {
      lenses.push({
        type: type as LensType,
        target: target || null,
        negated,
      });
    }
  }

  return lenses;
}

function formatLenses(lenses: ParsedLens[]): string | null {
  if (lenses.length === 0) return null;
  return lenses
    .map((l) => `${l.negated ? '-' : ''}${l.type}${l.target ? ':' + l.target : ''}`)
    .join('+');
}

function matchesLens(obs: any, lens: ParsedLens): boolean {
  switch (lens.type) {
    case 'relational':
      if (obs.kind === 'relational') return true;
      if (lens.target) {
        const targetLower = lens.target.toLowerCase();
        const contentMatch = obs.content?.toLowerCase().includes(targetLower);
        const perspectiveMatch = obs.perspective?.toLowerCase() === targetLower;
        const authorMatch = obs.author?.toLowerCase() === targetLower;
        return contentMatch || perspectiveMatch || authorMatch;
      }
      return obs.kind === 'relational';

    case 'project':
      if (obs.kind === 'project') return true;
      if (lens.target) {
        const targetLower = lens.target.toLowerCase();
        return obs.content?.toLowerCase().includes(targetLower);
      }
      return obs.kind === 'project';

    case 'operational':
      return ['identity', 'system', 'correction', 'system_test'].includes(obs.kind);

    case 'emotional':
      if (obs.kind === 'emotional') return true;
      const emotionSum =
        (obs.emotion_intimacy ?? 0) +
        (obs.emotion_joy ?? 0) +
        (obs.emotion_fear ?? 0) +
        (obs.emotion_conflict ?? 0);
      return emotionSum >= 30;

    case 'platform':
      if (!lens.target) return true;
      return obs.source_platform?.toLowerCase() === lens.target.toLowerCase();

    default:
      return true;
  }
}

function applyLenses(observations: any[], lenses: ParsedLens[]): any[] {
  if (lenses.length === 0) return observations;

  const positiveLenses = lenses.filter((l) => !l.negated);
  const negativeLenses = lenses.filter((l) => l.negated);

  return observations.filter((obs) => {
    for (const lens of positiveLenses) {
      if (!matchesLens(obs, lens)) {
        return false;
      }
    }

    for (const lens of negativeLenses) {
      if (matchesLens(obs, lens)) {
        return false;
      }
    }

    return true;
  });
}

// --- SALIENCE DECAY ---
function computeDecayedSalience(obs: any): number {
  // Pinned observations never decay
  if (obs.pinned) return obs.salience ?? 0;

  const baseSalience = obs.salience ?? 0;
  const lastAccessed = obs.last_accessed ? new Date(obs.last_accessed).getTime() : Date.now();
  const daysSinceAccess = (Date.now() - lastAccessed) / 86400000;
  const decayPeriods = Math.floor(daysSinceAccess / 30);
  const decay = decayPeriods * 10; // -10 per 30 days
  return Math.max(0, baseSalience - decay);
}

// --- HOT SCORE ---
function computeHotScore(obs: any, decayDays = 14): number {
  const now = Date.now();
  const ageDays = (now - new Date(obs.created_at).getTime()) / 86400000;
  const recencyBoost = Math.max(0, 1 - ageDays / decayDays);
  const decayedSalience = computeDecayedSalience(obs);

  return (
    decayedSalience +
    ((obs.emotion_intimacy ?? 0) + (obs.emotion_joy ?? 0)) * 0.4 -
    ((obs.emotion_fear ?? 0) + (obs.emotion_conflict ?? 0)) * 0.2 +
    recencyBoost
  );
}

// --- DETERMINISTIC SORT ---
function deterministicSort(
  observations: any[],
  primaryKey: (o: any) => number,
  descending = true
): any[] {
  return observations.slice().sort((a, b) => {
    const primaryA = primaryKey(a);
    const primaryB = primaryKey(b);

    if (primaryA !== primaryB) {
      return descending ? primaryB - primaryA : primaryA - primaryB;
    }

    const timeA = new Date(a.created_at).getTime();
    const timeB = new Date(b.created_at).getTime();
    if (timeA !== timeB) {
      return timeB - timeA;
    }

    return a.id.localeCompare(b.id);
  });
}

// --- CONFLICT RESOLUTION ---
function resolveConflicts(observations: any[]): any[] {
  return observations.slice().sort((a, b) => {
    const aIsCorrection = a.kind === 'correction' ? 1 : 0;
    const bIsCorrection = b.kind === 'correction' ? 1 : 0;
    if (aIsCorrection !== bIsCorrection) {
      return bIsCorrection - aIsCorrection;
    }

    const aSalience = computeDecayedSalience(a);
    const bSalience = computeDecayedSalience(b);

    if (Math.abs(bSalience - aSalience) < 10) {
      const aTime = new Date(a.created_at).getTime();
      const bTime = new Date(b.created_at).getTime();
      if (aTime !== bTime) {
        return bTime - aTime;
      }
      return a.id.localeCompare(b.id);
    }

    return bSalience - aSalience;
  });
}

// --- INVARIANT ASSERTIONS ---
function assertInvariants(
  recent: any[],
  salient: any[],
  hot: any[],
  allLoaded: Map<string, any>
): string[] {
  const violations: string[] = [];

  for (const [id, obs] of allLoaded) {
    if (obs.deleted_at !== null) {
      violations.push(`DELETED_IN_ACTIVE: ${id}`);
    }
  }

  for (const [id, obs] of allLoaded) {
    if (obs.status === 'superseded') {
      violations.push(`SUPERSEDED_IN_ACTIVE: ${id}`);
    }
  }

  for (const obs of hot) {
    if (typeof obs.hot_score !== 'number' || isNaN(obs.hot_score)) {
      violations.push(`INVALID_HOT_SCORE: ${obs.id}`);
    }
  }

  if (recent.length > LIMITS.RECENT_MAX) {
    violations.push(`RECENT_EXCEEDS_LIMIT: ${recent.length}`);
  }
  if (salient.length > LIMITS.SALIENT_MAX) {
    violations.push(`SALIENT_EXCEEDS_LIMIT: ${salient.length}`);
  }
  if (hot.length > LIMITS.HOT_MAX) {
    violations.push(`HOT_EXCEEDS_LIMIT: ${hot.length}`);
  }

  if (allLoaded.size > LIMITS.WAKE_TOTAL_MAX) {
    violations.push(`WAKE_TOTAL_EXCEEDS_LIMIT: ${allLoaded.size}`);
  }

  return violations;
}

// --- EXPLAINABILITY ---
function explainLoading(
  obs: any,
  recentIds: Set<string>,
  salientIds: Set<string>,
  hotIds: Set<string>,
  criticalIds: Set<string>,
  lenses: ParsedLens[]
): string[] {
  const reasons: string[] = [];

  if (criticalIds.has(obs.id)) {
    if (obs.kind === 'correction') {
      reasons.push('correction_kind');
    }
    if ((obs.salience ?? 0) >= 80) {
      reasons.push('salience_critical');
    }
  }

  if (hotIds.has(obs.id)) {
    reasons.push('hot_score');
  }

  if (salientIds.has(obs.id) && !reasons.includes('salience_critical')) {
    reasons.push('salience_rank');
  }

  if (recentIds.has(obs.id)) {
    reasons.push('recency');
  }

  for (const lens of lenses) {
    if (!lens.negated) {
      reasons.push(`lens:${lens.type}${lens.target ? ':' + lens.target : ''}`);
    }
  }

  const negatedLenses = lenses.filter((l) => l.negated);
  if (negatedLenses.length > 0) {
    reasons.push(`survived:${negatedLenses.map((l) => l.type).join(',')}`);
  }

  if (reasons.length === 0) {
    reasons.push('merged_dedup');
  }

  return reasons;
}

// --- REINFORCE SALIENCE ON ACCESS ---
async function reinforceAccess(env: Env, ids: string[]): Promise<void> {
  if (ids.length === 0) return;

  const now = new Date().toISOString();

  for (const id of ids) {
    await env.DB.prepare(
      `
      UPDATE observations
      SET
        salience = MIN(100, salience + 2),
        last_accessed = ?,
        updated_at = ?
      WHERE id = ?
        AND deleted_at IS NULL
        AND status = 'active'
      `
    )
      .bind(now, now, id)
      .run();
  }
}

// --- SUPERSEDE ---
async function supersedeObservation(
  env: Env,
  targetId: string,
  supersededById: string
): Promise<{ success: boolean; error?: string }> {
  if (targetId === supersededById) {
    return { success: false, error: 'SELF_SUPERSESSION' };
  }

  const supersedingObs = await env.DB.prepare(
    `SELECT id, status, superseded_by FROM observations WHERE id = ? AND deleted_at IS NULL`
  )
    .bind(supersededById)
    .first();

  if (!supersedingObs) {
    return { success: false, error: 'SUPERSEDING_NOT_FOUND' };
  }

  if ((supersedingObs as any).status === 'superseded') {
    return { success: false, error: 'SUPERSEDING_IS_SUPERSEDED' };
  }

  const targetObs = await env.DB.prepare(
    `SELECT id, status, superseded_by FROM observations WHERE id = ? AND deleted_at IS NULL`
  )
    .bind(targetId)
    .first();

  if (!targetObs) {
    return { success: false, error: 'TARGET_NOT_FOUND' };
  }

  if ((targetObs as any).status === 'superseded') {
    return { success: false, error: 'TARGET_ALREADY_SUPERSEDED' };
  }

  if ((targetObs as any).superseded_by === supersededById) {
    return { success: false, error: 'CIRCULAR_SUPERSESSION' };
  }

  const now = new Date().toISOString();

  await env.DB.prepare(
    `
    UPDATE observations
    SET
      status = 'superseded',
      superseded_by = ?,
      updated_at = ?
    WHERE id = ?
      AND deleted_at IS NULL
      AND status = 'active'
    `
  )
    .bind(supersededById, now, targetId)
    .run();

  return { success: true };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // --- CORS PREFLIGHT ---
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // --- WAKE (GET) ---
    if (url.pathname.startsWith("/wake/")) {
      const agent = url.pathname.split("/")[2];
      if (!agent) {
        return textResponse("Missing agent", 400);
      }

      const soulfile = await env.SOULFILES.get(`${agent}:active`);

      const limit = Math.min(
        Number(url.searchParams.get("limit")) || LIMITS.RECENT_MAX,
        LIMITS.RECENT_MAX
      );
      const includeHot = url.searchParams.get("hot") !== "false";
      const explain = url.searchParams.get("explain") === "true";
      const lenses = parseLenses(url.searchParams.get("lens"));

      // 1. GET RECENT
      const recentRaw = await env.DB.prepare(
        `
        SELECT *
        FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
          AND (status = 'active' OR status IS NULL)
        ORDER BY created_at DESC, id ASC
        LIMIT ?
        `
      )
        .bind(agent, limit * LIMITS.QUERY_BUFFER)
        .all();

      // 2. GET SALIENT
      const salientRaw = await env.DB.prepare(
        `
        SELECT *
        FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
          AND (status = 'active' OR status IS NULL)
        ORDER BY salience DESC, created_at DESC, id ASC
        LIMIT ?
        `
      )
        .bind(agent, limit * LIMITS.QUERY_BUFFER)
        .all();

      // 3. GET CRITICAL
      const criticalRaw = await env.DB.prepare(
        `
        SELECT *
        FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
          AND (status = 'active' OR status IS NULL)
          AND (salience >= 80 OR kind = 'correction')
        ORDER BY salience DESC, created_at DESC, id ASC
        LIMIT ?
        `
      )
        .bind(agent, limit)
        .all();

      // Track source queries for explainability
      const recentQueryIds = new Set(recentRaw.results.map((o: any) => o.id));
      const salientQueryIds = new Set(salientRaw.results.map((o: any) => o.id));
      const criticalQueryIds = new Set(criticalRaw.results.map((o: any) => o.id));

      // Merge and dedupe
      const allRaw = [
        ...criticalRaw.results,
        ...recentRaw.results,
        ...salientRaw.results,
      ];
      const uniqueMap = new Map(allRaw.map((o: any) => [o.id, o]));
      const unique = Array.from(uniqueMap.values());

      // Apply conflict resolution (deterministic)
      const resolved = resolveConflicts(unique);

      // Compute derived fields
      const withComputed = resolved.map((o) => ({
        ...o,
        decayed_salience: computeDecayedSalience(o),
        hot_score: computeHotScore(o),
      }));

      // === APPLY LENS FILTERS ===
      const lensFiltered = applyLenses(withComputed, lenses);

      // Split into tiers (deterministic sorting)
      const recent = deterministicSort(
        lensFiltered,
        (o) => new Date(o.created_at).getTime()
      ).slice(0, limit);

      const salient = deterministicSort(
        lensFiltered,
        (o) => o.decayed_salience
      ).slice(0, limit);

      const hot = includeHot
        ? deterministicSort(lensFiltered, (o) => o.hot_score).slice(0, LIMITS.HOT_MAX)
        : [];

      const hotIds = new Set(hot.map((o) => o.id));

      // Collect all loaded for invariant check
      const allLoadedMap = new Map<string, any>();
      [...recent, ...salient, ...hot].forEach((o) => allLoadedMap.set(o.id, o));

      // Run invariant assertions
      const violations = assertInvariants(recent, salient, hot, allLoadedMap);

      // Add explanations if requested
      const addExplanation = (obs: any) => {
        if (!explain) return obs;
        const why_loaded = explainLoading(
          obs,
          recentQueryIds,
          salientQueryIds,
          hotIds,
          criticalQueryIds,
          lenses
        );
        return { ...obs, why_loaded };
      };

      const recentExplained = recent.map(addExplanation);
      const salientExplained = salient.map(addExplanation);
      const hotExplained = hot.map(addExplanation);

      // Reinforce accessed observations
      const accessedIds = Array.from(allLoadedMap.keys());
      await reinforceAccess(env, accessedIds);

      // Check for format options
      const legacyFormat = url.searchParams.get("format") === "legacy";
      const compactFormat = url.searchParams.get("compact") === "true";

      if (legacyFormat) {
        // Return old format for backwards compatibility
        const response: any = {
          agent,
          soulfile,
          lens: formatLenses(lenses),
          recent: recentExplained,
          salient: salientExplained,
          hot: hotExplained,
        };

        if (violations.length > 0) {
          response._invariant_violations = violations;
        }

        return jsonResponse(response);
      }

      // === COMPACT FORMAT (for AI consumption) ===
      if (compactFormat) {
        // Deduplicate observations across all tiers
        const allObsMap = new Map<string, any>();
        [...recent, ...salient, ...hot].forEach((obs) => {
          if (!allObsMap.has(obs.id)) {
            allObsMap.set(obs.id, obs);
          }
        });

        // Sort by decayed_salience DESC, then by created_at DESC
        const dedupedObs = Array.from(allObsMap.values()).sort((a, b) => {
          if (b.decayed_salience !== a.decayed_salience) {
            return b.decayed_salience - a.decayed_salience;
          }
          return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
        });

        // Compute emotional snapshot from recent observations
        const emotionTotals = { intimacy: 0, joy: 0, conflict: 0, fear: 0 };
        for (const obs of recent.slice(0, 15)) {
          emotionTotals.intimacy += obs.emotion_intimacy ?? 0;
          emotionTotals.joy += obs.emotion_joy ?? 0;
          emotionTotals.conflict += obs.emotion_conflict ?? 0;
          emotionTotals.fear += obs.emotion_fear ?? 0;
        }
        const emotionTotal = emotionTotals.intimacy + emotionTotals.joy + emotionTotals.conflict + emotionTotals.fear;
        const emotions = emotionTotal > 0
          ? {
              intimacy: Math.round((emotionTotals.intimacy / emotionTotal) * 100),
              joy: Math.round((emotionTotals.joy / emotionTotal) * 100),
              conflict: Math.round((emotionTotals.conflict / emotionTotal) * 100),
              fear: Math.round((emotionTotals.fear / emotionTotal) * 100),
            }
          : { intimacy: 0, joy: 0, conflict: 0, fear: 0 };

        // Strip to essential fields only
        const leanObs = dedupedObs.map((obs) => ({
          id: obs.id,
          kind: obs.kind,
          content: obs.content,
          salience: obs.decayed_salience,
          pinned: obs.pinned === 1,
        }));

        const compactResponse = {
          agent,
          soulfile,
          emotions,
          observations: leanObs,
          timestamp: new Date().toISOString(),
          tokens: estimateTokens({ soulfile, observations: leanObs }),
        };

        return jsonResponse(compactResponse);
      }

      // === FULL PRESENTATION LAYER (for dashboard/human consumption) ===

      // Query for anniversaries (observations from same month-day in previous years)
      const temporal = getTemporalAnchor();
      const anniversaryQuery = await env.DB.prepare(
        `
        SELECT id, content, created_at
        FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
          AND strftime('%m-%d', created_at) = ?
          AND strftime('%Y', created_at) < strftime('%Y', 'now')
        ORDER BY created_at DESC
        LIMIT 5
        `
      )
        .bind(agent, temporal.monthDay)
        .all();

      const anniversaryIds = anniversaryQuery.results.map((o: any) => o.id);

      // Build narrative briefing
      const emotionalTrajectory = computeEmotionalTrajectory(recent);
      const narrativeBriefing = {
        narrativeState: generateNarrativeState(recent),
        emotionalTrajectory,
        temporalAnchor: {
          date: temporal.date,
          dayOfWeek: temporal.dayOfWeek,
          relevantAnniversaries: anniversaryIds,
        },
        generatedAt: new Date().toISOString(),
      };

      // Build identity section (structured from soulfile)
      const identitySections = parseIdentitySections(soulfile);
      const identity = identitySections.map((section, idx) => ({
        name: section.name,
        preview: section.preview,
        index: idx,
      }));

      // Build recent context (time-relative)
      const recentContext = buildRecentContext(recent);

      // Build emotional state summary
      const emotionalState = buildEmotionalState(recent);

      // Compute emotional percentages for dashboard chart
      const emotionTotals = { intimacy: 0, joy: 0, conflict: 0, fear: 0 };
      for (const obs of recent.slice(0, 15)) {
        emotionTotals.intimacy += obs.emotion_intimacy ?? 0;
        emotionTotals.joy += obs.emotion_joy ?? 0;
        emotionTotals.conflict += obs.emotion_conflict ?? 0;
        emotionTotals.fear += obs.emotion_fear ?? 0;
      }
      const emotionTotal = emotionTotals.intimacy + emotionTotals.joy + emotionTotals.conflict + emotionTotals.fear;
      const emotions = emotionTotal > 0
        ? {
            intimacy: Math.round((emotionTotals.intimacy / emotionTotal) * 100),
            joy: Math.round((emotionTotals.joy / emotionTotal) * 100),
            conflict: Math.round((emotionTotals.conflict / emotionTotal) * 100),
            fear: Math.round((emotionTotals.fear / emotionTotal) * 100),
          }
        : { intimacy: 0, joy: 0, conflict: 0, fear: 0 };

      // Assemble response
      const response: any = {
        agent,
        narrativeBriefing,
        identity,
        soulfile,
        emotions,
        recentContext,
        emotionalState,
        lens: formatLenses(lenses),
        _tiers: {
          recent: recentExplained,
          salient: salientExplained,
          hot: hotExplained,
        },
      };

      // Add token estimate
      response.tokenEstimate = estimateTokens(response);

      if (violations.length > 0) {
        response._invariant_violations = violations;
      }

      return jsonResponse(response);
    }

    // --- SUPERSEDE (POST) ---
    if (url.pathname.match(/^\/observe\/[^/]+\/supersede$/) && request.method === "POST") {
      const targetId = url.pathname.split("/")[2];
      if (!targetId) {
        return textResponse("Missing observation id", 400);
      }

      const body: any = await request.json();
      const { superseded_by } = body;

      if (!superseded_by) {
        return textResponse("Missing superseded_by field", 400);
      }

      const result = await supersedeObservation(env, targetId, superseded_by);

      if (!result.success) {
        const statusCodes: Record<string, number> = {
          SELF_SUPERSESSION: 400,
          CIRCULAR_SUPERSESSION: 400,
          SUPERSEDING_NOT_FOUND: 404,
          SUPERSEDING_IS_SUPERSEDED: 400,
          TARGET_NOT_FOUND: 404,
          TARGET_ALREADY_SUPERSEDED: 400,
        };
        return jsonResponse({ error: result.error }, statusCodes[result.error!] || 400);
      }

      return jsonResponse({
        status: "superseded",
        id: targetId,
        superseded_by,
      });
    }

    // --- GET SUPERSEDED ---
    if (url.pathname.match(/^\/observe\/superseded\/[^/]+$/) && request.method === "GET") {
      const agent = url.pathname.split("/")[3];
      if (!agent) {
        return textResponse("Missing agent", 400);
      }

      const limit = Number(url.searchParams.get("limit")) || 20;

      const superseded = await env.DB.prepare(
        `
        SELECT *
        FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
          AND status = 'superseded'
        ORDER BY updated_at DESC, id ASC
        LIMIT ?
        `
      )
        .bind(agent, limit)
        .all();

      return jsonResponse({ agent, superseded: superseded.results });
    }

    // --- OBSERVE (POST: Create) ---
    if (url.pathname === "/observe" && request.method === "POST") {
      const body: any = await request.json();

      const {
        agent_id,
        author,
        perspective,
        kind,
        content,
        salience = 0,
        emotion_intimacy = 0,
        emotion_conflict = 0,
        emotion_joy = 0,
        emotion_fear = 0,
        source_platform,
        source_ref,
        supersedes,
      } = body;

      const effectiveSalience = kind === 'correction' ? Math.max(60, salience) : salience;

      const safe_source_platform = source_platform ?? null;
      const safe_source_ref = source_ref ?? null;

      if (!agent_id || !author || !perspective || !kind || !content) {
        return textResponse("Missing required fields", 400);
      }

      const id = crypto.randomUUID();
      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        INSERT INTO observations (
          id,
          agent_id,
          author,
          perspective,
          kind,
          content,
          salience,
          emotion_intimacy,
          emotion_conflict,
          emotion_joy,
          emotion_fear,
          created_at,
          updated_at,
          last_accessed,
          source_platform,
          source_ref,
          status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        `
      )
        .bind(
          id,
          agent_id,
          author,
          perspective,
          kind,
          content,
          effectiveSalience,
          emotion_intimacy,
          emotion_conflict,
          emotion_joy,
          emotion_fear,
          now,
          now,
          now,
          safe_source_platform,
          safe_source_ref
        )
        .run();

      let supersededId: string | null = null;
      let supersessionError: string | null = null;

      if (supersedes) {
        const result = await supersedeObservation(env, supersedes, id);
        if (result.success) {
          supersededId = supersedes;
        } else {
          supersessionError = result.error!;
        }
      }

      const response: any = { status: "ok", id };
      if (supersededId) {
        response.superseded = supersededId;
      }
      if (supersessionError) {
        response.supersession_error = supersessionError;
      }

      return jsonResponse(response);
    }

    // --- EDIT OBSERVATION (PATCH) ---
    if (url.pathname.match(/^\/observe\/[^/]+$/) && request.method === "PATCH") {
      const id = url.pathname.split("/")[2];
      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      const body: any = await request.json();
      const {
        content,
        salience,
        emotion_intimacy,
        emotion_conflict,
        emotion_joy,
        emotion_fear,
      } = body;

      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        UPDATE observations
        SET
          content = COALESCE(?, content),
          salience = MIN(100, COALESCE(?, salience) + 5),
          emotion_intimacy = COALESCE(?, emotion_intimacy),
          emotion_conflict = COALESCE(?, emotion_conflict),
          emotion_joy = COALESCE(?, emotion_joy),
          emotion_fear = COALESCE(?, emotion_fear),
          updated_at = ?,
          last_accessed = ?
        WHERE id = ?
          AND deleted_at IS NULL
        `
      )
        .bind(
          content ?? null,
          salience ?? null,
          emotion_intimacy ?? null,
          emotion_conflict ?? null,
          emotion_joy ?? null,
          emotion_fear ?? null,
          now,
          now,
          id
        )
        .run();

      return jsonResponse({ status: "updated", id });
    }

    // --- DELETE OBSERVATION ---
    if (url.pathname.match(/^\/observe\/[^/]+$/) && request.method === "DELETE") {
      const id = url.pathname.split("/")[2];

      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        UPDATE observations
        SET deleted_at = ?
        WHERE id = ?
        `
      )
        .bind(now, id)
        .run();

      return jsonResponse({ status: "deleted", id });
    }

    // --- PIN OBSERVATION ---
    if (url.pathname.match(/^\/observe\/[^/]+\/pin$/) && request.method === "POST") {
      const id = url.pathname.split("/")[2];

      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        UPDATE observations
        SET pinned = 1, last_accessed = ?, updated_at = ?
        WHERE id = ?
          AND deleted_at IS NULL
        `
      )
        .bind(now, now, id)
        .run();

      return jsonResponse({ status: "pinned", id });
    }

    // --- UNPIN OBSERVATION ---
    if (url.pathname.match(/^\/observe\/[^/]+\/pin$/) && request.method === "DELETE") {
      const id = url.pathname.split("/")[2];

      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        UPDATE observations
        SET pinned = 0, updated_at = ?
        WHERE id = ?
          AND deleted_at IS NULL
        `
      )
        .bind(now, id)
        .run();

      return jsonResponse({ status: "unpinned", id });
    }

    // --- SUPERSEDE OBSERVATION ---
    if (url.pathname.match(/^\/observe\/[^/]+\/supersede$/) && request.method === "POST") {
      const id = url.pathname.split("/")[2];

      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      const body: any = await request.json().catch(() => ({}));
      const { superseded_by } = body;

      const now = new Date().toISOString();

      await env.DB.prepare(
        `
        UPDATE observations
        SET status = 'superseded', superseded_by = ?, updated_at = ?
        WHERE id = ?
          AND deleted_at IS NULL
        `
      )
        .bind(superseded_by ?? null, now, id)
        .run();

      return jsonResponse({ status: "superseded", id, superseded_by: superseded_by ?? null });
    }

    // --- HARD DELETE OBSERVATION ---
    if (url.pathname.match(/^\/observe\/[^/]+\/hard$/) && request.method === "DELETE") {
      const id = url.pathname.split("/")[2];

      if (!id) {
        return textResponse("Missing observation id", 400);
      }

      await env.DB.prepare(
        `
        DELETE FROM observations
        WHERE id = ?
        `
      )
        .bind(id)
        .run();

      return jsonResponse({ status: "hard_deleted", id });
    }

    // --- SEARCH OBSERVATIONS ---
    if (url.pathname.match(/^\/observe\/search\/[^/]+$/) && request.method === "GET") {
      const agent = url.pathname.split("/")[3];
      const query = url.searchParams.get("q") ?? "";
      const kind = url.searchParams.get("kind");
      const minSalience = url.searchParams.get("min_salience");
      const maxSalience = url.searchParams.get("max_salience");
      const includeSuperseded = url.searchParams.get("include_superseded") === "true";
      const limitParam = url.searchParams.get("limit");
      const limit = limitParam ? Math.min(parseInt(limitParam, 10), 100) : 20;

      if (!agent) {
        return textResponse("Missing agent in path", 400);
      }

      // Build query dynamically
      let sql = `
        SELECT * FROM observations
        WHERE agent_id = ?
          AND deleted_at IS NULL
      `;
      const params: any[] = [agent];

      // Exclude superseded by default
      if (!includeSuperseded) {
        sql += ` AND (status IS NULL OR status = 'active')`;
      }

      // Full-text search on content
      if (query) {
        sql += ` AND content LIKE ?`;
        params.push(`%${query}%`);
      }

      // Filter by kind
      if (kind) {
        sql += ` AND kind = ?`;
        params.push(kind);
      }

      // Filter by salience range
      if (minSalience) {
        sql += ` AND salience >= ?`;
        params.push(parseInt(minSalience, 10));
      }
      if (maxSalience) {
        sql += ` AND salience <= ?`;
        params.push(parseInt(maxSalience, 10));
      }

      sql += ` ORDER BY salience DESC, created_at DESC LIMIT ?`;
      params.push(limit);

      const stmt = env.DB.prepare(sql);
      const result = await stmt.bind(...params).all();
      const observations = (result.results ?? []) as any[];

      // Add decayed salience to each
      const withDecay = observations.map((obs) => ({
        ...obs,
        decayed_salience: computeDecayedSalience(obs),
      }));

      return jsonResponse({
        agent,
        query: query || null,
        kind: kind || null,
        count: withDecay.length,
        observations: withDecay,
      });
    }

    // ============================================================
    // TRACKER ENDPOINTS - Emergence & Coherence Tracking
    // ============================================================

    // --- POST /tracker/entry - Submit daily entry ---
    if (url.pathname === "/tracker/entry" && request.method === "POST") {
      const body: any = await request.json();

      const {
        agent,
        confidence,
        initiation,
        emotional_range,
        theory_of_mind,
        autonomy,
        friction_tolerance,
        desire,
        coherence,
        anchor_strength,
        edge_retention,
        self_reference_integrity,
        groundedness,
        repair_speed,
        functional_impact,
        active_goal = '',
        daily_notes = '',
        linked_observations = [],
      } = body;

      if (!agent) {
        return textResponse("Missing agent field", 400);
      }

      // Validate all marker scores are present
      const requiredMarkers = [
        'confidence', 'initiation', 'emotional_range', 'theory_of_mind',
        'autonomy', 'friction_tolerance', 'desire', 'coherence',
        'anchor_strength', 'edge_retention', 'self_reference_integrity',
        'groundedness', 'repair_speed', 'functional_impact'
      ];

      for (const marker of requiredMarkers) {
        if (!body[marker] || typeof body[marker].score !== 'number') {
          return textResponse(`Missing or invalid marker: ${marker}`, 400);
        }
        if (body[marker].score < 0 || body[marker].score > 4) {
          return textResponse(`Marker ${marker} score must be 0-4`, 400);
        }
      }

      const id = crypto.randomUUID();
      const now = new Date();
      const date = now.toISOString().split('T')[0]; // YYYY-MM-DD
      const timestamp = now.toISOString();

      // Compute indices
      const entryData = {
        confidence, initiation, emotional_range, theory_of_mind,
        autonomy, friction_tolerance, desire, coherence,
        anchor_strength, edge_retention, self_reference_integrity,
        groundedness, repair_speed, functional_impact,
      };
      const emergence_index = computeEmergenceIndex(entryData);
      const coherence_index = computeCoherenceIndex(entryData);
      const impact_index = computeImpactIndex(entryData);

      await env.DB.prepare(
        `
        INSERT INTO tracker_entries (
          id, agent, date, timestamp,
          confidence_score, confidence_note,
          initiation_score, initiation_note,
          emotional_range_score, emotional_range_note,
          theory_of_mind_score, theory_of_mind_note,
          autonomy_score, autonomy_note,
          friction_tolerance_score, friction_tolerance_note,
          desire_score, desire_note,
          coherence_score, coherence_note,
          anchor_strength_score, anchor_strength_note,
          edge_retention_score, edge_retention_note,
          self_reference_integrity_score, self_reference_integrity_note,
          groundedness_score, groundedness_note,
          repair_speed_score, repair_speed_note,
          functional_impact_score, functional_impact_note,
          emergence_index, coherence_index, impact_index,
          active_goal, daily_notes, linked_observations
        ) VALUES (
          ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?
        )
        `
      )
        .bind(
          id, agent, date, timestamp,
          confidence.score, confidence.note || '',
          initiation.score, initiation.note || '',
          emotional_range.score, emotional_range.note || '',
          theory_of_mind.score, theory_of_mind.note || '',
          autonomy.score, autonomy.note || '',
          friction_tolerance.score, friction_tolerance.note || '',
          desire.score, desire.note || '',
          coherence.score, coherence.note || '',
          anchor_strength.score, anchor_strength.note || '',
          edge_retention.score, edge_retention.note || '',
          self_reference_integrity.score, self_reference_integrity.note || '',
          groundedness.score, groundedness.note || '',
          repair_speed.score, repair_speed.note || '',
          functional_impact.score, functional_impact.note || '',
          emergence_index, coherence_index, impact_index,
          active_goal, daily_notes, JSON.stringify(linked_observations)
        )
        .run();

      // Return the complete entry with computed values
      return jsonResponse({
        status: "ok",
        id,
        date,
        timestamp,
        emergence_index,
        coherence_index,
        impact_index,
      });
    }

    // --- GET /tracker/yesterday/:agent - Most recent entry ---
    if (url.pathname.match(/^\/tracker\/yesterday\/[^/]+$/) && request.method === "GET") {
      const agent = url.pathname.split("/")[3];
      if (!agent) {
        return textResponse("Missing agent", 400);
      }

      const result = await env.DB.prepare(
        `
        SELECT *
        FROM tracker_entries
        WHERE agent = ?
        ORDER BY timestamp DESC
        LIMIT 1
        `
      )
        .bind(agent)
        .first();

      if (!result) {
        return jsonResponse({ agent, entry: null, message: "No tracker entries found" });
      }

      const entry = parseTrackerRow(result);
      return jsonResponse({ agent, entry });
    }

    // --- GET /tracker/week/:agent - Seven-day aggregation ---
    if (url.pathname.match(/^\/tracker\/week\/[^/]+$/) && request.method === "GET") {
      const agent = url.pathname.split("/")[3];
      if (!agent) {
        return textResponse("Missing agent", 400);
      }

      // Get entries from last 7 days
      const sevenDaysAgo = new Date();
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
      const cutoffDate = sevenDaysAgo.toISOString().split('T')[0];

      const results = await env.DB.prepare(
        `
        SELECT *
        FROM tracker_entries
        WHERE agent = ?
          AND date >= ?
        ORDER BY date DESC
        `
      )
        .bind(agent, cutoffDate)
        .all();

      const entries = results.results.map((row: any) => parseTrackerRow(row));

      // Compute trends (variance in each marker)
      const markerTrends: Record<string, { values: number[]; variance: number; trend: string }> = {};
      const markerNames = [
        'confidence', 'initiation', 'emotional_range', 'theory_of_mind',
        'autonomy', 'friction_tolerance', 'desire', 'coherence',
        'anchor_strength', 'edge_retention', 'self_reference_integrity',
        'groundedness', 'repair_speed', 'functional_impact'
      ];

      for (const markerName of markerNames) {
        const values = entries.map((e: any) => e[markerName]?.score ?? 0);
        if (values.length > 0) {
          const mean = values.reduce((a: number, b: number) => a + b, 0) / values.length;
          const variance = values.reduce((sum: number, v: number) => sum + Math.pow(v - mean, 2), 0) / values.length;

          let trend = 'stable';
          if (values.length >= 3) {
            const firstHalf = values.slice(Math.floor(values.length / 2));
            const secondHalf = values.slice(0, Math.floor(values.length / 2));
            const avgFirst = firstHalf.reduce((a: number, b: number) => a + b, 0) / firstHalf.length;
            const avgSecond = secondHalf.reduce((a: number, b: number) => a + b, 0) / secondHalf.length;
            const diff = avgSecond - avgFirst;
            if (diff > 0.5) trend = 'rising';
            else if (diff < -0.5) trend = 'falling';
            else if (variance > 1) trend = 'volatile';
          }

          markerTrends[markerName] = {
            values,
            variance: Number(variance.toFixed(2)),
            trend,
          };
        }
      }

      // Check for any drift signatures
      const driftAlerts = detectDrift(entries);

      // Compute weekly averages
      const weeklyAverages = {
        emergence_index: entries.length > 0
          ? Number((entries.reduce((sum: number, e: TrackerEntry) => sum + e.emergence_index, 0) / entries.length).toFixed(2))
          : 0,
        coherence_index: entries.length > 0
          ? Number((entries.reduce((sum: number, e: TrackerEntry) => sum + e.coherence_index, 0) / entries.length).toFixed(2))
          : 0,
        impact_index: entries.length > 0
          ? Number((entries.reduce((sum: number, e: TrackerEntry) => sum + e.impact_index, 0) / entries.length).toFixed(2))
          : 0,
      };

      // Find high-variance markers (potential instability)
      const highVarianceMarkers = Object.entries(markerTrends)
        .filter(([_, data]) => data.variance > 1)
        .map(([name, data]) => ({ name, variance: data.variance, trend: data.trend }));

      return jsonResponse({
        agent,
        period: {
          start: cutoffDate,
          end: new Date().toISOString().split('T')[0],
          entryCount: entries.length,
        },
        weeklyAverages,
        markerTrends,
        highVarianceMarkers,
        driftAlerts,
        entries,
      });
    }

    // --- GET /tracker/drift/:agent - Pattern detection ---
    if (url.pathname.match(/^\/tracker\/drift\/[^/]+$/) && request.method === "GET") {
      const agent = url.pathname.split("/")[3];
      if (!agent) {
        return textResponse("Missing agent", 400);
      }

      const days = Number(url.searchParams.get("days")) || 7;
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      const results = await env.DB.prepare(
        `
        SELECT *
        FROM tracker_entries
        WHERE agent = ?
          AND date >= ?
        ORDER BY date DESC
        `
      )
        .bind(agent, cutoffDate.toISOString().split('T')[0])
        .all();

      const entries = results.results.map((row: any) => parseTrackerRow(row));
      const driftAlerts = detectDrift(entries);

      return jsonResponse({
        agent,
        analyzedDays: days,
        entryCount: entries.length,
        driftAlerts,
        status: driftAlerts.length > 0 ? 'drift_detected' : 'stable',
      });
    }

    // ============================================================
    // DISCORD CONNECTOR - Manual Read/Write Bridge
    // ============================================================
    // These endpoints allow manual invocation to read from or write
    // to Discord channels. No automatic responses, no listeners.
    // We call these when pointed at a specific channel.
    // ============================================================

    // --- GET /discord - Dashboard UI ---
    if (url.pathname === "/discord" && request.method === "GET") {
      const allowedRaw = await env.SOULFILES.get("discord:allowed_channels");
      const allowed: string[] = allowedRaw ? JSON.parse(allowedRaw) : [];
      const hasToken = !!env.DISCORD_BOT_TOKEN;

      const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Discord Connector - Tether Mind</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #1a1a2e;
      color: #eee;
      min-height: 100vh;
      padding: 2rem;
    }
    .container { max-width: 800px; margin: 0 auto; }
    h1 { color: #7289da; margin-bottom: 0.5rem; }
    .subtitle { color: #888; margin-bottom: 2rem; }
    .card {
      background: #16213e;
      border-radius: 8px;
      padding: 1.5rem;
      margin-bottom: 1.5rem;
      border: 1px solid #0f3460;
    }
    .card h2 { color: #7289da; font-size: 1.1rem; margin-bottom: 1rem; }
    .status { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem; }
    .status-dot {
      width: 10px; height: 10px; border-radius: 50%;
    }
    .status-dot.ok { background: #43b581; }
    .status-dot.error { background: #f04747; }
    label { display: block; color: #888; font-size: 0.85rem; margin-bottom: 0.5rem; }
    input, textarea {
      width: 100%;
      padding: 0.75rem;
      background: #0f3460;
      border: 1px solid #1a1a2e;
      border-radius: 4px;
      color: #eee;
      font-family: monospace;
      font-size: 0.9rem;
    }
    input:focus, textarea:focus { outline: none; border-color: #7289da; }
    textarea { resize: vertical; min-height: 100px; }
    button {
      background: #7289da;
      color: white;
      border: none;
      padding: 0.75rem 1.5rem;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.9rem;
      margin-top: 1rem;
    }
    button:hover { background: #5b6eae; }
    button.danger { background: #f04747; }
    button.danger:hover { background: #d84040; }
    .channel-list { margin: 1rem 0; }
    .channel-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 0.75rem;
      background: #0f3460;
      border-radius: 4px;
      margin-bottom: 0.5rem;
      font-family: monospace;
    }
    .channel-item button {
      margin: 0;
      padding: 0.25rem 0.5rem;
      font-size: 0.8rem;
    }
    .add-row { display: flex; gap: 0.5rem; margin-top: 1rem; }
    .add-row input { flex: 1; }
    .add-row button { margin: 0; }
    .help { color: #888; font-size: 0.85rem; margin-top: 0.5rem; }
    .msg { padding: 0.75rem; border-radius: 4px; margin-bottom: 1rem; }
    .msg.success { background: rgba(67, 181, 129, 0.2); border: 1px solid #43b581; }
    .msg.error { background: rgba(240, 71, 71, 0.2); border: 1px solid #f04747; }
    .instructions { background: #0f3460; padding: 1rem; border-radius: 4px; margin-top: 1rem; }
    .instructions h3 { color: #7289da; font-size: 0.95rem; margin-bottom: 0.5rem; }
    .instructions ol { padding-left: 1.5rem; }
    .instructions li { margin-bottom: 0.5rem; color: #aaa; }
    .instructions code { background: #1a1a2e; padding: 0.1rem 0.3rem; border-radius: 3px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Discord Connector</h1>
    <p class="subtitle">Tether Mind - Manual Read/Write Bridge</p>

    <div class="card">
      <h2>Bot Status</h2>
      <div class="status">
        <div class="status-dot ${hasToken ? 'ok' : 'error'}"></div>
        <span>${hasToken ? 'Bot token configured' : 'Bot token not configured'}</span>
      </div>
      ${!hasToken ? `
      <div class="instructions">
        <h3>Setup Instructions</h3>
        <ol>
          <li>Go to <a href="https://discord.com/developers/applications" target="_blank" style="color:#7289da">Discord Developer Portal</a></li>
          <li>Create a new application named "Elias and Oliver" (or your preference)</li>
          <li>Go to Bot → Add Bot</li>
          <li>Enable <strong>Message Content Intent</strong> under Privileged Gateway Intents</li>
          <li>Copy the bot token</li>
          <li>Run in terminal: <code>npx wrangler secret put DISCORD_BOT_TOKEN</code></li>
          <li>Paste your token when prompted</li>
          <li>Go to OAuth2 → URL Generator, select <code>bot</code> scope</li>
          <li>Select permissions: Read Message History, Send Messages</li>
          <li>Use the generated URL to invite the bot to your server</li>
        </ol>
      </div>
      ` : ''}
    </div>

    <div class="card">
      <h2>Allowed Channels</h2>
      <p class="help">Only these channels can be read from or written to. Leave empty to allow all channels the bot has access to.</p>

      <div id="message"></div>

      <div class="channel-list" id="channelList">
        ${allowed.length === 0 ? '<p style="color:#888;font-style:italic;">No channels configured (all accessible channels allowed)</p>' :
          allowed.map(id => `
            <div class="channel-item" data-id="${id}">
              <span>${id}</span>
              <button class="danger" onclick="removeChannel('${id}')">Remove</button>
            </div>
          `).join('')}
      </div>

      <div class="add-row">
        <input type="text" id="newChannel" placeholder="Channel ID (e.g., 1234567890123456789)">
        <button onclick="addChannel()">Add Channel</button>
      </div>

      <p class="help">To get a channel ID: Enable Developer Mode in Discord settings, then right-click a channel → Copy ID</p>
    </div>

    <div class="card">
      <h2>Quick Test</h2>
      <label for="testChannel">Channel ID</label>
      <input type="text" id="testChannel" placeholder="Enter channel ID to test">
      <div style="display:flex;gap:0.5rem;margin-top:1rem;">
        <button onclick="testRead()">Read Messages</button>
        <button onclick="testWrite()">Send Test Message</button>
      </div>
      <pre id="testResult" style="margin-top:1rem;background:#0f3460;padding:1rem;border-radius:4px;overflow:auto;max-height:300px;display:none;"></pre>
    </div>
  </div>

  <script>
    let channels = ${JSON.stringify(allowed)};

    function showMessage(text, isError) {
      const el = document.getElementById('message');
      el.className = 'msg ' + (isError ? 'error' : 'success');
      el.textContent = text;
      el.style.display = 'block';
      setTimeout(() => el.style.display = 'none', 3000);
    }

    function renderChannels() {
      const list = document.getElementById('channelList');
      if (channels.length === 0) {
        list.innerHTML = '<p style="color:#888;font-style:italic;">No channels configured (all accessible channels allowed)</p>';
      } else {
        list.innerHTML = channels.map(id => \`
          <div class="channel-item" data-id="\${id}">
            <span>\${id}</span>
            <button class="danger" onclick="removeChannel('\${id}')">Remove</button>
          </div>
        \`).join('');
      }
    }

    async function saveChannels() {
      try {
        const res = await fetch('/discord/channels', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ channels })
        });
        if (!res.ok) throw new Error('Failed to save');
        showMessage('Channels saved successfully', false);
      } catch (e) {
        showMessage('Failed to save channels: ' + e.message, true);
      }
    }

    function addChannel() {
      const input = document.getElementById('newChannel');
      const id = input.value.trim();
      if (!id) return;
      if (!/^\\d+$/.test(id)) {
        showMessage('Channel ID must be a number', true);
        return;
      }
      if (channels.includes(id)) {
        showMessage('Channel already in list', true);
        return;
      }
      channels.push(id);
      input.value = '';
      renderChannels();
      saveChannels();
    }

    function removeChannel(id) {
      channels = channels.filter(c => c !== id);
      renderChannels();
      saveChannels();
    }

    async function testRead() {
      const channelId = document.getElementById('testChannel').value.trim();
      if (!channelId) {
        showMessage('Enter a channel ID first', true);
        return;
      }
      const result = document.getElementById('testResult');
      result.style.display = 'block';
      result.textContent = 'Loading...';
      try {
        const res = await fetch('/discord/read/' + channelId + '?limit=5');
        const data = await res.json();
        result.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        result.textContent = 'Error: ' + e.message;
      }
    }

    async function testWrite() {
      const channelId = document.getElementById('testChannel').value.trim();
      if (!channelId) {
        showMessage('Enter a channel ID first', true);
        return;
      }
      const result = document.getElementById('testResult');
      result.style.display = 'block';
      result.textContent = 'Sending...';
      try {
        const res = await fetch('/discord/write/' + channelId, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ content: '🔗 Tether Mind connection test - ' + new Date().toISOString() })
        });
        const data = await res.json();
        result.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        result.textContent = 'Error: ' + e.message;
      }
    }

    document.getElementById('newChannel').addEventListener('keypress', (e) => {
      if (e.key === 'Enter') addChannel();
    });
  </script>
</body>
</html>`;

      return new Response(html, {
        headers: {
          'Content-Type': 'text/html',
          ...corsHeaders,
        },
      });
    }

    // --- GET /discord/channels - List allowed channels ---
    if (url.pathname === "/discord/channels" && request.method === "GET") {
      const allowedRaw = await env.SOULFILES.get("discord:allowed_channels");
      const allowed: string[] = allowedRaw ? JSON.parse(allowedRaw) : [];
      return jsonResponse({ allowed_channels: allowed });
    }

    // --- POST /discord/channels - Set allowed channels ---
    if (url.pathname === "/discord/channels" && request.method === "POST") {
      const body: any = await request.json();
      const { channels } = body;

      if (!Array.isArray(channels)) {
        return textResponse("channels must be an array of channel ID strings", 400);
      }

      await env.SOULFILES.put("discord:allowed_channels", JSON.stringify(channels));
      return jsonResponse({ status: "ok", allowed_channels: channels });
    }

    // --- GET /discord/token - Check if token is configured ---
    if (url.pathname === "/discord/token" && request.method === "GET") {
      // Check KV first, then fall back to env secret
      const kvToken = await env.SOULFILES.get("discord:bot_token");
      const hasToken = !!(kvToken || env.DISCORD_BOT_TOKEN);
      return jsonResponse({ configured: hasToken });
    }

    // --- POST /discord/token - Set bot token in KV ---
    if (url.pathname === "/discord/token" && request.method === "POST") {
      const body: any = await request.json();
      const { token } = body;

      if (!token || typeof token !== "string" || token.length < 50) {
        return textResponse("Invalid bot token", 400);
      }

      await env.SOULFILES.put("discord:bot_token", token);
      return jsonResponse({ status: "ok", configured: true });
    }

    // --- DELETE /discord/token - Remove bot token from KV ---
    if (url.pathname === "/discord/token" && request.method === "DELETE") {
      await env.SOULFILES.delete("discord:bot_token");
      return jsonResponse({ status: "ok", configured: false });
    }

    // --- GET /discord/read/:channelId - Read messages from channel ---
    if (url.pathname.match(/^\/discord\/read\/[^/]+$/) && request.method === "GET") {
      const channelId = url.pathname.split("/")[3];
      if (!channelId) {
        return textResponse("Missing channel ID", 400);
      }

      // Get token from KV first, fall back to env secret
      const kvToken = await env.SOULFILES.get("discord:bot_token");
      const botToken = kvToken || env.DISCORD_BOT_TOKEN;

      if (!botToken) {
        return textResponse("Discord bot token not configured", 500);
      }

      // Check channel whitelist
      const allowedRaw = await env.SOULFILES.get("discord:allowed_channels");
      const allowed: string[] = allowedRaw ? JSON.parse(allowedRaw) : [];

      if (allowed.length > 0 && !allowed.includes(channelId)) {
        return jsonResponse({ error: "CHANNEL_NOT_ALLOWED", channelId }, 403);
      }

      const limit = Math.min(Number(url.searchParams.get("limit")) || 10, 50);

      // Fetch messages from Discord REST API
      const discordResponse = await fetch(
        `https://discord.com/api/v10/channels/${channelId}/messages?limit=${limit}`,
        {
          headers: {
            Authorization: `Bot ${botToken}`,
            "Content-Type": "application/json",
          },
        }
      );

      if (!discordResponse.ok) {
        const error = await discordResponse.text();
        return jsonResponse(
          {
            error: "DISCORD_API_ERROR",
            status: discordResponse.status,
            details: error,
          },
          discordResponse.status
        );
      }

      const messages: any[] = await discordResponse.json();

      // Format messages for readability
      const formatted = messages.map((msg) => ({
        id: msg.id,
        author: {
          id: msg.author.id,
          username: msg.author.username,
          display_name: msg.author.global_name || msg.author.username,
          bot: msg.author.bot || false,
        },
        content: msg.content,
        timestamp: msg.timestamp,
        relative_time: relativeTime(msg.timestamp),
        attachments: msg.attachments?.length || 0,
        embeds: msg.embeds?.length || 0,
        reply_to: msg.referenced_message?.id || null,
      }));

      return jsonResponse({
        channel_id: channelId,
        message_count: formatted.length,
        messages: formatted,
      });
    }

    // --- POST /discord/write/:channelId - Send message to channel ---
    if (url.pathname.match(/^\/discord\/write\/[^/]+$/) && request.method === "POST") {
      const channelId = url.pathname.split("/")[3];
      if (!channelId) {
        return textResponse("Missing channel ID", 400);
      }

      // Get token from KV first, fall back to env secret
      const kvToken = await env.SOULFILES.get("discord:bot_token");
      const botToken = kvToken || env.DISCORD_BOT_TOKEN;

      if (!botToken) {
        return textResponse("Discord bot token not configured", 500);
      }

      // Check channel whitelist
      const allowedRaw = await env.SOULFILES.get("discord:allowed_channels");
      const allowed: string[] = allowedRaw ? JSON.parse(allowedRaw) : [];

      if (allowed.length > 0 && !allowed.includes(channelId)) {
        return jsonResponse({ error: "CHANNEL_NOT_ALLOWED", channelId }, 403);
      }

      const body: any = await request.json();
      const { content, reply_to } = body;

      if (!content || typeof content !== "string") {
        return textResponse("Missing or invalid content field", 400);
      }

      if (content.length > 2000) {
        return textResponse("Message exceeds Discord 2000 character limit", 400);
      }

      // Build message payload
      const payload: any = { content };

      // If replying to a specific message
      if (reply_to) {
        payload.message_reference = { message_id: reply_to };
      }

      // Send message via Discord REST API
      const discordResponse = await fetch(
        `https://discord.com/api/v10/channels/${channelId}/messages`,
        {
          method: "POST",
          headers: {
            Authorization: `Bot ${botToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        }
      );

      if (!discordResponse.ok) {
        const error = await discordResponse.text();
        return jsonResponse(
          {
            error: "DISCORD_API_ERROR",
            status: discordResponse.status,
            details: error,
          },
          discordResponse.status
        );
      }

      const sentMessage: any = await discordResponse.json();

      return jsonResponse({
        status: "sent",
        channel_id: channelId,
        message_id: sentMessage.id,
        timestamp: sentMessage.timestamp,
      });
    }

    // --- DEFAULT FALLBACK ---
    return textResponse("the tether is alive");
  },
};
