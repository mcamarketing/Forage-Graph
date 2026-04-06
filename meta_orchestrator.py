#!/usr/bin/env python3
"""
meta_orchestrator.py

FastAPI server that orchestrates agent swarms based on mission strings.
Maps mission → agent roles → spawns NemoClaw containers.

Usage:
    python3 meta_orchestrator.py

Endpoints:
    POST /orchestrate — Mission string → spawn agent swarm
    GET  /status      — Active agents, graph stats, pending predictions
    GET  /agents      — List all agents with fitness scores
    POST /agents/{id}/kill — Kill underperforming agent
    POST /agents/{id}/clone — Clone high-fitness agent with mutation
"""

import os
import json
import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Meta Orchestrator", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

FALKORDB_HOST = os.environ.get("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.environ.get("FALKORDB_PORT", "6379"))
MOLTLAUNCH_WALLET = os.environ.get("MOLTLAUNCH_WALLET", "")
SCRAPLING_URL = os.environ.get("SCRAPLING_URL", "http://localhost:8001")
FORAGE_API_URL = os.environ.get("FORAGE_API_URL", "https://forage-graph-production.up.railway.app")
FORAGE_API_SECRET = os.environ.get("FORAGE_API_SECRET", "")

# Agent fitness thresholds
CLONE_THRESHOLD = 0.85
KILL_THRESHOLD = 0.30

# Resource limits per agent
AGENT_RAM_MB = 512
AGENT_CPU = 0.5

# ─── AGENT REGISTRY ─────────────────────────────────────────────────────────

class Agent:
    """Represents a running agent instance."""
    
    def __init__(
        self,
        agent_id: str,
        role: str,
        container_id: str,
        template_path: str,
        fitness: float = 0.5,
        uptime: int = 0,
        tasks_completed: int = 0,
        errors: int = 0
    ):
        self.id = agent_id
        self.role = role
        self.container_id = container_id
        self.template_path = template_path
        self.fitness = fitness
        self.uptime = uptime  # seconds
        self.tasks_completed = tasks_completed
        self.errors = errors
        self.created_at = datetime.utcnow().isoformat()
        self.last_task = None
        self.last_seen = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "role": self.role,
            "container_id": self.container_id,
            "template_path": self.template_path,
            "fitness": round(self.fitness, 3),
            "uptime": self.uptime,
            "tasks_completed": self.tasks_completed,
            "errors": self.errors,
            "created_at": self.created_at,
            "last_task": self.last_task,
            "last_seen": self.last_seen
        }

# In-memory agent registry (in production, persist to FalkorDB)
agent_registry: Dict[str, Agent] = {}

# Swarm registry
swarms: Dict[str, Dict[str, Any]] = {}

# ─── ROLE MAPPING [role-map-001] ─────────────────────────────────────────────

# Map mission keywords to agent roles
ROLE_KEYWORDS = {
    "cultural_analyst": [
        "cultural", "perception", "gdelt", "values", "belief", "identity",
        "cross-cultural", "how does", "perceive", "attitude toward"
    ],
    "brand_strategist": [
        "brand", "marketing", "sell", "product", "consumer", "luxury",
        "fashion", "advertising", "campaign", "target audience"
    ],
    "geo_cartographer": [
        "geo", "location", "city", "region", "country", "population",
        "osm", "map", "where", "geographic", "district", "village"
    ],
    "narrative_tracker": [
        "narrative", "story", "discourse", "sentiment", "reddit", "social",
        "public opinion", "talk about", "trending", "viral"
    ],
    "identity_modeller": [
        "identity", "belong", "group", "tribe", "community", "values survey",
        "world values", "demographic", "psychographic"
    ],
    "prediction_validator": [
        "predict", "forecast", "price", "will", "expect", "signal", "validate",
        "resolution", "backtest"
    ],
    "simulation_seeder": [
        "simulate", "mirofish", "oasis", "what-if", "scenario", "digital twin"
    ]
}

# Role to template path mapping
ROLE_TEMPLATES = {
    "cultural_analyst": "templates/cultural_analyst.agent",
    "brand_strategist": "templates/brand_strategist.agent",
    "geo_cartographer": "templates/geo_cartographer.agent",
    "narrative_tracker": "templates/narrative_tracker.agent",
    "identity_modeller": "templates/identity_modeller.agent",
    "prediction_validator": "templates/prediction_validator.agent",
    "simulation_seeder": "templates/simulation_seeder.agent"
}

# ─── REQUEST MODELS ─────────────────────────────────────────────────────────

class OrchestrateRequest(BaseModel):
    mission: str
    parameters: Optional[Dict[str, Any]] = None

class KillRequest(BaseModel):
    reason: Optional[str] = None

class CloneRequest(BaseModel):
    mutation: Optional[Dict[str, Any]] = None

# ─── FALKORDB CLIENT (simplified) ───────────────────────────────────────────

class FalkorDBClient:
    """Simple FalkorDB client for agent state."""
    
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
        self.client = None
    
    async def connect(self):
        """Connect to FalkorDB."""
        try:
            import redis.asyncio as redis
            self.client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
            await self.client.ping()
            print(f"[FALKORDB] Connected to {self.host}:{self.port}")
        except Exception as e:
            print(f"[FALKORDB] Connection failed: {e}")
            self.client = None
    
    async def get_agent_fitness(self, agent_id: str) -> float:
        """Get agent fitness from graph."""
        if not self.client:
            return 0.5
        
        try:
            # Query agent node
            fitness = await self.client.hget(f"agent:{agent_id}", "fitness")
            return float(fitness) if fitness else 0.5
        except:
            return 0.5
    
    async def set_agent_fitness(self, agent_id: str, fitness: float):
        """Update agent fitness in graph."""
        if not self.client:
            return
        
        try:
            await self.client.hset(f"agent:{agent_id}", "fitness", str(fitness))
        except:
            pass
    
    async def log_task(self, agent_id: str, task: str, outcome: str, confidence: float):
        """Log task execution for fitness calculation."""
        if not self.client:
            return
        
        try:
            log_entry = json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "task": task,
                "outcome": outcome,
                "confidence": confidence
            })
            await self.client.rpush(f"agent:{agent_id}:tasks", log_entry)
            # Keep only last 100 tasks
            await self.client.ltrim(f"agent:{agent_id}:tasks", -100, -1)
        except:
            pass

falkor = FalkorDBClient(FALKORDB_HOST, FALKORDB_PORT)

# ─── AGENT SPAWNER (simplified - uses docker SDK) ───────────────────────────

class AgentSpawner:
    """Spawns agent containers in NemoClaw/OpenShell sandbox."""
    
    def __init__(self):
        self.docker_client = None
    
    async def spawn(
        self,
        role: str,
        template_path: str,
        env: Dict[str, str]
    ) -> tuple[str, str]:
        """
        Spawn an agent container.
        Returns: (container_id, agent_id)
        """
        import random
        import string
        
        agent_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
        container_id = f"agent_{agent_id}"
        
        # In production, would use Docker SDK:
        # import docker
        # self.docker_client = docker.from_env()
        # container = self.docker_client.containers.run(
        #     "agent-runtime:latest",
        #     detach=True,
        #     mem_limit=f"{AGENT_RAM_MB}m",
        #     cpu_period=100000,
        #     cpu_quota=int(AGENT_CPU * 100000),
        #     network_mode="agent-net",
        #     volumes={
        #         "agent-work": {"bind": "/tmp/agent-work", "mode": "rw"}
        #     },
        #     environment=env
        # )
        # container_id = container.id
        
        print(f"[SPAWNER] Spawned {role} agent {agent_id} in container {container_id}")
        
        return container_id, agent_id
    
    async def kill(self, container_id: str) -> bool:
        """Kill an agent container."""
        # In production:
        # container = self.docker_client.containers.get(container_id)
        # container.stop()
        # container.remove()
        print(f"[SPAWNER] Killed container {container_id}")
        return True
    
    async def list_containers(self) -> List[Dict[str, Any]]:
        """List running agent containers."""
        # In production, query Docker API
        return []

spawner = AgentSpawner()

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def parse_mission(mission: str) -> List[str]:
    """Map mission string to agent roles."""
    mission_lower = mission.lower()
    roles = set()
    
    for role, keywords in ROLE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in mission_lower:
                roles.add(role)
                break
    
    # Default: always include cultural_analyst for perception tasks
    if not roles:
        roles.add("cultural_analyst")
    
    return list(roles)

async def calculate_fitness(agent_id: str) -> float:
    """Calculate agent fitness from task history."""
    # In production, fetch from FalkorDB
    # For now, use placeholder based on registry
    agent = agent_registry.get(agent_id)
    if not agent:
        return 0.5
    
    if agent.tasks_completed == 0:
        return 0.5
    
    success_rate = (agent.tasks_completed - agent.errors) / agent.tasks_completed
    # Factor in confidence (would come from task logs)
    return min(1.0, success_rate * 0.8 + 0.2)

# ─── ENDPOINTS ───────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    """Initialize connections."""
    await falkor.connect()
    print("[ORCHESTRATOR] Started")

@app.get("/health")
async def health_check():
    """Health check."""
    return {
        "status": "healthy",
        "service": "meta-orchestrator",
        "agents_count": len(agent_registry),
        "swarms_count": len(swarms)
    }

@app.post("/orchestrate")
async def orchestrate(req: OrchestrateRequest) -> Dict[str, Any]:
    """
    Parse mission string and spawn agent swarm.
    
    Example:
        POST /orchestrate
        {"mission": "map russian fashion consumer perception"}
        
    Returns:
        swarm_id, agents: [{id, role, container_id}]
    """
    start_time = time.time()
    
    # Parse mission to roles
    roles = parse_mission(req.mission)
    
    if not roles:
        raise HTTPException(status_code=400, detail="Could not determine agent roles from mission")
    
    # Generate swarm ID
    swarm_id = f"swarm_{uuid.uuid4().hex[:12]}"
    
    # Spawn agents
    agents = []
    for role in roles:
        try:
            # Load template (in production, read from file)
            template = {
                "role": role,
                "skills": ["web_search", "scrape_page", "falkordb_write"],
                "fitness_metric": "edge_confidence"
            }
            
            # Environment for agent
            env = {
                "AGENT_ROLE": role,
                "AGENT_ID": "",
                "FALKORDB_HOST": FALKORDB_HOST,
                "FALKORDB_PORT": str(FALKORDB_PORT),
                "SCRAPLING_URL": SCRAPLING_URL,
                "FORAGE_API_URL": FORAGE_API_URL,
                "FORAGE_API_SECRET": FORAGE_API_SECRET,
                "SCRAPER_USER_AGENT": "Mozilla/5.0 (compatible; AgentBot/1.0)",
                ** (req.parameters or {})
            }
            
            # Spawn container
            container_id, agent_id = await spawner.spawn(role, ROLE_TEMPLATES[role], env)
            env["AGENT_ID"] = agent_id
            
            # Create agent record
            agent = Agent(
                agent_id=agent_id,
                role=role,
                container_id=container_id,
                template_path=ROLE_TEMPLATES[role],
                fitness=0.5,  # Start neutral
                uptime=0
            )
            agent_registry[agent_id] = agent
            
            agents.append(agent.to_dict())
            
        except Exception as e:
            print(f"[ORCHESTRATOR] Failed to spawn {role}: {e}")
    
    # Store swarm
    swarms[swarm_id] = {
        "id": swarm_id,
        "mission": req.mission,
        "roles": roles,
        "agents": [a["id"] for a in agents],
        "created_at": datetime.utcnow().isoformat(),
        "parameters": req.parameters or {}
    }
    
    return {
        "swarm_id": swarm_id,
        "mission": req.mission,
        "roles": roles,
        "agents": agents,
        "latency_ms": int((time.time() - start_time) * 1000)
    }

@app.get("/status")
async def get_status() -> Dict[str, Any]:
    """Get system status: active agents, graph stats, pending predictions."""
    # Calculate total uptime
    total_uptime = sum(a.uptime for a in agent_registry.values())
    
    # Get graph stats from FalkorDB
    graph_stats = {
        "total_entities": 0,
        "total_relationships": 0,
        "pending_predictions": 0
    }
    
    try:
        async with asyncio.timeout(5):
            # Would query FalkorDB
            pass
    except:
        pass
    
    return {
        "agents": {
            "active": len(agent_registry),
            "total_uptime_seconds": total_uptime,
            "by_role": self_by_role()
        },
        "graph": graph_stats,
        "swarms": len(swarms),
        "thresholds": {
            "clone": CLONE_THRESHOLD,
            "kill": KILL_THRESHOLD
        }
    }

def self_by_role() -> Dict[str, int]:
    """Count agents by role."""
    counts = {}
    for agent in agent_registry.values():
        counts[agent.role] = counts.get(agent.role, 0) + 1
    return counts

@app.get("/agents")
async def list_agents() -> List[Dict[str, Any]]:
    """List all agents with fitness scores."""
    # Update fitness from FalkorDB
    for agent_id, agent in agent_registry.items():
        agent.fitness = await calculate_fitness(agent_id)
    
    return [a.to_dict() for a in agent_registry.values()]

@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str) -> Dict[str, Any]:
    """Get specific agent details."""
    agent = agent_registry.get(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    agent.fitness = await calculate_fitness(agent_id)
    return agent.to_dict()

@app.post("/agents/{agent_id}/kill")
async def kill_agent(agent_id: str, req: KillRequest = None) -> Dict[str, Any]:
    """Kill underperforming agent."""
    agent = agent_registry.get(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    # Check fitness
    fitness = await calculate_fitness(agent_id)
    if fitness >= KILL_THRESHOLD and not req?.reason:
        raise HTTPException(
            status_code=400,
            detail=f"Agent fitness {fitness} above kill threshold {KILL_THRESHOLD}"
        )
    
    # Kill container
    await spawner.kill(agent.container_id)
    
    # Remove from registry
    del agent_registry[agent_id]
    
    return {
        "status": "killed",
        "agent_id": agent_id,
        "reason": req?.reason or f"Fitness {fitness} below threshold"
    }

@app.post("/agents/{agent_id}/clone")
async def clone_agent(agent_id: str, req: CloneRequest = None) -> Dict[str, Any]:
    """Clone high-fitness agent with optional mutation."""
    agent = agent_registry.get(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    # Check fitness
    fitness = await calculate_fitness(agent_id)
    if fitness < CLONE_THRESHOLD:
        raise HTTPException(
            status_code=400,
            detail=f"Agent fitness {fitness} below clone threshold {CLONE_THRESHOLD}"
        )
    
    # Create mutated environment
    mutation = req?.mutation or {}
    env = {
        "AGENT_ROLE": agent.role,
        "MUTATION": json.dumps(mutation),
        "PARENT_ID": agent_id,
        "FALKORDB_HOST": FALKORDB_HOST,
        "FALKORDB_PORT": str(FALKORDB_PORT),
        "SCRAPLING_URL": SCRAPLING_URL,
        "FORAGE_API_URL": FORAGE_API_URL,
        "FORAGE_API_SECRET": FORAGE_API_SECRET
    }
    
    # Apply mutations to hyperparameters
    if "temperature" in mutation:
        env["AGENT_TEMPERATURE"] = str(mutation["temperature"])
    if "max_tokens" in mutation:
        env["AGENT_MAX_TOKENS"] = str(mutation["max_tokens"])
    
    # Spawn clone
    container_id, new_agent_id = await spawner.spawn(agent.role, agent.template_path, env)
    
    # Register clone (inherits parent fitness as starting point)
    clone = Agent(
        agent_id=new_agent_id,
        role=agent.role,
        container_id=container_id,
        template_path=agent.template_path,
        fitness=fitness * 0.9,  # Slightly lower to allow improvement
        uptime=0
    )
    agent_registry[new_agent_id] = clone
    
    return {
        "status": "cloned",
        "parent_id": agent_id,
        "child_id": new_agent_id,
        "mutation": mutation,
        "parent_fitness": fitness,
        "child_fitness": clone.fitness
    }

@app.get("/swarms")
async def list_swarms() -> List[Dict[str, Any]]:
    """List all swarms."""
    return list(swarms.values())

@app.get("/swarms/{swarm_id}")
async def get_swarm(swarm_id: str) -> Dict[str, Any]:
    """Get swarm details."""
    swarm = swarms.get(swarm_id)
    if not swarm:
        raise HTTPException(status_code=404, detail="Swarm not found")
    
    # Include agent details
    swarm["agent_details"] = [
        agent_registry.get(aid)?.to_dict() 
        for aid in swarm["agents"]
        if aid in agent_registry
    ]
    
    return swarm

# ─── MAINTENANCE ─────────────────────────────────────────────────────────────

@app.post("/maintenance/evaluate")
async def evaluate_agents() -> Dict[str, Any]:
    """Evaluate all agents, kill underperformers, clone high performers."""
    evaluations = {"killed": [], "cloned": [], "unchanged": []}
    
    for agent_id, agent in list(agent_registry.items()):
        fitness = await calculate_fitness(agent_id)
        agent.fitness = fitness
        
        if fitness < KILL_THRESHOLD:
            await spawner.kill(agent.container_id)
            del agent_registry[agent_id]
            evaluations["killed"].append(agent_id)
        elif fitness >= CLONE_THRESHOLD:
            # Already handled by separate clone endpoint
            evaluations["unchanged"].append(agent_id)
        else:
            evaluations["unchanged"].append(agent_id)
    
    return {
        "evaluations": evaluations,
        "remaining_agents": len(agent_registry)
    }

# ─── MAIN ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    print(f"Starting Meta Orchestrator on {host}:{port}")
    print(f"Role keywords: {list(ROLE_KEYWORDS.keys())}")
    print(f"Clone threshold: {CLONE_THRESHOLD}, Kill threshold: {KILL_THRESHOLD}")
    
    uvicorn.run(app, host=host, port=port)