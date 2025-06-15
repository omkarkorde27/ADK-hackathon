# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Cost-optimized model routing with fallback mechanisms."""

import os
from typing import Dict, Any, Optional, List
from enum import Enum
import time
import logging

class ModelTier(Enum):
    PREMIUM = "premium"      # GPT-4, Claude 3 Opus
    STANDARD = "standard"    # Claude 3 Sonnet, GPT-3.5 Turbo
    EFFICIENT = "efficient"  # Gemini Pro, Gemini Flash
    FALLBACK = "fallback"    # Gemini Flash, basic models

class AgentRole(Enum):
    DATA_DETECTIVE = "data_detective"
    OPTIMIST_ANALYST = "optimist_analyst" 
    PESSIMIST_CRITIC = "pessimist_critic"
    ETHICAL_AUDITOR = "ethical_auditor"
    SYNTHESIS_MODERATOR = "synthesis_moderator"
    ORCHESTRATOR = "orchestrator"

class ModelRouter:
    """Routes agents to optimal models based on cost, capability, and availability."""
    
    def __init__(self):
        self.model_costs = self._initialize_model_costs()
        self.model_capabilities = self._initialize_model_capabilities()
        self.fallback_chain = self._initialize_fallback_chain()
        self.usage_tracking = {}
        
    def _initialize_model_costs(self) -> Dict[str, float]:
        """Initialize relative model costs (per 1K tokens)."""
        return {
            # Premium tier
            "gpt-4": 0.06,
            "claude-3-opus": 0.075,
            "grok-1": 0.05,
            
            # Standard tier  
            "claude-3-sonnet": 0.015,
            "gpt-3.5-turbo": 0.002,
            "claude-3-haiku": 0.0025,
            
            # Efficient tier
            "gemini-1.5-pro": 0.007,
            "gemini-pro": 0.0005,
            "gemini-flash": 0.0002,
            
            # Fallback
            "gemini-1.5-flash": 0.0001
        }
    
    def _initialize_model_capabilities(self) -> Dict[str, Dict[str, float]]:
        """Initialize model capability scores (0-1 scale)."""
        return {
            # Premium models - high capability
            "gpt-4": {
                "reasoning": 0.95,
                "ethics": 0.98,
                "bias_detection": 0.92,
                "data_analysis": 0.88,
                "debate": 0.90
            },
            "claude-3-opus": {
                "reasoning": 0.93,
                "ethics": 0.85,
                "bias_detection": 0.88,
                "data_analysis": 0.85,
                "debate": 0.95
            },
            "grok-1": {
                "reasoning": 0.88,
                "ethics": 0.75,
                "bias_detection": 0.80,
                "data_analysis": 0.82,
                "debate": 0.92
            },
            
            # Standard tier
            "claude-3-sonnet": {
                "reasoning": 0.85,
                "ethics": 0.80,
                "bias_detection": 0.75,
                "data_analysis": 0.88,
                "debate": 0.85
            },
            "gpt-3.5-turbo": {
                "reasoning": 0.75,
                "ethics": 0.70,
                "bias_detection": 0.65,
                "data_analysis": 0.70,
                "debate": 0.75
            },
            
            # Efficient tier
            "gemini-1.5-pro": {
                "reasoning": 0.90,
                "ethics": 0.75,
                "bias_detection": 0.78,
                "data_analysis": 0.95,
                "debate": 0.80
            },
            "gemini-pro": {
                "reasoning": 0.82,
                "ethics": 0.70,
                "bias_detection": 0.72,
                "data_analysis": 0.90,
                "debate": 0.75
            },
            "gemini-flash": {
                "reasoning": 0.75,
                "ethics": 0.65,
                "bias_detection": 0.68,
                "data_analysis": 0.85,
                "debate": 0.70
            }
        }
    
    def _initialize_fallback_chain(self) -> Dict[str, List[str]]:
        """Initialize fallback chains for each primary model."""
        return {
            "gpt-4": ["claude-3-opus", "claude-3-sonnet", "gemini-1.5-pro"],
            "claude-3-opus": ["gpt-4", "claude-3-sonnet", "gemini-1.5-pro"], 
            "claude-3-sonnet": ["gemini-1.5-pro", "claude-3-haiku", "gemini-pro"],
            "grok-1": ["claude-3-sonnet", "gemini-1.5-pro", "gemini-pro"],
            "gemini-1.5-pro": ["gemini-pro", "claude-3-sonnet", "gemini-flash"],
            "gemini-pro": ["gemini-flash", "gemini-1.5-flash", "gpt-3.5-turbo"],
            "gemini-flash": ["gemini-1.5-flash", "gemini-pro", "gpt-3.5-turbo"]
        }
    
    def route_agent_model(
        self, 
        agent_role: AgentRole, 
        complexity_score: float = 0.5,
        budget_constraint: Optional[float] = None
    ) -> str:
        """Route agent to optimal model based on role, complexity, and budget."""
        
        # Define role-specific model preferences
        role_preferences = {
            AgentRole.DATA_DETECTIVE: {
                "primary_capability": "data_analysis",
                "preferred_models": ["gemini-1.5-pro", "gemini-pro", "claude-3-sonnet"]
            },
            AgentRole.OPTIMIST_ANALYST: {
                "primary_capability": "reasoning", 
                "preferred_models": ["claude-3-sonnet", "claude-3-opus", "gemini-1.5-pro"]
            },
            AgentRole.PESSIMIST_CRITIC: {
                "primary_capability": "debate",
                "preferred_models": ["grok-1", "claude-3-opus", "claude-3-sonnet"]
            },
            AgentRole.ETHICAL_AUDITOR: {
                "primary_capability": "ethics",
                "preferred_models": ["gpt-4", "claude-3-opus", "claude-3-sonnet"]
            },
            AgentRole.SYNTHESIS_MODERATOR: {
                "primary_capability": "reasoning",
                "preferred_models": ["gemini-1.5-pro", "claude-3-opus", "gpt-4"]
            },
            AgentRole.ORCHESTRATOR: {
                "primary_capability": "reasoning",
                "preferred_models": ["gemini-1.5-pro", "claude-3-opus", "gemini-pro"]
            }
        }
        
        preferences = role_preferences[agent_role]
        primary_capability = preferences["primary_capability"]
        
        # Filter models by capability threshold
        min_capability = 0.6 + (complexity_score * 0.3)  # Scale 0.6-0.9 based on complexity
        
        candidate_models = []
        for model in preferences["preferred_models"]:
            if model in self.model_capabilities:
                capability = self.model_capabilities[model][primary_capability]
                if capability >= min_capability:
                    candidate_models.append((model, capability))
        
        # Sort by capability (descending)
        candidate_models.sort(key=lambda x: x[1], reverse=True)
        
        # Apply budget constraints if specified
        if budget_constraint:
            candidate_models = [
                (model, capability) for model, capability in candidate_models
                if self.model_costs.get(model, 0.1) <= budget_constraint
            ]
        
        # Select best available model
        if candidate_models:
            selected_model = candidate_models[0][0]
        else:
            # Fallback to most cost-efficient model
            selected_model = "gemini-flash"
        
        # Try to get model, use fallback if unavailable
        final_model = self._try_model_with_fallback(selected_model)
        
        # Track usage
        self._track_usage(agent_role, final_model)
        
        return final_model
    
    def _try_model_with_fallback(self, primary_model: str) -> str:
        """Try primary model, use fallback chain if unavailable."""
        
        # Simulate model availability check (in practice, would check API status)
        if self._is_model_available(primary_model):
            return primary_model
        
        # Try fallback chain
        fallback_models = self.fallback_chain.get(primary_model, ["gemini-flash"])
        
        for fallback_model in fallback_models:
            if self._is_model_available(fallback_model):
                logging.warning(f"Primary model {primary_model} unavailable, using fallback {fallback_model}")
                return fallback_model
        
        # Final fallback
        logging.error(f"All fallback models unavailable for {primary_model}, using emergency fallback")
        return "gemini-flash"
    
    def _is_model_available(self, model: str) -> bool:
        """Check if model is available (simplified simulation)."""
        # In practice, would check API status, rate limits, etc.
        # For simulation, assume 95% availability for most models
        import random
        
        availability_rates = {
            "gpt-4": 0.92,
            "claude-3-opus": 0.94,
            "claude-3-sonnet": 0.96,
            "grok-1": 0.88,
            "gemini-1.5-pro": 0.98,
            "gemini-pro": 0.99,
            "gemini-flash": 0.999
        }
        
        rate = availability_rates.get(model, 0.95)
        return random.random() < rate
    
    def _track_usage(self, agent_role: AgentRole, model: str):
        """Track model usage for cost optimization."""
        if agent_role not in self.usage_tracking:
            self.usage_tracking[agent_role] = {}
        
        if model not in self.usage_tracking[agent_role]:
            self.usage_tracking[agent_role][model] = 0
        
        self.usage_tracking[agent_role][model] += 1
    
    def get_cost_estimate(self, agent_assignments: Dict[AgentRole, str], token_estimate: int = 1000) -> Dict[str, float]:
        """Estimate costs for agent model assignments."""
        total_cost = 0.0
        cost_breakdown = {}
        
        for agent_role, model in agent_assignments.items():
            model_cost = self.model_costs.get(model, 0.01)
            agent_cost = (model_cost * token_estimate) / 1000  # Cost per 1K tokens
            cost_breakdown[f"{agent_role.value}_{model}"] = agent_cost
            total_cost += agent_cost
        
        cost_breakdown["total"] = total_cost
        return cost_breakdown
    
    def optimize_agent_assignments(
        self, 
        agent_roles: List[AgentRole],
        complexity_score: float = 0.5,
        budget_limit: float = 1.0
    ) -> Dict[AgentRole, str]:
        """Optimize model assignments across all agents within budget."""
        
        assignments = {}
        remaining_budget = budget_limit
        
        # Sort agents by importance (ethical auditor and synthesis most important)
        priority_order = [
            AgentRole.ETHICAL_AUDITOR,
            AgentRole.SYNTHESIS_MODERATOR, 
            AgentRole.DATA_DETECTIVE,
            AgentRole.PESSIMIST_CRITIC,
            AgentRole.OPTIMIST_ANALYST,
            AgentRole.ORCHESTRATOR
        ]
        
        # Filter to requested agents and maintain priority order
        ordered_agents = [role for role in priority_order if role in agent_roles]
        ordered_agents.extend([role for role in agent_roles if role not in priority_order])
        
        for agent_role in ordered_agents:
            # Calculate per-agent budget
            agents_remaining = len([r for r in ordered_agents if r not in assignments])
            agent_budget = remaining_budget / agents_remaining if agents_remaining > 0 else 0.01
            
            # Route with budget constraint
            selected_model = self.route_agent_model(
                agent_role, 
                complexity_score, 
                budget_constraint=agent_budget * 2  # Allow some flexibility
            )
            
            assignments[agent_role] = selected_model
            
            # Update remaining budget
            model_cost = self.model_costs.get(selected_model, 0.01)
            remaining_budget -= model_cost
            remaining_budget = max(0, remaining_budget)
        
        return assignments
    
    def get_usage_report(self) -> Dict[str, Any]:
        """Generate usage and cost report."""
        report = {
            "usage_by_agent": self.usage_tracking,
            "total_calls": sum(
                sum(models.values()) for models in self.usage_tracking.values()
            ),
            "most_used_models": {},
            "cost_efficiency": {}
        }
        
        # Calculate most used models overall
        model_usage = {}
        for agent_usage in self.usage_tracking.values():
            for model, count in agent_usage.items():
                model_usage[model] = model_usage.get(model, 0) + count
        
        report["most_used_models"] = dict(
            sorted(model_usage.items(), key=lambda x: x[1], reverse=True)
        )
        
        # Calculate cost efficiency (capability per cost)
        for model in model_usage.keys():
            if model in self.model_costs and model in self.model_capabilities:
                avg_capability = sum(self.model_capabilities[model].values()) / len(self.model_capabilities[model])
                cost = self.model_costs[model]
                efficiency = avg_capability / cost if cost > 0 else 0
                report["cost_efficiency"][model] = efficiency
        
        return report