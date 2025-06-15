# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Debate moderation and orchestration logic."""

import time
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from enum import Enum

class DebatePhase(Enum):
    INITIALIZATION = "initialization"
    OPENING_STATEMENTS = "opening_statements" 
    ADVERSARIAL_EXCHANGE = "adversarial_exchange"
    EVIDENCE_REVIEW = "evidence_review"
    CONSENSUS_BUILDING = "consensus_building"
    FINAL_SYNTHESIS = "final_synthesis"

@dataclass
class DebateArgument:
    agent_name: str
    argument: str
    evidence: List[str]
    confidence: float
    timestamp: float
    round_number: int
    response_to: str = None

@dataclass
class DebateMetrics:
    conflict_intensity: float  # 1-10 scale
    consensus_level: float     # 0-1 scale  
    bias_score: float         # 0-1 scale
    evidence_strength: float   # 0-1 scale
    participation_balance: float  # 0-1 scale

class DebateModerator:
    """Orchestrates multi-agent debates with conflict tracking and consensus building."""
    
    def __init__(self, max_rounds: int = 3, conflict_threshold: float = 7.0):
        self.max_rounds = max_rounds
        self.conflict_threshold = conflict_threshold
        self.debate_log: List[DebateArgument] = []
        self.current_round = 0
        self.phase = DebatePhase.INITIALIZATION
        
    def initiate_debate(self, question: str, agents: Dict[str, Any]) -> Dict[str, Any]:
        """Start a new debate session."""
        self.debate_log = []
        self.current_round = 0
        self.phase = DebatePhase.INITIALIZATION
        
        debate_context = {
            "question": question,
            "agents": agents,
            "start_time": time.time(),
            "debate_id": f"debate_{int(time.time())}"
        }
        
        return debate_context
    
    def facilitate_round(self, debate_context: Dict[str, Any]) -> Tuple[List[DebateArgument], DebateMetrics]:
        """Facilitate one round of debate."""
        self.current_round += 1
        round_arguments = []
        
        # Collect arguments from each agent
        for agent_name, agent in debate_context["agents"].items():
            # Inject previous arguments for dynamic prompt chaining
            context = self._build_agent_context(agent_name)
            
            argument = self._get_agent_argument(agent, context, debate_context["question"])
            
            if argument:
                debate_arg = DebateArgument(
                    agent_name=agent_name,
                    argument=argument["text"],
                    evidence=argument.get("evidence", []),
                    confidence=argument.get("confidence", 0.5),
                    timestamp=time.time(),
                    round_number=self.current_round
                )
                
                round_arguments.append(debate_arg)
                self.debate_log.append(debate_arg)
        
        # Calculate debate metrics
        metrics = self._calculate_debate_metrics(round_arguments)
        
        return round_arguments, metrics
    
    def _build_agent_context(self, agent_name: str) -> str:
        """Build context including opponent arguments for dynamic prompt chaining."""
        if not self.debate_log:
            return ""
            
        recent_arguments = [arg for arg in self.debate_log[-6:] if arg.agent_name != agent_name]
        
        context = "Recent opponent arguments:\n"
        for arg in recent_arguments:
            context += f"\n{arg.agent_name}: {arg.argument}\n"
            if arg.evidence:
                context += f"Evidence: {', '.join(arg.evidence[:2])}\n"
        
        return context
    
    def _calculate_debate_metrics(self, arguments: List[DebateArgument]) -> DebateMetrics:
        """Calculate conflict intensity and other debate metrics."""
        
        # Conflict intensity based on argument disagreement
        conflict_intensity = self._calculate_conflict_intensity(arguments)
        
        # Consensus level based on agreement patterns
        consensus_level = 1.0 - (conflict_intensity / 10.0)
        
        # Bias score based on evidence quality and agent balance
        bias_score = self._calculate_bias_score(arguments)
        
        # Evidence strength based on citations and data quality
        evidence_strength = self._calculate_evidence_strength(arguments)
        
        # Participation balance across agents
        participation_balance = self._calculate_participation_balance(arguments)
        
        return DebateMetrics(
            conflict_intensity=conflict_intensity,
            consensus_level=consensus_level,
            bias_score=bias_score,
            evidence_strength=evidence_strength,
            participation_balance=participation_balance
        )
    
    def _calculate_conflict_intensity(self, arguments: List[DebateArgument]) -> float:
        """Calculate conflict intensity on 1-10 scale."""
        if len(arguments) < 2:
            return 1.0
            
        # Analyze argument sentiment and opposition
        disagreement_indicators = 0
        total_comparisons = 0
        
        for i, arg1 in enumerate(arguments):
            for arg2 in arguments[i+1:]:
                total_comparisons += 1
                
                # Simple keyword-based conflict detection
                conflict_keywords = [
                    "disagree", "wrong", "incorrect", "flawed", "however", "but",
                    "contrary", "opposite", "challenge", "dispute", "refute"
                ]
                
                for keyword in conflict_keywords:
                    if keyword in arg1.argument.lower() or keyword in arg2.argument.lower():
                        disagreement_indicators += 1
                        break
        
        if total_comparisons == 0:
            return 1.0
            
        conflict_ratio = disagreement_indicators / total_comparisons
        return min(10.0, 1.0 + (conflict_ratio * 9.0))
    
    def should_continue_debate(self, metrics: DebateMetrics) -> bool:
        """Determine if debate should continue based on metrics."""
        if self.current_round >= self.max_rounds:
            return False
            
        # Continue if high conflict and low consensus
        if metrics.conflict_intensity > self.conflict_threshold and metrics.consensus_level < 0.6:
            return True
            
        # Stop if strong consensus reached
        if metrics.consensus_level > 0.8:
            return False
            
        return self.current_round < self.max_rounds