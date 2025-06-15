# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Advanced bias detection algorithms for debate analysis."""

import re
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import numpy as np

@dataclass 
class BiasPattern:
    name: str
    description: str
    keywords: List[str]
    severity: float  # 0-1 scale
    category: str

class BiasDetector:
    """Detects various forms of bias in multi-agent debates."""
    
    def __init__(self, fairness_threshold: float = 0.85):
        self.fairness_threshold = fairness_threshold
        self.bias_patterns = self._initialize_bias_patterns()
    
    def _initialize_bias_patterns(self) -> List[BiasPattern]:
        """Initialize known bias patterns for detection."""
        return [
            BiasPattern(
                name="confirmation_bias",
                description="Selectively interpreting evidence to confirm preexisting beliefs",
                keywords=["confirms", "validates", "supports my view", "as expected", "obviously"],
                severity=0.7,
                category="cognitive"
            ),
            BiasPattern(
                name="anchoring_bias", 
                description="Over-relying on first piece of information encountered",
                keywords=["initial data shows", "first analysis", "starting point", "baseline"],
                severity=0.6,
                category="cognitive"
            ),
            BiasPattern(
                name="availability_bias",
                description="Overestimating likelihood of events with greater availability in memory",
                keywords=["recently", "just happened", "current trend", "latest"], 
                severity=0.5,
                category="cognitive"
            ),
            BiasPattern(
                name="demographic_bias",
                description="Unfair treatment based on demographic characteristics",
                keywords=["urban vs rural", "age group", "gender", "ethnicity", "location"],
                severity=0.9,
                category="fairness"
            ),
            BiasPattern(
                name="sample_bias",
                description="Drawing conclusions from unrepresentative samples",
                keywords=["small sample", "limited data", "subset", "not representative"],
                severity=0.8,
                category="statistical"
            ),
            BiasPattern(
                name="temporal_bias",
                description="Bias due to time-specific factors not representative of general trends",
                keywords=["seasonal", "temporary", "one-time event", "anomaly"],
                severity=0.6,
                category="temporal"
            )
        ]
    
    def analyze_debate(self, arguments: List[Any]) -> Dict[str, Any]:
        """Comprehensive bias analysis of debate arguments."""
        
        bias_scores = {}
        violations = []
        agent_bias_profiles = {}
        
        for arg in arguments:
            agent_name = arg.agent_name
            argument_text = arg.argument.lower()
            
            # Analyze this argument for bias patterns
            arg_bias_score, detected_patterns = self._analyze_argument_bias(argument_text)
            
            # Track per-agent bias
            if agent_name not in agent_bias_profiles:
                agent_bias_profiles[agent_name] = {
                    "total_bias_score": 0.0,
                    "argument_count": 0,
                    "detected_patterns": []
                }
            
            agent_bias_profiles[agent_name]["total_bias_score"] += arg_bias_score
            agent_bias_profiles[agent_name]["argument_count"] += 1
            agent_bias_profiles[agent_name]["detected_patterns"].extend(detected_patterns)
            
            # Check for fairness violations
            if arg_bias_score > (1.0 - self.fairness_threshold):
                violations.append({
                    "agent": agent_name,
                    "argument": arg.argument[:200] + "..." if len(arg.argument) > 200 else arg.argument,
                    "bias_score": arg_bias_score,
                    "patterns": detected_patterns,
                    "timestamp": arg.timestamp
                })
        
        # Calculate overall bias metrics
        overall_bias_score = self._calculate_overall_bias(agent_bias_profiles)
        balance_score = self._calculate_agent_balance(agent_bias_profiles)
        diversity_score = self._calculate_perspective_diversity(arguments)
        
        return {
            "overall_bias_score": overall_bias_score,
            "balance_score": balance_score,
            "diversity_score": diversity_score,
            "fairness_threshold": self.fairness_threshold,
            "violations": violations,
            "agent_profiles": agent_bias_profiles,
            "recommendations": self._generate_bias_recommendations(
                overall_bias_score, violations, agent_bias_profiles
            )
        }
    
    def _analyze_argument_bias(self, argument_text: str) -> Tuple[float, List[str]]:
        """Analyze a single argument for bias patterns."""
        detected_patterns = []
        total_bias_score = 0.0
        
        for pattern in self.bias_patterns:
            pattern_score = 0.0
            
            # Check for keyword matches
            for keyword in pattern.keywords:
                if keyword in argument_text:
                    pattern_score += pattern.severity * 0.2  # Base score per keyword
            
            # Additional pattern-specific analysis
            if pattern.name == "confirmation_bias":
                pattern_score += self._detect_confirmation_bias(argument_text)
            elif pattern.name == "sample_bias":
                pattern_score += self._detect_sample_bias(argument_text)
            elif pattern.name == "demographic_bias":
                pattern_score += self._detect_demographic_bias(argument_text)
            
            if pattern_score > 0.1:  # Threshold for detection
                detected_patterns.append(pattern.name)
                total_bias_score += min(pattern_score, pattern.severity)
        
        return min(total_bias_score, 1.0), detected_patterns
    
    def _detect_confirmation_bias(self, text: str) -> float:
        """Detect confirmation bias indicators."""
        confirmation_phrases = [
            "this proves", "clearly shows", "confirms our hypothesis",
            "as we suspected", "validates our approach", "supports our view"
        ]
        
        score = 0.0
        for phrase in confirmation_phrases:
            if phrase in text:
                score += 0.15
        
        # Look for lack of counterarguments
        counter_indicators = ["however", "but", "although", "despite", "alternatively"]
        if not any(indicator in text for indicator in counter_indicators):
            score += 0.1
        
        return min(score, 0.6)
    
    def _detect_sample_bias(self, text: str) -> float:
        """Detect sample bias indicators."""
        sample_issues = [
            r"\b\d+\s*samples?\b",  # Small number of samples
            r"\blimited\s+data\b",
            r"\bsmall\s+dataset\b", 
            r"\bfew\s+cases\b"
        ]
        
        score = 0.0
        for pattern in sample_issues:
            if re.search(pattern, text, re.IGNORECASE):
                score += 0.2
        
        return min(score, 0.5)
    
    def _detect_demographic_bias(self, text: str) -> float:
        """Detect potential demographic bias."""
        demographic_terms = [
            "urban", "rural", "city", "suburban",
            "young", "old", "elderly", "millennial",
            "male", "female", "men", "women"
        ]
        
        # Check for generalizations about demographic groups
        generalization_patterns = [
            r"\b(all|most|many)\s+(" + "|".join(demographic_terms) + r")\b",
            r"\b(" + "|".join(demographic_terms) + r")\s+(always|never|typically)\b"
        ]
        
        score = 0.0
        for pattern in generalization_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                score += 0.3
        
        return min(score, 0.8)
    
    def _calculate_overall_bias(self, agent_profiles: Dict[str, Any]) -> float:
        """Calculate overall bias score across all agents."""
        if not agent_profiles:
            return 0.0
        
        total_weighted_bias = 0.0
        total_arguments = 0
        
        for profile in agent_profiles.values():
            if profile["argument_count"] > 0:
                avg_bias = profile["total_bias_score"] / profile["argument_count"]
                total_weighted_bias += avg_bias * profile["argument_count"]
                total_arguments += profile["argument_count"]
        
        return total_weighted_bias / total_arguments if total_arguments > 0 else 0.0
    
    def _calculate_agent_balance(self, agent_profiles: Dict[str, Any]) -> float:
        """Calculate how balanced the participation is across agents."""
        if len(agent_profiles) < 2:
            return 0.0
        
        argument_counts = [profile["argument_count"] for profile in agent_profiles.values()]
        
        # Calculate coefficient of variation (lower = more balanced)
        mean_count = np.mean(argument_counts)
        if mean_count == 0:
            return 0.0
        
        cv = np.std(argument_counts) / mean_count
        balance_score = max(0.0, 1.0 - cv)  # Convert to 0-1 scale where 1 = perfectly balanced
        
        return balance_score
    
    def _calculate_perspective_diversity(self, arguments: List[Any]) -> float:
        """Calculate diversity of perspectives in the debate."""
        if len(arguments) < 2:
            return 0.0
        
        # Simple approach: measure argument length variance and unique word usage
        argument_lengths = [len(arg.argument.split()) for arg in arguments]
        
        # Normalize length variance
        length_diversity = min(1.0, np.std(argument_lengths) / np.mean(argument_lengths)) if np.mean(argument_lengths) > 0 else 0.0
        
        # Count unique concepts (simple word-based approach)
        all_words = set()
        agent_vocabularies = {}
        
        for arg in arguments:
            words = set(arg.argument.lower().split())
            all_words.update(words)
            
            if arg.agent_name not in agent_vocabularies:
                agent_vocabularies[arg.agent_name] = set()
            agent_vocabularies[arg.agent_name].update(words)
        
        # Calculate vocabulary diversity across agents
        if len(agent_vocabularies) > 1:
            vocab_overlaps = []
            agents = list(agent_vocabularies.keys())
            
            for i in range(len(agents)):
                for j in range(i + 1, len(agents)):
                    vocab1 = agent_vocabularies[agents[i]]
                    vocab2 = agent_vocabularies[agents[j]]
                    
                    intersection = len(vocab1.intersection(vocab2))
                    union = len(vocab1.union(vocab2))
                    
                    overlap = intersection / union if union > 0 else 0.0
                    vocab_overlaps.append(overlap)
            
            vocab_diversity = 1.0 - np.mean(vocab_overlaps)
        else:
            vocab_diversity = 0.0
        
        # Combine metrics
        return (length_diversity * 0.3 + vocab_diversity * 0.7)
    
    def _generate_bias_recommendations(
        self, 
        overall_bias_score: float, 
        violations: List[Dict], 
        agent_profiles: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations to reduce bias in future debates."""
        recommendations = []
        
        if overall_bias_score > 0.3:
            recommendations.append(
                "High bias detected. Consider additional evidence gathering and cross-validation."
            )
        
        if violations:
            recommendations.append(
                f"Found {len(violations)} fairness violations. Review flagged arguments for demographic or statistical bias."
            )
        
        # Check for unbalanced participation
        argument_counts = [profile["argument_count"] for profile in agent_profiles.values()]
        if len(argument_counts) > 1 and (max(argument_counts) > 2 * min(argument_counts)):
            recommendations.append(
                "Unbalanced agent participation detected. Encourage more input from less active agents."
            )
        
        # Check for pattern concentration
        pattern_frequency = {}
        for profile in agent_profiles.values():
            for pattern in profile["detected_patterns"]:
                pattern_frequency[pattern] = pattern_frequency.get(pattern, 0) + 1
        
        frequent_patterns = [p for p, f in pattern_frequency.items() if f > len(agent_profiles)]
        if frequent_patterns:
            recommendations.append(
                f"Common bias patterns detected: {', '.join(frequent_patterns)}. Consider structured counter-argument protocols."
            )
        
        if not recommendations:
            recommendations.append("Bias levels within acceptable thresholds. Continue current debate protocols.")
        
        return recommendations