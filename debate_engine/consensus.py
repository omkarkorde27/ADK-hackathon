# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Consensus building algorithms using Borda count voting and evidence weighting."""

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import numpy as np

@dataclass
class Evidence:
    source: str
    data_points: List[Any]
    confidence: float
    agent_source: str

@dataclass
class Claim:
    statement: str
    supporting_evidence: List[Evidence]
    agent_votes: Dict[str, float]  # agent_name -> confidence score
    final_score: float = 0.0

class BordaConsensusBuilder:
    """Implements Borda count voting with evidence weighting for consensus building."""
    
    def __init__(self, evidence_weight: float = 0.4, vote_weight: float = 0.6):
        self.evidence_weight = evidence_weight
        self.vote_weight = vote_weight
    
    def build_consensus(self, debate_arguments: List[Any]) -> Dict[str, Any]:
        """Build consensus from debate arguments using Borda count voting."""
        
        # Extract claims and evidence from arguments
        claims = self._extract_claims(debate_arguments)
        
        # Calculate Borda scores
        borda_scores = self._calculate_borda_scores(claims)
        
        # Weight by evidence quality
        evidence_weighted_scores = self._weight_by_evidence(claims, borda_scores)
        
        # Generate consensus report
        consensus_report = self._generate_consensus_report(claims, evidence_weighted_scores)
        
        return consensus_report
    
    def _extract_claims(self, arguments: List[Any]) -> List[Claim]:
        """Extract distinct claims from debate arguments."""
        claims = []
        
        # Simple claim extraction (in practice, would use NLP)
        for arg in arguments:
            # Extract sentences as potential claims
            sentences = arg.argument.split('. ')
            
            for sentence in sentences:
                if len(sentence.strip()) > 20:  # Filter short sentences
                    claim = Claim(
                        statement=sentence.strip(),
                        supporting_evidence=self._extract_evidence(arg),
                        agent_votes={arg.agent_name: arg.confidence}
                    )
                    
                    # Check if similar claim already exists
                    existing_claim = self._find_similar_claim(claims, claim)
                    if existing_claim:
                        existing_claim.agent_votes[arg.agent_name] = arg.confidence
                    else:
                        claims.append(claim)
        
        return claims
    
    def _calculate_borda_scores(self, claims: List[Claim]) -> Dict[str, float]:
        """Calculate Borda count scores for claims."""
        borda_scores = {}
        
        for claim in claims:
            total_score = 0.0
            total_voters = len(claim.agent_votes)
            
            if total_voters == 0:
                borda_scores[claim.statement] = 0.0
                continue
            
            # Borda count: higher confidence = higher rank
            sorted_votes = sorted(claim.agent_votes.values(), reverse=True)
            
            for agent, confidence in claim.agent_votes.items():
                rank = sorted_votes.index(confidence)
                borda_points = total_voters - rank
                total_score += borda_points * confidence
            
            borda_scores[claim.statement] = total_score / total_voters if total_voters > 0 else 0.0
        
        return borda_scores
    
    def _weight_by_evidence(self, claims: List[Claim], borda_scores: Dict[str, float]) -> Dict[str, float]:
        """Weight Borda scores by evidence quality."""
        weighted_scores = {}
        
        for claim in claims:
            borda_score = borda_scores.get(claim.statement, 0.0)
            evidence_score = self._calculate_evidence_score(claim.supporting_evidence)
            
            final_score = (self.vote_weight * borda_score + 
                          self.evidence_weight * evidence_score)
            
            weighted_scores[claim.statement] = final_score
            claim.final_score = final_score
        
        return weighted_scores
    
    def _calculate_evidence_score(self, evidence_list: List[Evidence]) -> float:
        """Calculate quality score for supporting evidence."""
        if not evidence_list:
            return 0.0
        
        total_score = 0.0
        for evidence in evidence_list:
            # Score based on data points and confidence
            data_quality = min(1.0, len(evidence.data_points) / 10.0)  # Normalize to 0-1
            score = (evidence.confidence * 0.7) + (data_quality * 0.3)
            total_score += score
        
        return total_score / len(evidence_list)
    
    def _generate_consensus_report(self, claims: List[Claim], scores: Dict[str, float]) -> Dict[str, Any]:
        """Generate final consensus report."""
        
        # Sort claims by final score
        sorted_claims = sorted(claims, key=lambda x: x.final_score, reverse=True)
        
        # Calculate overall consensus strength
        top_score = sorted_claims[0].final_score if sorted_claims else 0.0
        consensus_strength = min(1.0, top_score / 10.0)  # Normalize
        
        # Identify primary and secondary insights
        primary_claims = [c for c in sorted_claims if c.final_score >= top_score * 0.8]
        secondary_claims = [c for c in sorted_claims if top_score * 0.5 <= c.final_score < top_score * 0.8]
        
        return {
            "consensus_strength": consensus_strength,
            "primary_insights": [
                {
                    "claim": claim.statement,
                    "confidence": claim.final_score,
                    "supporting_agents": list(claim.agent_votes.keys()),
                    "evidence_count": len(claim.supporting_evidence)
                }
                for claim in primary_claims[:3]  # Top 3
            ],
            "secondary_insights": [
                {
                    "claim": claim.statement,
                    "confidence": claim.final_score,
                    "supporting_agents": list(claim.agent_votes.keys())
                }
                for claim in secondary_claims[:5]  # Top 5
            ],
            "total_claims_analyzed": len(claims),
            "agreement_level": self._calculate_agreement_level(sorted_claims)
        }
    
    def _calculate_agreement_level(self, claims: List[Claim]) -> float:
        """Calculate overall agreement level among agents."""
        if not claims:
            return 0.0
        
        total_agreement = 0.0
        for claim in claims:
            if len(claim.agent_votes) > 1:
                votes = list(claim.agent_votes.values())
                agreement = 1.0 - (np.std(votes) / np.mean(votes)) if np.mean(votes) > 0 else 0.0
                total_agreement += max(0.0, agreement)
        
        return total_agreement / len(claims) if claims else 0.0