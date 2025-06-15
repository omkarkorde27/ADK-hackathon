# Copyright 2025 Google LLC
# Licensed under the Apache License, Version 2.0

"""Comprehensive debate logging and transcript management."""

import json
import time
from typing import Dict, List, Any
from pathlib import Path
from dataclasses import asdict
import hashlib

class DebateLogger:
    """Manages debate transcripts and audit trails."""
    
    def __init__(self, log_directory: str = "debate_logs"):
        self.log_directory = Path(log_directory)
        self.log_directory.mkdir(exist_ok=True)
    
    def save_debate_transcript(
        self, 
        debate_context: Dict[str, Any],
        arguments: List[Any],
        consensus_report: Dict[str, Any]
    ) -> str:
        """Save complete debate transcript with provenance tracking."""
        
        transcript_id = self._generate_transcript_id(debate_context)
        
        # Build comprehensive transcript
        transcript = {
            "metadata": {
                "transcript_id": transcript_id,
                "question": debate_context["question"],
                "start_time": debate_context["start_time"],
                "end_time": time.time(),
                "duration_seconds": time.time() - debate_context["start_time"],
                "agents_participating": list(debate_context["agents"].keys()),
                "total_arguments": len(arguments)
            },
            "debate_flow": self._build_debate_flow(arguments),
            "argument_details": [self._serialize_argument(arg) for arg in arguments],
            "consensus_report": consensus_report,
            "insight_provenance": self._build_insight_provenance(arguments, consensus_report),
            "audit_trail": self._build_audit_trail(debate_context, arguments)
        }
        
        # Save to file
        transcript_file = self.log_directory / f"debate_{transcript_id}.json"
        with open(transcript_file, 'w') as f:
            json.dump(transcript, f, indent=2, default=str)
        
        # Save summary for quick access
        self._save_debate_summary(transcript_id, transcript)
        
        return transcript_id
    
    def _generate_transcript_id(self, debate_context: Dict[str, Any]) -> str:
        """Generate unique transcript ID."""
        content = f"{debate_context['question']}{debate_context['start_time']}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def _build_debate_flow(self, arguments: List[Any]) -> List[Dict[str, Any]]:
        """Build chronological flow of debate."""
        flow = []
        
        # Group arguments by round
        rounds = {}
        for arg in arguments:
            round_num = arg.round_number
            if round_num not in rounds:
                rounds[round_num] = []
            rounds[round_num].append(arg)
        
        # Build flow by round
        for round_num in sorted(rounds.keys()):
            round_args = rounds[round_num]
            
            flow.append({
                "round": round_num,
                "arguments_count": len(round_args),
                "agents_participated": [arg.agent_name for arg in round_args],
                "round_summary": self._summarize_round(round_args),
                "key_conflicts": self._identify_round_conflicts(round_args)
            })
        
        return flow
    
    def _build_insight_provenance(
        self, 
        arguments: List[Any], 
        consensus_report: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build provenance tracking from raw data to final insights."""
        
        provenance = {
            "data_sources": [],
            "reasoning_chain": [],
            "evidence_trail": [],
            "consensus_formation": []
        }
        
        # Extract data sources from arguments
        for arg in arguments:
            if hasattr(arg, 'evidence') and arg.evidence:
                for evidence in arg.evidence:
                    if evidence not in provenance["data_sources"]:
                        provenance["data_sources"].append(evidence)
        
        # Build reasoning chain
        for arg in arguments:
            reasoning_step = {
                "agent": arg.agent_name,
                "timestamp": arg.timestamp,
                "reasoning": arg.argument[:200] + "..." if len(arg.argument) > 200 else arg.argument,
                "confidence": arg.confidence,
                "evidence_count": len(arg.evidence) if hasattr(arg, 'evidence') else 0
            }
            provenance["reasoning_chain"].append(reasoning_step)
        
        # Document consensus formation
        if "primary_insights" in consensus_report:
            for insight in consensus_report["primary_insights"]:
                consensus_step = {
                    "insight": insight["claim"],
                    "supporting_agents": insight["supporting_agents"],
                    "confidence": insight["confidence"],
                    "evidence_count": insight["evidence_count"]
                }
                provenance["consensus_formation"].append(consensus_step)
        
        return provenance
    
    def _build_audit_trail(
        self, 
        debate_context: Dict[str, Any], 
        arguments: List[Any]
    ) -> List[Dict[str, Any]]:
        """Build detailed audit trail for compliance and review."""
        
        audit_events = []
        
        # Debate initiation
        audit_events.append({
            "timestamp": debate_context["start_time"],
            "event_type": "debate_initiated",
            "details": {
                "question": debate_context["question"],
                "agents": list(debate_context["agents"].keys())
            }
        })
        
        # Argument events
        for arg in arguments:
            audit_events.append({
                "timestamp": arg.timestamp,
                "event_type": "argument_submitted",
                "agent": arg.agent_name,
                "details": {
                    "round": arg.round_number,
                    "confidence": arg.confidence,
                    "argument_length": len(arg.argument),
                    "evidence_count": len(arg.evidence) if hasattr(arg, 'evidence') else 0
                }
            })
        
        # Sort by timestamp
        audit_events.sort(key=lambda x: x["timestamp"])
        
        return audit_events
    
    def _serialize_argument(self, arg: Any) -> Dict[str, Any]:
        """Serialize argument object to dictionary."""
        if hasattr(arg, '__dict__'):
            return asdict(arg) if hasattr(arg, '__dataclass_fields__') else arg.__dict__
        else:
            return str(arg)
    
    def _summarize_round(self, round_args: List[Any]) -> str:
        """Generate summary for a debate round."""
        agent_positions = {}
        
        for arg in round_args:
            # Simple keyword extraction for position identification
            position_keywords = self._extract_position_keywords(arg.argument)
            agent_positions[arg.agent_name] = position_keywords[:3]  # Top 3 keywords
        
        summary = f"Round with {len(round_args)} arguments. "
        
        if len(set(str(pos) for pos in agent_positions.values())) > 1:
            summary += "Significant disagreement observed. "
        else:
            summary += "General agreement on key points. "
        
        return summary
    
    def _identify_round_conflicts(self, round_args: List[Any]) -> List[str]:
        """Identify key conflicts in a debate round."""
        conflicts = []
        
        # Simple conflict detection based on negation words
        conflict_indicators = ["disagree", "wrong", "incorrect", "however", "but", "contrary"]
        
        for arg in round_args:
            for indicator in conflict_indicators:
                if indicator in arg.argument.lower():
                    conflicts.append(f"{arg.agent_name} expressed disagreement")
                    break
        
        return list(set(conflicts))  # Remove duplicates
    
    def _extract_position_keywords(self, argument: str) -> List[str]:
        """Extract key position indicators from argument."""
        # Simple keyword extraction (in practice, would use NLP)
        words = argument.lower().split()
        
        # Filter for meaningful words (exclude common words)
        stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by"}
        keywords = [word for word in words if len(word) > 3 and word not in stop_words]
        
        # Return most frequent meaningful words
        word_freq = {}
        for word in keywords:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        return sorted(word_freq.keys(), key=lambda x: word_freq[x], reverse=True)
    
    def _save_debate_summary(self, transcript_id: str, transcript: Dict[str, Any]):
        """Save debate summary for quick access."""
        summary = {
            "transcript_id": transcript_id,
            "question": transcript["metadata"]["question"],
            "duration": transcript["metadata"]["duration_seconds"],
            "agents": transcript["metadata"]["agents_participating"],
            "total_arguments": transcript["metadata"]["total_arguments"],
            "consensus_strength": transcript["consensus_report"].get("consensus_strength", 0.0),
            "primary_insights_count": len(transcript["consensus_report"].get("primary_insights", [])),
            "timestamp": transcript["metadata"]["start_time"]
        }
        
        summary_file = self.log_directory / "debate_summaries.jsonl"
        with open(summary_file, 'a') as f:
            f.write(json.dumps(summary) + '\n')
    
    def get_debate_transcript(self, transcript_id: str) -> Dict[str, Any]:
        """Retrieve complete debate transcript."""
        transcript_file = self.log_directory / f"debate_{transcript_id}.json"
        
        if transcript_file.exists():
            with open(transcript_file, 'r') as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"Transcript {transcript_id} not found")
    
    def list_recent_debates(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent debates with summaries."""
        summary_file = self.log_directory / "debate_summaries.jsonl"
        
        if not summary_file.exists():
            return []
        
        summaries = []
        with open(summary_file, 'r') as f:
            for line in f:
                summaries.append(json.loads(line.strip()))
        
        # Sort by timestamp (most recent first) and limit
        summaries.sort(key=lambda x: x["timestamp"], reverse=True)
        return summaries[:limit]