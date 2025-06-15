"""Tools for the Optimist Analyst agent."""

from typing import Dict, Any, List
from google.adk.tools import ToolContext
import pandas as pd
import numpy as np

def identify_opportunities(
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Identify growth opportunities and positive trends in the data."""
    
    if "query_results" not in tool_context.state:
        return {"error": "No data available for opportunity analysis"}
    
    try:
        df = pd.DataFrame(tool_context.state["query_results"])
        
        opportunities = {
            "growth_indicators": [],
            "positive_trends": [],
            "market_opportunities": [],
            "improvement_areas": []
        }
        
        # Analyze for growth indicators
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            values = df[col].dropna()
            if len(values) > 1:
                # Check for positive trend
                if len(values) >= 3:
                    recent_avg = values.tail(3).mean()
                    older_avg = values.head(3).mean()
                    
                    if recent_avg > older_avg:
                        growth_rate = ((recent_avg - older_avg) / older_avg) * 100
                        opportunities["growth_indicators"].append({
                            "metric": col,
                            "growth_rate": float(growth_rate),
                            "trend": "positive",
                            "recent_value": float(recent_avg),
                            "baseline_value": float(older_avg)
                        })
                
                # Identify high-performing segments
                if col.endswith(('_rate', '_percentage', '_score')):
                    top_performers = values.quantile(0.8)
                    if top_performers > values.median() * 1.2:
                        opportunities["market_opportunities"].append({
                            "metric": col,
                            "top_performer_threshold": float(top_performers),
                            "median_value": float(values.median()),
                            "potential_uplift": float((top_performers / values.median() - 1) * 100)
                        })
        
        # Analyze categorical data for opportunities
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        for cat_col in categorical_cols:
            if len(numeric_cols) > 0:
                # Find best performing categories
                for num_col in numeric_cols:
                    category_performance = df.groupby(cat_col)[num_col].agg(['mean', 'count']).reset_index()
                    
                    # Filter categories with sufficient data
                    significant_categories = category_performance[category_performance['count'] >= 3]
                    
                    if len(significant_categories) > 1:
                        best_category = significant_categories.loc[significant_categories['mean'].idxmax()]
                        worst_category = significant_categories.loc[significant_categories['mean'].idxmin()]
                        
                        improvement_potential = ((best_category['mean'] - worst_category['mean']) / worst_category['mean']) * 100
                        
                        if improvement_potential > 10:  # Significant improvement potential
                            opportunities["improvement_areas"].append({
                                "category_dimension": cat_col,
                                "performance_metric": num_col,
                                "best_performer": best_category[cat_col],
                                "best_performance": float(best_category['mean']),
                                "worst_performer": worst_category[cat_col],
                                "worst_performance": float(worst_category['mean']),
                                "improvement_potential": float(improvement_potential)
                            })
        
        # Generate positive narrative
        positive_insights = []
        
        if opportunities["growth_indicators"]:
            avg_growth = np.mean([gi["growth_rate"] for gi in opportunities["growth_indicators"]])
            positive_insights.append(f"Positive momentum detected with average growth rate of {avg_growth:.1f}%")
        
        if opportunities["market_opportunities"]:
            positive_insights.append(f"High-performing segments identified with potential uplifts up to {max([mo['potential_uplift'] for mo in opportunities['market_opportunities']]):.1f}%")
        
        if opportunities["improvement_areas"]:
            max_improvement = max([ia["improvement_potential"] for ia in opportunities["improvement_areas"]])
            positive_insights.append(f"Significant improvement opportunities available with potential gains up to {max_improvement:.1f}%")
        
        tool_context.state["opportunity_analysis"] = opportunities
        
        return {
            "status": "success",
            "opportunities": opportunities,
            "positive_insights": positive_insights,
            "opportunity_count": len(opportunities["growth_indicators"]) + len(opportunities["market_opportunities"]) + len(opportunities["improvement_areas"])
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e)
        }

def analyze_success_patterns(
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Analyze patterns of success and best practices from the data."""
    
    if "query_results" not in tool_context.state:
        return {"error": "No data available for success pattern analysis"}
    
    try:
        df = pd.DataFrame(tool_context.state["query_results"])
        
        success_patterns = {
            "top_performers": {},
            "success_factors": [],
            "best_practices": [],
            "recovery_patterns": []
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        # Identify top performers
        for col in numeric_cols:
            if col.endswith(('_score', '_rate', '_performance', '_efficiency')):
                top_10_percent = df[col].quantile(0.9)
                top_performers = df[df[col] >= top_10_percent]
                
                if len(top_performers) > 0:
                    success_patterns["top_performers"][col] = {
                        "threshold": float(top_10_percent),
                        "count": len(top_performers),
                        "average_performance": float(top_performers[col].mean()),
                        "characteristics": {}
                    }
                    
                    # Analyze characteristics of top performers
                    for cat_col in categorical_cols:
                        if cat_col in top_performers.columns:
                            top_performer_distribution = top_performers[cat_col].value_counts()
                            overall_distribution = df[cat_col].value_counts()
                            
                            # Find over-represented categories in top performers
                            for category in top_performer_distribution.index:
                                top_pct = (top_performer_distribution[category] / len(top_performers)) * 100
                                overall_pct = (overall_distribution[category] / len(df)) * 100
                                
                                if top_pct > overall_pct * 1.5:  # Over-represented
                                    success_patterns["top_performers"][col]["characteristics"][f"{cat_col}_{category}"] = {
                                        "representation_in_top_performers": float(top_pct),
                                        "overall_representation": float(overall_pct),
                                        "over_representation_factor": float(top_pct / overall_pct)
                                    }
        
        # Identify success factors (strong correlations with positive outcomes)
        if len(numeric_cols) > 1:
            correlation_matrix = df[numeric_cols].corr()
            
            performance_metrics = [col for col in numeric_cols if any(keyword in col.lower() 
                                 for keyword in ['performance', 'score', 'success', 'efficiency', 'rate'])]
            
            for perf_metric in performance_metrics:
                if perf_metric in correlation_matrix.columns:
                    correlations = correlation_matrix[perf_metric].abs().sort_values(ascending=False)
                    
                    for factor, corr_value in correlations.items():
                        if factor != perf_metric and corr_value > 0.3:  # Moderate to strong correlation
                            success_patterns["success_factors"].append({
                                "performance_metric": perf_metric,
                                "success_factor": factor,
                                "correlation_strength": float(corr_value),
                                "relationship": "positive" if correlation_matrix.loc[perf_metric, factor] > 0 else "negative"
                            })
        
        # Look for recovery patterns (V-shaped recoveries)
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0 and len(numeric_cols) > 0:
            date_col = date_cols[0]
            df_sorted = df.sort_values(date_col)
            
            for metric in numeric_cols:
                values = df_sorted[metric].dropna()
                if len(values) >= 6:  # Need sufficient data points
                    # Look for V-shaped pattern (decline followed by recovery)
                    rolling_mean = values.rolling(window=3).mean()
                    
                    # Find local minima followed by recovery
                    for i in range(2, len(rolling_mean) - 2):
                        if (rolling_mean.iloc[i] < rolling_mean.iloc[i-1] and 
                            rolling_mean.iloc[i] < rolling_mean.iloc[i+1] and
                            rolling_mean.iloc[i+2] > rolling_mean.iloc[i] * 1.1):  # 10% recovery
                            
                            recovery_strength = ((rolling_mean.iloc[i+2] - rolling_mean.iloc[i]) / rolling_mean.iloc[i]) * 100
                            
                            success_patterns["recovery_patterns"].append({
                                "metric": metric,
                                "recovery_point": i,
                                "lowest_value": float(rolling_mean.iloc[i]),
                                "recovery_value": float(rolling_mean.iloc[i+2]),
                                "recovery_strength": float(recovery_strength),
                                "time_to_recovery": 2  # periods
                            })
        
        # Generate best practices from patterns
        if success_patterns["success_factors"]:
            for factor in success_patterns["success_factors"]:
                if factor["relationship"] == "positive":
                    success_patterns["best_practices"].append(
                        f"Focus on improving {factor['success_factor']} to enhance {factor['performance_metric']} (correlation: {factor['correlation_strength']:.2f})"
                    )
        
        if success_patterns["top_performers"]:
            for metric, data in success_patterns["top_performers"].items():
                if data["characteristics"]:
                    char_list = list(data["characteristics"].keys())[:3]  # Top 3 characteristics
                    success_patterns["best_practices"].append(
                        f"Top performers in {metric} are characterized by: {', '.join(char_list)}"
                    )
        
        tool_context.state["success_analysis"] = success_patterns
        
        return {
            "status": "success",
            "success_patterns": success_patterns,
            "success_factor_count": len(success_patterns["success_factors"]),
            "best_practices_count": len(success_patterns["best_practices"])
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e)
        }

def generate_solutions(
    problem_description: str,
    tool_context: ToolContext,
) -> Dict[str, Any]:
    """Generate actionable solutions based on data insights."""
    
    solutions = {
        "immediate_actions": [],
        "strategic_initiatives": [],
        "preventive_measures": [],
        "success_metrics": []
    }
    
    try:
        # Use opportunity analysis if available
        if "opportunity_analysis" in tool_context.state:
            opportunities = tool_context.state["opportunity_analysis"]
            
            # Generate immediate actions from opportunities
            for growth_indicator in opportunities.get("growth_indicators", []):
                solutions["immediate_actions"].append({
                    "action": f"Amplify the positive trend in {growth_indicator['metric']}",
                    "rationale": f"Currently showing {growth_indicator['growth_rate']:.1f}% growth",
                    "expected_impact": "high",
                    "timeframe": "immediate"
                })
            
            for improvement_area in opportunities.get("improvement_areas", []):
                solutions["strategic_initiatives"].append({
                    "initiative": f"Replicate best practices from {improvement_area['best_performer']} to improve {improvement_area['category_dimension']}",
                    "potential_impact": f"{improvement_area['improvement_potential']:.1f}% improvement in {improvement_area['performance_metric']}",
                    "complexity": "medium",
                    "timeframe": "3-6 months"
                })
        
        # Use success patterns if available
        if "success_analysis" in tool_context.state:
            success_patterns = tool_context.state["success_analysis"]
            
            # Generate strategic initiatives from success factors
            for success_factor in success_patterns.get("success_factors", []):
                if success_factor["relationship"] == "positive":
                    solutions["strategic_initiatives"].append({
                        "initiative": f"Invest in improving {success_factor['success_factor']}",
                        "rationale": f"Strong correlation ({success_factor['correlation_strength']:.2f}) with {success_factor['performance_metric']}",
                        "expected_impact": "high",
                        "timeframe": "6-12 months"
                    })
            
            # Generate preventive measures from recovery patterns
            for recovery in success_patterns.get("recovery_patterns", []):
                solutions["preventive_measures"].append({
                    "measure": f"Implement early warning system for {recovery['metric']}",
                    "rationale": f"Historical data shows recovery is possible ({recovery['recovery_strength']:.1f}% improvement)",
                    "monitoring_threshold": recovery['lowest_value'] * 1.1,  # 10% buffer above historical low
                    "response_time": "immediate"
                })
        
        # Generate success metrics
        if "query_results" in tool_context.state:
            df = pd.DataFrame(tool_context.state["query_results"])
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                current_mean = df[col].mean()
                solutions["success_metrics"].append({
                    "metric": col,
                    "baseline": float(current_mean),
                    "target": float(current_mean * 1.1),  # 10% improvement target
                    "measurement_frequency": "monthly",
                    "success_threshold": float(current_mean * 1.05)  # 5% minimum improvement
                })
        
        # Add optimistic framing
        optimistic_narrative = [
            "Data shows clear pathways to improvement",
            "Historical patterns demonstrate recovery potential",
            "Multiple leverage points identified for positive impact",
            "Success factors are within organizational control"
        ]
        
        tool_context.state["solution_recommendations"] = solutions
        
        return {
            "status": "success",
            "solutions": solutions,
            "optimistic_narrative": optimistic_narrative,
            "total_recommendations": (len(solutions["immediate_actions"]) + 
                                   len(solutions["strategic_initiatives"]) + 
                                   len(solutions["preventive_measures"]))
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e)
        }
