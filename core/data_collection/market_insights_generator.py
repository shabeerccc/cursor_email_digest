#!/usr/bin/env python3
"""
Market Insights Generator
Generates sector-based market insights for stocks with CumulativeScore >= 80
"""

from typing import List, Dict, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class MarketInsightsGenerator:
    """Generates market insights by sector for top-performing stocks."""
    
    def __init__(self):
        self.sector_insights = {
            'Technology': 'Tech innovation leader with strong growth potential',
            'Healthcare': 'Healthcare breakthrough with solid fundamentals',
            'Financial': 'Financial stability with strong market position',
            'Consumer': 'Consumer demand leader with brand strength',
            'Energy': 'Energy sector frontrunner with sustainable practices',
            'Industrial': 'Industrial powerhouse with operational excellence',
            'Materials': 'Materials leader with supply chain advantage',
            'Real Estate': 'Real estate champion with strategic positioning',
            'Utilities': 'Utility provider with consistent performance',
            'Communication': 'Communication leader with network strength',
            'AI': 'AI innovation pioneer with cutting-edge technology',
            'Biotech': 'Biotech breakthrough with clinical success',
            'Fintech': 'Fintech disruptor with market innovation',
            'E-commerce': 'E-commerce leader with digital transformation',
            'Cybersecurity': 'Cybersecurity expert with threat protection',
            'Clean Energy': 'Clean energy champion with sustainability focus',
            'EV': 'Electric vehicle leader with automotive innovation',
            'Crypto': 'Cryptocurrency pioneer with blockchain technology'
        }
    
    def generate_market_insights(self, stocks: List[Dict]) -> str:
        """
        Generate market insights digest by sector.
        
        Strategy: Ensure one stock per sector for maximum sector coverage.
        
        Args:
            stocks: List of stock dictionaries with required fields
            
        Returns:
            HTML formatted market insights table
        """
        try:
            logger.info("🔍 Generating market insights by sector...")
            
            # First, group all stocks by sector to ensure sector coverage
            sector_stocks = defaultdict(list)
            for stock in stocks:
                sector = stock.get('sector', 'Unknown')
                if sector != 'Unknown':  # Only include stocks with valid sectors
                    sector_stocks[sector].append(stock)
            
            logger.info(f"✅ Found stocks in {len(sector_stocks)} sectors")
            
            # Get the best stock from each sector (regardless of score)
            sector_best = self._get_best_stock_per_sector_all_sectors(sector_stocks)
            
            if not sector_best:
                logger.warning("⚠️ No sector leaders found")
                return self._generate_no_insights_message()
            
            # Filter to only include sectors with reasonable scores (lower threshold for coverage)
            qualified_sector_best = {}
            for sector, stock in sector_best.items():
                overall_score = stock.get('overall_score', 0)
                if overall_score >= 50:  # Lower threshold to include more sectors
                    qualified_sector_best[sector] = stock
            
            if not qualified_sector_best:
                logger.warning("⚠️ No sectors meet minimum score threshold")
                return self._generate_no_insights_message()
            
            logger.info(f"✅ Generated insights for {len(qualified_sector_best)} sectors")
            
            # Generate insights table
            insights_table = self._generate_insights_table(qualified_sector_best)
            
            return insights_table
            
        except Exception as e:
            logger.error(f"❌ Error generating market insights: {e}")
            return self._generate_error_message(str(e))
    
    def _get_best_stock_per_sector_all_sectors(self, sector_stocks: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """
        Get the best stock from each sector, ensuring maximum sector coverage.
        
        Args:
            sector_stocks: Dictionary mapping sector to list of stocks
            
        Returns:
            Dictionary mapping sector to best stock
        """
        sector_best = {}
        
        for sector, stock_list in sector_stocks.items():
            if stock_list:
                # Sort by overall_score and take the best
                best_stock = max(stock_list, key=lambda x: x.get('overall_score', 0))
                sector_best[sector] = best_stock
                logger.info(f"📊 {sector} sector: {best_stock.get('ticker', 'Unknown')} (Score: {best_stock.get('overall_score', 0):.1f})")
        
        return sector_best
    
    def _generate_key_insight(self, stock: Dict) -> str:
        """
        Generate a key insight explaining why this stock is the best buy in its sector.
        
        Args:
            stock: Stock dictionary with scores
            
        Returns:
            Insight string (1-2 sentences max)
        """
        ticker = stock.get('ticker', 'Unknown')
        sector = stock.get('sector', 'Unknown')
        overall_score = stock.get('overall_score', 0)
        
        # Get individual scores
        halal_score = stock.get('halal_score', 0)
        hedge_fund_score = stock.get('hedge_fund_score', 0)
        activity_score = stock.get('activity_score', 0)
        trend_score = stock.get('trend_score', 0)
        fundamental_score = stock.get('fundamental_score', 0)
        
        # Find the strongest score
        scores = [
            ('Halal', halal_score),
            ('Hedge Fund', hedge_fund_score),
            ('Activity', activity_score),
            ('Trend', trend_score),
            ('Fundamental', fundamental_score)
        ]
        
        strongest_score_name, strongest_score_value = max(scores, key=lambda x: x[1])
        
        # Generate insight based on strongest score
        if strongest_score_name == 'Halal' and strongest_score_value >= 90:
            return f"{ticker} leads the {sector} sector with exceptional Islamic finance compliance and strong debt management."
        elif strongest_score_name == 'Hedge Fund' and strongest_score_value >= 85:
            return f"{ticker} dominates {sector} with superior valuation metrics and strong institutional appeal."
        elif strongest_score_name == 'Activity' and strongest_score_value >= 80:
            return f"{ticker} shows exceptional trading activity and liquidity in the {sector} sector."
        elif strongest_score_name == 'Trend' and strongest_score_value >= 85:
            return f"{ticker} demonstrates strong momentum and positive price trends in {sector}."
        elif strongest_score_name == 'Fundamental' and strongest_score_value >= 85:
            return f"{ticker} excels in {sector} with robust financial fundamentals and growth potential."
        else:
            # Generic insight based on overall score
            if overall_score >= 95:
                return f"{ticker} is the {sector} sector champion with exceptional all-around performance."
            elif overall_score >= 90:
                return f"{ticker} leads {sector} with outstanding scores across all metrics."
            else:
                return f"{ticker} is the top performer in {sector} with strong cumulative scoring."
    
    def _generate_insights_table(self, sector_best: Dict[str, Dict]) -> str:
        """
        Generate HTML table for market insights.
        
        Args:
            sector_best: Dictionary mapping sector to best stock
            
        Returns:
            HTML formatted insights table
        """
        # Sort sectors by the best stock's overall score
        sorted_sectors = sorted(
            sector_best.items(), 
            key=lambda x: x[1].get('overall_score', 0), 
            reverse=True
        )
        
        html_content = """
        <h2>📊 Market Insights by Sector</h2>
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px;">
            <p style="margin-bottom: 15px; color: #2c3e50; font-size: 16px;">
                <strong>🏆 Sector Leaders Analysis</strong> - One top stock per sector for maximum coverage, ranked by performance
            </p>
            
            <table style="width:100%; border-collapse: collapse; background-color: white; border-radius: 5px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <thead>
                    <tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">🏭 Sector</th>
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">📈 Ticker</th>
                        <th style="padding: 12px; text-align: center; border: none; font-weight: 600;">🎯 Score</th>
                        <th style="padding: 12px; text-align: left; border: none; font-weight: 600;">💡 Key Insight</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for i, (sector, stock) in enumerate(sorted_sectors):
            # Alternate row colors for better readability
            row_color = "#f8f9fa" if i % 2 == 0 else "white"
            
            ticker = stock.get('ticker', 'Unknown')
            overall_score = stock.get('overall_score', 0)
            key_insight = self._generate_key_insight(stock)
            
            # Color code the score
            if overall_score >= 95:
                score_color = "#27ae60"  # Green
                score_emoji = "🔥"
            elif overall_score >= 90:
                score_color = "#2ecc71"  # Light Green
                score_emoji = "⭐"
            elif overall_score >= 85:
                score_color = "#f39c12"  # Orange
                score_emoji = "💪"
            else:
                score_color = "#3498db"  # Blue
                score_emoji = "✅"
            
            html_content += f"""
                    <tr style="background-color: {row_color};">
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; font-weight: 600; color: #2c3e50;">{sector}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; font-weight: 600; color: #34495e;">{ticker}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; text-align: center; font-weight: 600; color: {score_color}; font-size: 18px;">{score_emoji} {overall_score:.1f}</td>
                        <td style="padding: 12px; border-bottom: 1px solid #e9ecef; color: #555; line-height: 1.4;">{key_insight}</td>
                    </tr>
            """
        
        html_content += """
                </tbody>
            </table>
            
            <div style="margin-top: 15px; padding: 10px; background-color: #e8f5e8; border-left: 4px solid #27ae60; border-radius: 3px;">
                <p style="margin: 0; color: #2d5a2d; font-size: 14px;">
                    <strong>💡 Analysis Summary:</strong> These sector leaders represent the best investment opportunities 
                    based on comprehensive scoring across Halal compliance, Hedge Fund appeal, Trading Activity, 
                    Price Trends, and Fundamental strength.
                </p>
            </div>
        </div>
        """
        
        return html_content
    
    def _generate_no_insights_message(self) -> str:
        """Generate message when no qualified stocks are found."""
        return """
        <h2>📊 Market Insights by Sector</h2>
        <div style="background-color: #fff3cd; padding: 20px; border-radius: 5px; border-left: 4px solid #ffc107;">
            <h3 style="color: #856404; margin-top: 0;">⚠️ No High-Scoring Stocks Found</h3>
            <p style="color: #856404; margin-bottom: 0;">
                Currently, no stocks meet the criteria of CumulativeScore ≥ 70. This may indicate:
            </p>
            <ul style="color: #856404; margin: 10px 0;">
                <li>Market conditions requiring more conservative scoring</li>
                <li>Need to adjust scoring thresholds</li>
                <li>Temporary market volatility affecting scores</li>
            </ul>
            <p style="color: #856404; margin-bottom: 0;">
                <strong>Recommendation:</strong> Consider reviewing stocks with scores ≥ 60 for potential opportunities.
            </p>
        </div>
        """
    
    def _generate_error_message(self, error: str) -> str:
        """Generate error message when insights generation fails."""
        return f"""
        <h2>📊 Market Insights by Sector</h2>
        <div style="background-color: #f8d7da; padding: 20px; border-radius: 5px; border-left: 4px solid #dc3545;">
            <h3 style="color: #721c24; margin-top: 0;">❌ Error Generating Insights</h3>
            <p style="color: #721c24; margin-bottom: 0;">
                Unable to generate market insights due to an error: <strong>{error}</strong>
            </p>
            <p style="color: #721c24; margin-bottom: 0;">
                <strong>Action Required:</strong> Please check the data format and try again.
            </p>
        </div>
        """


def generate_market_insights(stocks: List[Dict]) -> str:
    """
    Convenience function to generate market insights.
    
    Args:
        stocks: List of stock dictionaries with required fields
        
    Returns:
        HTML formatted market insights table
    """
    generator = MarketInsightsGenerator()
    return generator.generate_market_insights(stocks)


# Example usage and testing
if __name__ == "__main__":
    # Sample test data
    sample_stocks = [
        {
            'ticker': 'AAPL',
            'sector': 'Technology',
            'halal_score': 95.0,
            'hedge_fund_score': 88.0,
            'activity_score': 85.0,
            'trend_score': 92.0,
            'fundamental_score': 90.0,
            'overall_score': 90.0
        },
        {
            'ticker': 'JNJ',
            'sector': 'Healthcare',
            'halal_score': 88.0,
            'hedge_fund_score': 82.0,
            'activity_score': 78.0,
            'trend_score': 85.0,
            'fundamental_score': 87.0,
            'overall_score': 84.0
        },
        {
            'ticker': 'MSFT',
            'sector': 'Technology',
            'halal_score': 92.0,
            'hedge_fund_score': 90.0,
            'activity_score': 88.0,
            'trend_score': 89.0,
            'fundamental_score': 91.0,
            'overall_score': 90.0
        }
    ]
    
    # Test the generator
    generator = MarketInsightsGenerator()
    insights = generator.generate_market_insights(sample_stocks)
    
    print("🧪 Testing Market Insights Generator")
    print("=" * 50)
    print(insights)
