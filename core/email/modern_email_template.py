#!/usr/bin/env python3
"""
Modern Email Templates for Stock Digest
Beautiful, responsive HTML email templates with professional design
"""

from datetime import datetime
from typing import List, Dict, Any
import pandas as pd


class ModernEmailTemplate:
    """
    Modern email template generator with professional design.
    Drop-in replacement for existing email generation.
    """
    
    @staticmethod
    def generate_html(scored_stocks: pd.DataFrame, generation_time: str = None) -> str:
        """
        Generate modern HTML email from scored stocks DataFrame.
        
        Args:
            scored_stocks: DataFrame with stock data and scores
            generation_time: Optional timestamp string
            
        Returns:
            str: Complete HTML email content
        """
        if generation_time is None:
            generation_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        # Prepare data
        top_stocks = scored_stocks.nlargest(10, 'total_score') if len(scored_stocks) > 10 else scored_stocks
        sector_summary = ModernEmailTemplate._get_sector_summary(scored_stocks)
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily Stock Digest</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f5f5f7; line-height: 1.6;">
    
     Email Container 
    <table role="presentation" style="width: 100%; border-collapse: collapse; background-color: #f5f5f7;">
        <tr>
            <td style="padding: 40px 20px;">
                
                 Main Content 
                <table role="presentation" style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                    
                     Header 
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 40px 30px; text-align: center;">
                            <h1 style="margin: 0; color: #ffffff; font-size: 32px; font-weight: 700; letter-spacing: -0.5px;">
                                📈 Daily Stock Digest
                            </h1>
                            <p style="margin: 10px 0 0 0; color: rgba(255, 255, 255, 0.9); font-size: 16px;">
                                {generation_time}
                            </p>
                        </td>
                    </tr>
                    
                     Summary Stats 
                    <tr>
                        <td style="padding: 30px;">
                            <table role="presentation" style="width: 100%; border-collapse: collapse;">
                                <tr>
                                    <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #f8f9fa; border-radius: 8px;">
                                        <div style="font-size: 28px; font-weight: 700; color: #667eea; margin-bottom: 5px;">
                                            {len(scored_stocks)}
                                        </div>
                                        <div style="font-size: 12px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px;">
                                            Stocks Analyzed
                                        </div>
                                    </td>
                                    <td style="width: 10px;"></td>
                                    <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #f8f9fa; border-radius: 8px;">
                                        <div style="font-size: 28px; font-weight: 700; color: #28a745; margin-bottom: 5px;">
                                            {len(sector_summary)}
                                        </div>
                                        <div style="font-size: 12px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px;">
                                            Sectors
                                        </div>
                                    </td>
                                    <td style="width: 10px;"></td>
                                    <td style="width: 33.33%; text-align: center; padding: 15px; background-color: #f8f9fa; border-radius: 8px;">
                                        <div style="font-size: 28px; font-weight: 700; color: #764ba2; margin-bottom: 5px;">
                                            {top_stocks.iloc[0]['total_score']:.1f}
                                        </div>
                                        <div style="font-size: 12px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px;">
                                            Top Score
                                        </div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                     Top Stocks Section 
                    <tr>
                        <td style="padding: 0 30px 30px 30px;">
                            <h2 style="margin: 0 0 20px 0; color: #1a1a1a; font-size: 24px; font-weight: 700;">
                                🏆 Top Performing Stocks
                            </h2>
                            
                            {ModernEmailTemplate._generate_stock_cards(top_stocks)}
                        </td>
                    </tr>
                    
                     Sector Summary 
                    <tr>
                        <td style="padding: 0 30px 30px 30px;">
                            <h2 style="margin: 0 0 20px 0; color: #1a1a1a; font-size: 24px; font-weight: 700;">
                                📊 Sector Performance
                            </h2>
                            
                            {ModernEmailTemplate._generate_sector_summary(sector_summary)}
                        </td>
                    </tr>
                    
                     Footer 
                    <tr>
                        <td style="background-color: #f8f9fa; padding: 30px; text-align: center; border-top: 1px solid #e9ecef;">
                            <p style="margin: 0 0 10px 0; color: #6c757d; font-size: 14px;">
                                Generated by Stock Digest Platform
                            </p>
                            <p style="margin: 0; color: #adb5bd; font-size: 12px;">
                                Data sourced from Yahoo Finance & Alpha Vantage
                            </p>
                        </td>
                    </tr>
                    
                </table>
                
            </td>
        </tr>
    </table>
    
</body>
</html>
"""
        return html
    
    @staticmethod
    def _generate_stock_cards(stocks: pd.DataFrame) -> str:
        """Generate HTML for stock cards."""
        cards_html = ""
        
        for idx, row in stocks.iterrows():
            # Determine price change color and arrow
            price_change = row.get('price_change_pct', 0)
            if price_change > 0:
                change_color = "#28a745"
                arrow = "↑"
            elif price_change < 0:
                change_color = "#dc3545"
                arrow = "↓"
            else:
                change_color = "#6c757d"
                arrow = "→"
            
            # Get score color
            total_score = row.get('total_score', 0)
            score_color = ModernEmailTemplate._get_score_color(total_score)
            
            cards_html += f"""
            <table role="presentation" style="width: 100%; border-collapse: collapse; margin-bottom: 15px; background-color: #ffffff; border: 1px solid #e9ecef; border-radius: 8px; overflow: hidden;">
                <tr>
                    <td style="padding: 20px;">
                        <table role="presentation" style="width: 100%;">
                            <tr>
                                <td style="width: 70%;">
                                    <div style="font-size: 18px; font-weight: 700; color: #1a1a1a; margin-bottom: 5px;">
                                        {row.get('ticker', 'N/A')}
                                    </div>
                                    <div style="font-size: 14px; color: #6c757d; margin-bottom: 10px;">
                                        {row.get('sector', 'Unknown Sector')}
                                    </div>
                                    <div style="font-size: 16px; color: {change_color}; font-weight: 600;">
                                        {arrow} {abs(price_change):.2f}%
                                    </div>
                                </td>
                                <td style="width: 30%; text-align: right; vertical-align: top;">
                                    <div style="display: inline-block; background-color: {score_color}; color: #ffffff; padding: 8px 16px; border-radius: 20px; font-size: 16px; font-weight: 700;">
                                        {total_score:.1f}
                                    </div>
                                </td>
                            </tr>
                        </table>
                        
                         Score Breakdown 
                        <table role="presentation" style="width: 100%; margin-top: 15px; font-size: 12px; color: #6c757d;">
                            <tr>
                                <td style="padding: 5px 0;">
                                    <span style="display: inline-block; width: 80px;">Momentum:</span>
                                    <span style="font-weight: 600; color: #1a1a1a;">{row.get('momentum_score', 0):.1f}</span>
                                </td>
                                <td style="padding: 5px 0;">
                                    <span style="display: inline-block; width: 80px;">Value:</span>
                                    <span style="font-weight: 600; color: #1a1a1a;">{row.get('value_score', 0):.1f}</span>
                                </td>
                            </tr>
                            <tr>
                                <td style="padding: 5px 0;">
                                    <span style="display: inline-block; width: 80px;">Quality:</span>
                                    <span style="font-weight: 600; color: #1a1a1a;">{row.get('quality_score', 0):.1f}</span>
                                </td>
                                <td style="padding: 5px 0;">
                                    <span style="display: inline-block; width: 80px;">Volatility:</span>
                                    <span style="font-weight: 600; color: #1a1a1a;">{row.get('volatility_score', 0):.1f}</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
            """
        
        return cards_html
    
    @staticmethod
    def _generate_sector_summary(sector_summary: Dict[str, Dict[str, Any]]) -> str:
        """Generate HTML for sector summary."""
        sector_html = ""
        
        for sector, data in sorted(sector_summary.items(), key=lambda x: x[1]['avg_score'], reverse=True):
            avg_score = data['avg_score']
            count = data['count']
            score_color = ModernEmailTemplate._get_score_color(avg_score)
            
            sector_html += f"""
            <table role="presentation" style="width: 100%; border-collapse: collapse; margin-bottom: 12px;">
                <tr>
                    <td style="padding: 15px; background-color: #f8f9fa; border-radius: 8px;">
                        <table role="presentation" style="width: 100%;">
                            <tr>
                                <td style="width: 60%;">
                                    <div style="font-size: 16px; font-weight: 600; color: #1a1a1a; margin-bottom: 5px;">
                                        {sector}
                                    </div>
                                    <div style="font-size: 13px; color: #6c757d;">
                                        {count} stock{'s' if count != 1 else ''}
                                    </div>
                                </td>
                                <td style="width: 40%; text-align: right;">
                                    <div style="display: inline-block; background-color: {score_color}; color: #ffffff; padding: 6px 14px; border-radius: 16px; font-size: 14px; font-weight: 700;">
                                        {avg_score:.1f}
                                    </div>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
            """
        
        return sector_html
    
    @staticmethod
    def _get_sector_summary(scored_stocks: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Calculate sector summary statistics."""
        sector_summary = {}
        
        for sector in scored_stocks['sector'].unique():
            sector_stocks = scored_stocks[scored_stocks['sector'] == sector]
            sector_summary[sector] = {
                'count': len(sector_stocks),
                'avg_score': sector_stocks['total_score'].mean()
            }
        
        return sector_summary
    
    @staticmethod
    def _get_score_color(score: float) -> str:
        """Get color based on score value."""
        if score >= 8.0:
            return "#28a745"  # Green
        elif score >= 6.0:
            return "#667eea"  # Purple
        elif score >= 4.0:
            return "#ffc107"  # Yellow
        else:
            return "#dc3545"  # Red
    
    @staticmethod
    def generate_text(scored_stocks: pd.DataFrame, generation_time: str = None) -> str:
        """
        Generate plain text version of email.
        
        Args:
            scored_stocks: DataFrame with stock data and scores
            generation_time: Optional timestamp string
            
        Returns:
            str: Plain text email content
        """
        if generation_time is None:
            generation_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        top_stocks = scored_stocks.nlargest(10, 'total_score') if len(scored_stocks) > 10 else scored_stocks
        
        text = f"""
DAILY STOCK DIGEST
{generation_time}

{'='*60}

SUMMARY
-------
Total Stocks Analyzed: {len(scored_stocks)}
Sectors Covered: {len(scored_stocks['sector'].unique())}
Top Score: {top_stocks.iloc[0]['total_score']:.1f}

{'='*60}

TOP PERFORMING STOCKS
---------------------

"""
        
        for idx, row in top_stocks.iterrows():
            price_change = row.get('price_change_pct', 0)
            arrow = "↑" if price_change > 0 else "↓" if price_change < 0 else "→"
            
            text += f"""
{row.get('ticker', 'N/A')} - Score: {row.get('total_score', 0):.1f}
Sector: {row.get('sector', 'Unknown')}
Price Change: {arrow} {abs(price_change):.2f}%
Scores - Momentum: {row.get('momentum_score', 0):.1f} | Value: {row.get('value_score', 0):.1f} | Quality: {row.get('quality_score', 0):.1f} | Volatility: {row.get('volatility_score', 0):.1f}

"""
        
        text += f"""
{'='*60}

Generated by Stock Digest Platform
Data sourced from Yahoo Finance & Alpha Vantage
"""
        
        return text
