"""
Modern HTML Email Templates for Stock Digest
Beautiful, responsive email templates with professional styling
"""

from datetime import datetime
from typing import List, Dict, Any
import pandas as pd


class EmailTemplate:
    """Generate beautiful HTML email templates for stock digests."""
    
    @staticmethod
    def generate_daily_digest_html(scored_stocks: pd.DataFrame, metadata: Dict[str, Any] = None) -> str:
        """
        Generate a beautiful HTML email for daily stock digest.
        
        Args:
            scored_stocks: DataFrame with stock data and scores
            metadata: Optional metadata (generation time, stock count, etc.)
        
        Returns:
            str: Complete HTML email content
        """
        if metadata is None:
            metadata = {}
        
        # Prepare data
        generation_time = metadata.get('generation_time', datetime.now().strftime('%B %d, %Y at %I:%M %p'))
        total_stocks = len(scored_stocks)
        
        # Get top performers
        top_stocks = scored_stocks.nlargest(10, 'total_score') if 'total_score' in scored_stocks.columns else scored_stocks.head(10)
        
        # Get sector breakdown
        sector_summary = EmailTemplate._get_sector_summary(scored_stocks)
        
        # Build HTML
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily Stock Digest</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f5f5f7; line-height: 1.6;">
    
     Main Container 
    <table role="presentation" style="width: 100%; border-collapse: collapse; background-color: #f5f5f7;">
        <tr>
            <td style="padding: 40px 20px;">
                
                 Email Content 
                <table role="presentation" style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.07);">
                    
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
                                    <td style="width: 50%; padding: 20px; background-color: #f8f9fa; border-radius: 8px; text-align: center;">
                                        <div style="font-size: 36px; font-weight: 700; color: #667eea; margin-bottom: 5px;">
                                            {total_stocks}
                                        </div>
                                        <div style="font-size: 14px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px;">
                                            Stocks Analyzed
                                        </div>
                                    </td>
                                    <td style="width: 10px;"></td>
                                    <td style="width: 50%; padding: 20px; background-color: #f8f9fa; border-radius: 8px; text-align: center;">
                                        <div style="font-size: 36px; font-weight: 700; color: #764ba2; margin-bottom: 5px;">
                                            {len(sector_summary)}
                                        </div>
                                        <div style="font-size: 14px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px;">
                                            Sectors Covered
                                        </div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                     Top Performers Section 
                    <tr>
                        <td style="padding: 0 30px 30px 30px;">
                            <h2 style="margin: 0 0 20px 0; color: #1a1a1a; font-size: 24px; font-weight: 700;">
                                🏆 Top Performers
                            </h2>
                            
                            {EmailTemplate._generate_stock_cards(top_stocks)}
                        </td>
                    </tr>
                    
                     Sector Breakdown 
                    <tr>
                        <td style="padding: 0 30px 30px 30px;">
                            <h2 style="margin: 0 0 20px 0; color: #1a1a1a; font-size: 24px; font-weight: 700;">
                                📊 Sector Breakdown
                            </h2>
                            
                            {EmailTemplate._generate_sector_cards(sector_summary)}
                        </td>
                    </tr>
                    
                     Footer 
                    <tr>
                        <td style="background-color: #f8f9fa; padding: 30px; text-align: center; border-top: 1px solid #e9ecef;">
                            <p style="margin: 0 0 10px 0; color: #6c757d; font-size: 14px;">
                                Generated by Stock Digest Platform
                            </p>
                            <p style="margin: 0; color: #adb5bd; font-size: 12px;">
                                This digest is for informational purposes only and does not constitute investment advice.
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
        """Generate HTML cards for individual stocks."""
        cards_html = ""
        
        for idx, row in stocks.iterrows():
            ticker = row.get('ticker', 'N/A')
            company_name = row.get('company_name', ticker)
            sector = row.get('sector', 'Unknown')
            total_score = row.get('total_score', 0)
            price = row.get('current_price', 0)
            change_percent = row.get('change_percent', 0)
            
            # Determine color based on change
            change_color = '#10b981' if change_percent >= 0 else '#ef4444'
            change_symbol = '▲' if change_percent >= 0 else '▼'
            
            # Score color gradient
            score_color = EmailTemplate._get_score_color(total_score)
            
            card_html = f"""
            <div style="background-color: #ffffff; border: 1px solid #e9ecef; border-radius: 8px; padding: 20px; margin-bottom: 15px;">
                <table role="presentation" style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="width: 70%;">
                            <div style="font-size: 18px; font-weight: 700; color: #1a1a1a; margin-bottom: 5px;">
                                {ticker}
                            </div>
                            <div style="font-size: 14px; color: #6c757d; margin-bottom: 8px;">
                                {company_name}
                            </div>
                            <div style="display: inline-block; background-color: #f8f9fa; padding: 4px 10px; border-radius: 4px; font-size: 12px; color: #495057;">
                                {sector}
                            </div>
                        </td>
                        <td style="width: 30%; text-align: right; vertical-align: top;">
                            <div style="font-size: 20px; font-weight: 700; color: #1a1a1a; margin-bottom: 5px;">
                                ${price:.2f}
                            </div>
                            <div style="font-size: 14px; font-weight: 600; color: {change_color};">
                                {change_symbol} {abs(change_percent):.2f}%
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td colspan="2" style="padding-top: 15px;">
                            <div style="display: flex; align-items: center;">
                                <div style="flex: 1; background-color: #e9ecef; height: 8px; border-radius: 4px; overflow: hidden;">
                                    <div style="width: {min(total_score, 100)}%; height: 100%; background-color: {score_color}; border-radius: 4px;"></div>
                                </div>
                                <div style="margin-left: 10px; font-size: 14px; font-weight: 600; color: {score_color};">
                                    {total_score:.0f}
                                </div>
                            </div>
                        </td>
                    </tr>
                </table>
            </div>
            """
            cards_html += card_html
        
        return cards_html
    
    @staticmethod
    def _generate_sector_cards(sector_summary: List[Dict[str, Any]]) -> str:
        """Generate HTML cards for sector breakdown."""
        cards_html = ""
        
        for sector_data in sector_summary:
            sector_name = sector_data.get('sector', 'Unknown')
            stock_count = sector_data.get('count', 0)
            avg_score = sector_data.get('avg_score', 0)
            
            score_color = EmailTemplate._get_score_color(avg_score)
            
            card_html = f"""
            <div style="background-color: #f8f9fa; border-left: 4px solid {score_color}; border-radius: 6px; padding: 15px 20px; margin-bottom: 12px;">
                <table role="presentation" style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="width: 60%;">
                            <div style="font-size: 16px; font-weight: 600; color: #1a1a1a; margin-bottom: 5px;">
                                {sector_name}
                            </div>
                            <div style="font-size: 13px; color: #6c757d;">
                                {stock_count} stocks
                            </div>
                        </td>
                        <td style="width: 40%; text-align: right;">
                            <div style="font-size: 24px; font-weight: 700; color: {score_color};">
                                {avg_score:.0f}
                            </div>
                            <div style="font-size: 12px; color: #6c757d;">
                                Avg Score
                            </div>
                        </td>
                    </tr>
                </table>
            </div>
            """
            cards_html += card_html
        
        return cards_html
    
    @staticmethod
    def _get_sector_summary(stocks: pd.DataFrame) -> List[Dict[str, Any]]:
        """Get sector summary statistics."""
        if 'sector' not in stocks.columns:
            return []
        
        sector_groups = stocks.groupby('sector').agg({
            'ticker': 'count',
            'total_score': 'mean'
        }).reset_index()
        
        sector_groups.columns = ['sector', 'count', 'avg_score']
        sector_groups = sector_groups.sort_values('avg_score', ascending=False)
        
        return sector_groups.to_dict('records')
    
    @staticmethod
    def _get_score_color(score: float) -> str:
        """Get color based on score value."""
        if score >= 80:
            return '#10b981'  # Green
        elif score >= 60:
            return '#3b82f6'  # Blue
        elif score >= 40:
            return '#f59e0b'  # Orange
        else:
            return '#ef4444'  # Red
    
    @staticmethod
    def generate_plain_text(scored_stocks: pd.DataFrame, metadata: Dict[str, Any] = None) -> str:
        """
        Generate plain text version of the email for email clients that don't support HTML.
        
        Args:
            scored_stocks: DataFrame with stock data and scores
            metadata: Optional metadata
        
        Returns:
            str: Plain text email content
        """
        if metadata is None:
            metadata = {}
        
        generation_time = metadata.get('generation_time', datetime.now().strftime('%B %d, %Y at %I:%M %p'))
        total_stocks = len(scored_stocks)
        
        top_stocks = scored_stocks.nlargest(10, 'total_score') if 'total_score' in scored_stocks.columns else scored_stocks.head(10)
        
        text = f"""
DAILY STOCK DIGEST
{generation_time}

{'=' * 60}

SUMMARY
-------
Total Stocks Analyzed: {total_stocks}

TOP PERFORMERS
--------------
"""
        
        for idx, row in top_stocks.iterrows():
            ticker = row.get('ticker', 'N/A')
            company_name = row.get('company_name', ticker)
            sector = row.get('sector', 'Unknown')
            total_score = row.get('total_score', 0)
            price = row.get('current_price', 0)
            change_percent = row.get('change_percent', 0)
            
            change_symbol = '▲' if change_percent >= 0 else '▼'
            
            text += f"""
{ticker} - {company_name}
Sector: {sector}
Price: ${price:.2f} {change_symbol} {abs(change_percent):.2f}%
Score: {total_score:.0f}/100

"""
        
        text += """
{'=' * 60}

This digest is for informational purposes only and does not constitute investment advice.

Generated by Stock Digest Platform
"""
        
        return text
