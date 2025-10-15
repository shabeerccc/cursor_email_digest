#!/usr/bin/env python3
"""
Send Email Now - Direct Email Sending Script
Uses the Stock Digest email system to send an email immediately
"""

import os
import sys
from datetime import datetime

def send_email_now():
    """Send an email using the Stock Digest email system."""
    print("📧 Stock Digest Email Sender")
    print("=" * 40)
    
    try:
        # Import the email sender
        from core.email.smart_email_sender import SmartEmailSender
        print("✅ Email system imported successfully")
        
        # Initialize email sender
        email_sender = SmartEmailSender()
        print("✅ Email sender initialized")
        
        # Get recipient email from environment or use default
        recipient_email = os.getenv("RECIPIENT_EMAIL")
        if not recipient_email:
            print("⚠️ No RECIPIENT_EMAIL in environment, using default")
            recipient_email = "your-email@example.com"  # Replace with your email
        
        print(f"📤 Sending email to: {recipient_email}")
        print("⏳ This may take a few minutes...")
        
        # Send the daily digest email
        success = email_sender.send_daily_digest_email(force_refresh=False)
        
        if success:
            print("✅ Email sent successfully!")
            print(f"📅 Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("❌ Email sending failed")
            
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    print("🚀 Starting email sending process...")
    success = send_email_now()
    
    if success:
        print("\n🎉 Email process completed!")
    else:
        print("\n⚠️ Email process had issues. Check the logs above.")
        sys.exit(1)
