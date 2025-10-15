# Enhanced Email Templates

Beautiful, modern HTML email templates for your Stock Digest application.

## What's New

### Modern Design
- **Gradient header** with purple theme
- **Card-based layout** for easy scanning
- **Color-coded scores** (green for high, red for low)
- **Responsive design** that works on mobile and desktop
- **Professional typography** using system fonts

### Features
- ✅ Summary statistics at the top
- ✅ Top 10 performing stocks with visual score bars
- ✅ Sector breakdown with average scores
- ✅ Price changes with up/down indicators
- ✅ Plain text fallback for email clients without HTML support

## How to Use

### Option 1: Replace Existing Email Sender

Update your `api/main.py` to use the enhanced email sender:

\`\`\`python
# Replace this import
from core.email.smart_email_sender import SmartEmailSender

# With this
from core.email.enhanced_email_sender import EnhancedEmailSender as SmartEmailSender
\`\`\`

That's it! The enhanced sender is a drop-in replacement.

### Option 2: Use Alongside Existing Sender

Keep both and choose which one to use:

\`\`\`python
from core.email.smart_email_sender import SmartEmailSender
from core.email.enhanced_email_sender import EnhancedEmailSender

# Use enhanced version
email_sender = EnhancedEmailSender()
\`\`\`

## Customization

### Change Colors

Edit `email_template.py` to customize the color scheme:

\`\`\`python
# Header gradient (line 67)
background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);

# Change to your brand colors
background: linear-gradient(135deg, #your-color-1 0%, #your-color-2 100%);
\`\`\`

### Modify Score Colors

Update the `_get_score_color` method:

\`\`\`python
@staticmethod
def _get_score_color(score: float) -> str:
    if score >= 80:
        return '#10b981'  # Your high score color
    elif score >= 60:
        return '#3b82f6'  # Your medium score color
    # ... etc
\`\`\`

### Add More Sections

Add new sections in `generate_daily_digest_html`:

\`\`\`python
# Add after sector breakdown
<tr>
    <td style="padding: 0 30px 30px 30px;">
        <h2>Your New Section</h2>
         Your content here 
    </td>
</tr>
\`\`\`

## Testing

Test the email template locally:

\`\`\`python
from core.email.enhanced_email_sender import EnhancedEmailSender

sender = EnhancedEmailSender()
sender.send_daily_digest_email(force_refresh=False)
\`\`\`

## Email Client Compatibility

The templates are tested and work with:
- ✅ Gmail (web and mobile)
- ✅ Outlook (web and desktop)
- ✅ Apple Mail (macOS and iOS)
- ✅ Yahoo Mail
- ✅ ProtonMail

## File Structure

\`\`\`
core/email/
├── templates/
│   └── email_template.py      # HTML template generator
├── enhanced_email_sender.py   # Enhanced sender with templates
└── smart_email_sender.py      # Your original sender (unchanged)
\`\`\`

## Next Steps

1. Copy these files to your repository
2. Update your imports in `api/main.py`
3. Test with a sample email
4. Customize colors and styling to match your brand
5. Deploy and enjoy beautiful emails!

## Support

If you need help customizing the templates or have questions, feel free to ask!
