# Email Template Integration Guide

## Overview

The new modern email templates are **100% backward compatible** with your existing `SmartEmailSender` class. You can switch between old and new templates with a single parameter.

## Quick Start

### Option 1: Use Modern Templates (Recommended)

\`\`\`python
# In your api/main.py or wherever you initialize SmartEmailSender
from core.email.smart_email_sender import SmartEmailSender

# Enable modern templates (default)
email_sender = SmartEmailSender(use_modern_template=True)
\`\`\`

### Option 2: Keep Using Legacy Templates

\`\`\`python
# Use your existing email format
email_sender = SmartEmailSender(use_modern_template=False)
\`\`\`

### Option 3: No Changes Required

\`\`\`python
# Modern templates are enabled by default
email_sender = SmartEmailSender()  # Automatically uses modern templates
\`\`\`

## What Changed

### Files Added
- `core/email/templates/modern_email_template.py` - New modern email templates
- `INTEGRATION_GUIDE.md` - This file

### Files Modified
- `core/email/smart_email_sender.py` - Enhanced with template switching capability

### No Breaking Changes
- All existing functionality preserved
- Legacy email generation still available
- Same API, same method signatures
- Same data flow and caching system

## Features of Modern Templates

✅ **Professional gradient header** with purple theme
✅ **Responsive design** - looks great on mobile and desktop
✅ **Color-coded scores** - visual indicators for performance
✅ **Card-based layout** - clean, modern stock cards
✅ **Sector summary** - aggregated sector performance
✅ **Price change indicators** - up/down arrows with colors
✅ **Score breakdowns** - momentum, value, quality, volatility
✅ **Summary statistics** - total stocks, sectors, top score

## Testing

### Test with Modern Templates

\`\`\`python
# Test the new templates
email_sender = SmartEmailSender(use_modern_template=True)
success = email_sender.send_daily_digest_email()
\`\`\`

### Test with Legacy Templates

\`\`\`python
# Test your original templates
email_sender = SmartEmailSender(use_modern_template=False)
success = email_sender.send_daily_digest_email()
\`\`\`

### A/B Testing

\`\`\`python
# Send to different recipients for comparison
modern_sender = SmartEmailSender(use_modern_template=True)
legacy_sender = SmartEmailSender(use_modern_template=False)

# Compare results
modern_sender.send_daily_digest_email()
legacy_sender.send_daily_digest_email()
\`\`\`

## Rollback Plan

If you encounter any issues:

1. **Immediate rollback**: Set `use_modern_template=False`
2. **Remove new files**: Delete `core/email/templates/` directory
3. **Revert changes**: Restore original `smart_email_sender.py` from git

## Environment Variables

No new environment variables required! Uses your existing configuration:
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `RECIPIENT_EMAIL`

## Customization

### Change Colors

Edit `core/email/templates/modern_email_template.py`:

\`\`\`python
# Header gradient
style="background: linear-gradient(135deg, #YOUR_COLOR_1 0%, #YOUR_COLOR_2 100%);"

# Score colors
def _get_score_color(score: float) -> str:
    if score >= 8.0:
        return "#YOUR_GREEN"  # High scores
    elif score >= 6.0:
        return "#YOUR_BLUE"   # Good scores
    # ... etc
\`\`\`

### Modify Layout

All HTML is in `ModernEmailTemplate.generate_html()` method. Edit the table structure to customize layout.

### Add New Sections

Add new methods to `ModernEmailTemplate` class:

\`\`\`python
@staticmethod
def _generate_custom_section(data: pd.DataFrame) -> str:
    """Your custom section HTML."""
    return "<div>Custom content</div>"
\`\`\`

## Support

If you encounter issues:

1. Check logs for error messages
2. Verify DataFrame structure matches expected format
3. Test with `use_modern_template=False` to isolate issues
4. Review the `_generate_legacy_html()` method for your original implementation

## Next Steps

1. **Deploy**: Push changes to your repository
2. **Test**: Run a test email with modern templates
3. **Monitor**: Check email delivery and rendering
4. **Iterate**: Customize colors and layout as needed
5. **Go Live**: Set as default once satisfied

---

**Questions?** Review the code comments in `modern_email_template.py` for detailed documentation.
