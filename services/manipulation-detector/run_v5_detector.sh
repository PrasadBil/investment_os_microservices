
#!/bin/bash
# ============================================================================
# v5.0 MANIPULATION DETECTOR - DAILY RUN SCRIPT (Phase 2 Migration)
# ============================================================================
# Investment OS - Empire Manipulation Detection
#
# PURPOSE: Scan 296 CSE stocks for manipulation patterns daily
# SCHEDULE: Every day 7:00 PM (19:00)
# OUTPUT: Daily report + email via common.email_sender
#
# Migration Notes:
# - WORK_DIR changed: /opt/selenium_automation → /opt/investment-os
# - send_v5_email.py ELIMINATED → common.email_sender.EmailSender
# - Python scripts use common library (no more inline create_client/load_dotenv)
# - PYTHONPATH already configured in ~/.bashrc (Phase 1)
#
# Original: /opt/selenium_automation/run_v5_detector.sh
# ============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

WORK_DIR="/opt/investment-os"
SERVICE_DIR="${WORK_DIR}/services/manipulation-detector"
LOG_DIR="${WORK_DIR}/v5_logs"
REPORT_DIR="${WORK_DIR}/v5_reports"

# Create directories
mkdir -p "$LOG_DIR"
mkdir -p "$REPORT_DIR"

# Timestamp
DATE=$(date +%Y-%m-%d)
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
LOG_FILE="$LOG_DIR/v5_detector_${DATE}.log"

# =============================================================================
# LOGGING
# =============================================================================

log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

log_section() {
    echo "" | tee -a "$LOG_FILE"
    echo "==========================================" | tee -a "$LOG_FILE"
    echo "$1" | tee -a "$LOG_FILE"
    echo "==========================================" | tee -a "$LOG_FILE"
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

log_section "v5.0 MANIPULATION DETECTOR - START"

# Change to working directory
cd "$WORK_DIR" || {
    log "ERROR: Cannot change to $WORK_DIR"
    exit 1
}

log "Working directory: $(pwd)"

# Verify Python scripts exist
if [ ! -f "${SERVICE_DIR}/manipulation_detector_v5_0.py" ]; then
    log "ERROR: manipulation_detector_v5_0.py not found in ${SERVICE_DIR}"
    exit 1
fi

# Verify .env exists
if [ ! -f ".env" ]; then
    log "ERROR: .env file not found"
    exit 1
fi

log "All required files found"

# =============================================================================
# STEP 1: RUN MANIPULATION DETECTOR
# =============================================================================

log_section "STEP 1: Running Manipulation Detector"

log "Scanning 296 CSE stocks for patterns..."

cd "${SERVICE_DIR}"
python3 manipulation_detector_v5_0.py --scan --output "${REPORT_DIR}/v5_report_${DATE}.txt" >> "$LOG_FILE" 2>&1

if [ $? -eq 0 ]; then
    log "Detector completed successfully"
else
    log "Detector failed with exit code $?"
    exit 1
fi

# =============================================================================
# STEP 2: VERIFY REPORT GENERATED
# =============================================================================

log_section "STEP 2: Verify Report"

REPORT_FILE="$REPORT_DIR/v5_report_${DATE}.txt"

if [ -f "$REPORT_FILE" ]; then
    REPORT_SIZE=$(stat -c%s "$REPORT_FILE" 2>/dev/null || stat -f%z "$REPORT_FILE" 2>/dev/null)
    log "Report generated: $REPORT_FILE"
    log "   Size: $REPORT_SIZE bytes"

    # Extract opportunity counts from report
    if command -v grep &> /dev/null; then
        HIGH_COUNT=$(grep "HIGH PRIORITY OPPORTUNITIES" "$REPORT_FILE" | grep -oE '\([0-9]+\)' | grep -oE '[0-9]+' || echo "0")
        MEDIUM_COUNT=$(grep "MEDIUM PRIORITY" "$REPORT_FILE" | grep -oE '\([0-9]+\)' | grep -oE '[0-9]+' || echo "0")
        log "   HIGH Priority: $HIGH_COUNT opportunities"
        log "   MEDIUM Priority: $MEDIUM_COUNT opportunities"
    fi
else
    log "Report file not found: $REPORT_FILE"
    log "   Detector may not have found any opportunities"
fi

# =============================================================================
# STEP 3: SEND EMAIL (via common.email_sender - replaces send_v5_email.py)
# =============================================================================

log_section "STEP 3: Send Email"

if [ -f "$REPORT_FILE" ]; then
    log "Sending email notification via common.email_sender..."

    cd "${WORK_DIR}"
    python3 << 'PYTHON_EMAIL' >> "$LOG_FILE" 2>&1
import os
from datetime import datetime
from common.email_sender import EmailSender

DATE = datetime.now().strftime('%Y-%m-%d')
REPORT_FILE = f"/opt/investment-os/v5_reports/v5_report_{DATE}.txt"

try:
    # Read report
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        report_text = f.read()

    print(f"Report loaded ({len(report_text)} bytes)")

    # Create HTML version (preserves exact v5 email template style)
    html_content = f"""
    <html>
    <head>
        <style>
           body {{
                font-family: 'Courier New', monospace;
                background-color: #ffffff;
                color: #000000;
                padding: 20px;
            }}
            .header {{
                background-color: #f0f0f0;
                padding: 20px;
                border-left: 4px solid #007acc;
                margin-bottom: 20px;
            }}
            .section {{
                background-color: #fafafa;
                padding: 15px;
                margin-bottom: 15px;
                border-radius: 4px;
                border: 1px solid #e0e0e0;
            }}
            pre {{
                background-color: #f5f5f5;
                color: #000000;
                padding: 10px;
                border-radius: 4px;
                overflow-x: auto;
                white-space: pre-wrap;
                word-wrap: break-word;
                border: 1px solid #ddd;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>v5.0 Manipulation Detector</h2>
            <p>Daily Report - {DATE}</p>
        </div>
        <div class="section">
            <pre>{report_text}</pre>
        </div>
        <div class="footer" style="color: #858585; font-size: 12px; margin-top: 20px;">
            <p>Investment OS - Automated Daily Scan</p>
            <p>Empire Manipulation Detection System</p>
        </div>
    </body>
    </html>
    """

    # Send via common library
    sender = EmailSender()
    sender.send_html(
        f'v5.0 Manipulation Detector - {DATE}',
        html_content,
        report_text
    )
    print("Email sent successfully")

except FileNotFoundError:
    print(f"Report file not found: {REPORT_FILE}")
except Exception as e:
    print(f"Email failed: {e}")
    print("Report still available locally")
PYTHON_EMAIL

    if [ $? -eq 0 ]; then
        log "Email sent successfully"
    else
        log "Email failed (non-critical)"
    fi
elif [ ! -f "$REPORT_FILE" ]; then
    log "No report to email - skipping"
fi

# =============================================================================
# STEP 4: CLEANUP
# =============================================================================

log_section "STEP 4: Cleanup"

# Keep only last 30 days of logs
DELETED_LOGS=$(find "$LOG_DIR" -name "v5_detector_*.log" -mtime +30 -delete -print 2>/dev/null | wc -l)
if [ "$DELETED_LOGS" -gt 0 ]; then
    log "Deleted $DELETED_LOGS old log files (>30 days)"
fi

# Keep only last 90 days of reports
DELETED_REPORTS=$(find "$REPORT_DIR" -name "v5_report_*.txt" -mtime +90 -delete -print 2>/dev/null | wc -l)
if [ "$DELETED_REPORTS" -gt 0 ]; then
    log "Deleted $DELETED_REPORTS old reports (>90 days)"
fi

# =============================================================================
# SUMMARY
# =============================================================================

log_section "v5.0 DETECTOR - COMPLETE"

log "Summary:"
log "   Date: $DATE"
log "   Report: $REPORT_FILE"
log "   Log: $LOG_FILE"
log ""
log "Next run: Tomorrow 7:00 PM"

exit 0
