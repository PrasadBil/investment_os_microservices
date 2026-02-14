#!/usr/bin/env python3
"""
Investment OS - Email Sender Module
======================================
Unified email sending for all Investment OS services.
Extracts and centralizes the SMTP logic from send_v5_email.py.

Currently Used By:
    - Manipulation Detector v5.0 (daily 7:00 PM)
    - Calendar Signal Monitor (daily 6:30 PM) — planned migration
    - 7D Weekly Scoring (Saturday 6:00 PM) — planned migration

Design Decisions:
    - Pulls credentials from common.config singleton (not os.getenv)
    - Keeps the proven HTML template from send_v5_email.py
    - Supports both plain text and HTML email
    - Gmail App Password authentication (STARTTLS on port 587)
    - 30s timeout matches existing production behavior

Usage:
    from common.email_sender import EmailSender

    sender = EmailSender()

    # Send a plain text report
    sender.send_report(
        subject="v5.0 Manipulation Detector - 2026-02-09",
        body=report_text
    )

    # Send with custom HTML
    sender.send_html(
        subject="Weekly 7D Scoring Results",
        html_content="<h1>22 STRONG BUY</h1>...",
        plain_fallback="22 STRONG BUY recommendations..."
    )

Replaces:
    - send_v5_email.py (lines 1-197) — entire file
    - Inline email config (os.getenv calls, lines 19-23)
    - Hardcoded WORK_DIR path (line 26)
"""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional

from common.config import get_config

logger = logging.getLogger(__name__)


class EmailSender:
    """
    Investment OS email sender.

    Wraps Gmail SMTP with the exact authentication flow
    proven in 16 months of production (send_v5_email.py).

    Credentials sourced from common.config singleton:
        - V5_EMAIL_FROM
        - V5_EMAIL_TO
        - V5_EMAIL_PASSWORD
        - V5_SMTP_SERVER (default: smtp.gmail.com)
        - V5_SMTP_PORT (default: 587)
    """

    def __init__(self, recipient: Optional[str] = None):
        """
        Initialize email sender with config from .env.

        Args:
            recipient: Override recipient (default: V5_EMAIL_TO from config)
        """
        config = get_config()

        self.email_from = config.V5_EMAIL_FROM
        self.email_to = recipient or config.V5_EMAIL_TO
        self.email_password = config.V5_EMAIL_PASSWORD
        self.smtp_server = config.V5_SMTP_SERVER
        self.smtp_port = config.V5_SMTP_PORT

    def validate(self) -> bool:
        """
        Validate that email configuration is complete.

        Returns:
            True if all required credentials are set
        """
        if not all([self.email_from, self.email_to, self.email_password]):
            logger.error(
                "Email configuration incomplete. Required in .env:\n"
                "  V5_EMAIL_FROM=your@gmail.com\n"
                "  V5_EMAIL_TO=recipient@gmail.com\n"
                "  V5_EMAIL_PASSWORD=your-app-password"
            )
            return False
        return True

    def send_report(self, subject: str, body: str) -> bool:
        """
        Send a report email with auto-generated HTML formatting.

        Uses the proven HTML template from send_v5_email.py:
        monospace font, blue left-border header, light sections.

        This is the primary method for daily automated emails
        (manipulation detector, calendar signals, weekly scoring).

        Args:
            subject: Email subject line
            body: Plain text report content (will be wrapped in HTML)

        Returns:
            True if sent successfully, False otherwise
        """
        html_content = self._wrap_report_html(subject, body)
        return self.send_html(
            subject=subject,
            html_content=html_content,
            plain_fallback=body
        )

    def send_html(
        self,
        subject: str,
        html_content: str,
        plain_fallback: str = ""
    ) -> bool:
        """
        Send email with custom HTML content.

        Sends multipart/alternative with both plain text and HTML.
        Email clients that support HTML render the rich version;
        others fall back to plain text.

        Args:
            subject: Email subject line
            html_content: Full HTML email body
            plain_fallback: Plain text version (optional)

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.validate():
            return False

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.email_from
        msg['To'] = self.email_to

        # Plain text version
        if plain_fallback:
            msg.attach(MIMEText(plain_fallback, 'plain', 'utf-8'))

        # HTML version
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        return self._send(msg)

    def _send(self, msg: MIMEMultipart) -> bool:
        """
        Send email via SMTP (internal method).

        Exact same flow as production send_v5_email.py:
        SMTP → STARTTLS → login → send → close

        Args:
            msg: Composed MIMEMultipart message

        Returns:
            True if sent, False on any error
        """
        try:
            logger.info(f"Connecting to {self.smtp_server}:{self.smtp_port}...")

            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30) as server:
                server.set_debuglevel(0)
                server.starttls()

                logger.info(f"Logging in as {self.email_from}...")
                server.login(self.email_from, self.email_password)

                logger.info(f"Sending to {self.email_to}...")
                server.send_message(msg)

            logger.info("Email sent successfully")
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error(
                "SMTP authentication failed. "
                "Check V5_EMAIL_FROM and V5_EMAIL_PASSWORD in .env. "
                "Gmail users: Use App Password from "
                "https://myaccount.google.com/apppasswords"
            )
            return False

        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False

        except Exception as e:
            logger.error(f"Email send error: {e}")
            return False

    def _wrap_report_html(self, title: str, report_text: str) -> str:
        """
        Wrap plain text report in the proven HTML template.

        Template preserved exactly from send_v5_email.py:
        - Courier New monospace font
        - Blue left-border header (#007acc)
        - Light gray sections (#fafafa)
        - Pre-formatted text block for report content
        - Investment OS footer

        Args:
            title: Report title (shown in header)
            report_text: Plain text content

        Returns:
            Complete HTML email body
        """
        date_str = datetime.now().strftime('%Y-%m-%d')

        return f"""
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
            .high-priority {{
                color: #008000;
                font-weight: bold;
            }}
            .medium-priority {{
                color: #ff8c00;
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
            <h2>{title}</h2>
            <p>Daily Report - {date_str}</p>
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
