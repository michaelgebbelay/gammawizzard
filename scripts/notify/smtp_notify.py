#!/usr/bin/env python3
import os
import sys
import smtplib
from email.message import EmailMessage


def main():
    if len(sys.argv) < 3:
        print("Usage: smtp_notify.py <subject> <body>")
        return 2

    subject = sys.argv[1]
    body = sys.argv[2]

    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    smtp_to = os.environ.get("SMTP_TO", "").strip() or smtp_user
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587").strip())

    if not smtp_user or not smtp_pass or not smtp_to:
        print("SMTP secrets missing: SMTP_USER/SMTP_PASS/SMTP_TO")
        return 1

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = smtp_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

    print("SMTP notify sent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
