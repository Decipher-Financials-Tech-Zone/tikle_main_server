# send_tasks.py
from fastapi import FastAPI, HTTPException
from datetime import date
import httpx
import asyncio
import smtplib
from email.mime.text import MIMEText

app = FastAPI(title="Tasks -> Email")

# -------------------------------
# Hardcoded email + SMTP settings
SMTP_USER = "deciphertechzone@gmail.com"
SMTP_PASSWORD = "noek ykyt ddya jeqk"  # <-- replace with Gmail App Password
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465

SENDER = "deciphertechzone@gmail.com"
RECIPIENTS = ["megha.punjabi@decipherfinancials.com","luv.ratan@decipherfinancials.com"]
# -------------------------------

TASKS_API_BASE = "http://13.201.129.153:5050/app/tasks/"

def build_html_table_grouped(data_list: list) -> str:
    """Build HTML table grouped by user with rowspan for name/email."""
    html = """
    <html>
    <body>
      <p>Your Task Sheet Summary for today.</p>
      <table border="1" cellpadding="6" cellspacing="0"
             style="border-collapse:collapse; font-family:Arial, sans-serif;">
        <thead style="background:#f2f2f2;">
          <tr>
            <th>User</th>
            <th>Email</th>
            <th>Date</th>
            <th>Tasks</th>
            <th>Client</th>
            <th>Project</th>
            <th>Comments</th>
          </tr>
        </thead>
        <tbody>
    """
    for user in data_list:
        name = user.get("name", "")
        email = user.get("email", "")
        tasks = user.get("tasks", []) or []
        if not tasks:
            html += f"""
            <tr>
              <td>{name}</td>
              <td>{email}</td>
              <td colspan="5" style="text-align:center;">No tasks</td>
            </tr>
            """
            continue

        rowspan = len(tasks)
        for idx, t in enumerate(tasks):
            tasks_html = (t.get("tasks", "") or "").replace("\n", "<br>")
            date_str = t.get("date", "")
            client = t.get("client", "")
            project = t.get("project", "")
            comments = t.get("comments", "")
            html += "<tr>"
            if idx == 0:  # merge name/email cells
                html += f'<td rowspan="{rowspan}">{name}</td>'
                html += f'<td rowspan="{rowspan}">{email}</td>'
            html += f"""
              <td>{date_str}</td>
              <td>{tasks_html}</td>
              <td>{client}</td>
              <td>{project}</td>
              <td>{comments}</td>
            """
            html += "</tr>"

    return html

async def fetch_tasks_for_today() -> list:
    """Fetch tasks JSON for today's date from API."""
    today = date.today().strftime("%Y-%m-%d")
    url = f"{TASKS_API_BASE}?date={today}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()

def send_email(html_body: str, subject: str):
    """Send HTML email using Gmail SMTP (blocking)."""
    msg = MIMEText(html_body, "html")
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = ", ".join(RECIPIENTS)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SENDER, RECIPIENTS, msg.as_string())

