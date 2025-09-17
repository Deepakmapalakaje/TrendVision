import os
import smtplib
from email.mime.text import MIMEText


def send_otp_email(to_email: str, otp: str):
    sender_email = os.getenv("SENDER_EMAIL", "dscatreeing@gmail.com")
    sender_password = os.getenv("SENDER_PASSWORD", "lszl urfy lhlm vshz")

    subject = "Your OTP Code"
    body = f"Your OTP for password reset is: {otp}"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, [to_email], msg.as_string())


