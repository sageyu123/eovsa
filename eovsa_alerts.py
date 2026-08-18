#!/usr/bin/env python
'''Small, configuration-driven email helper for unattended EOVSA alerts.'''

import os
import smtplib
from email.mime.text import MIMEText


def send_email(recipient, subject, body):
    '''Send one plain-text email using the configured SMTP relay.

    :param recipient: Destination email address.
    :type recipient: str
    :param subject: Message subject.
    :type subject: str
    :param body: Plain-text message body.
    :type body: str
    :returns: Success flag and a human-readable delivery detail.
    :rtype: tuple(bool, str)

    The required setting is ``EOVSA_SMTP_HOST``.  Optional settings are
    ``EOVSA_SMTP_PORT`` (default 25), ``EOVSA_SMTP_FROM``,
    ``EOVSA_SMTP_STARTTLS``, ``EOVSA_SMTP_USER``, and
    ``EOVSA_SMTP_PASSWORD``.  The return value is ``(success, detail)``.
    '''
    host = os.environ.get('EOVSA_SMTP_HOST')
    if not host:
        return False, 'EOVSA_SMTP_HOST is not configured'
    try:
        port = int(os.environ.get('EOVSA_SMTP_PORT', '25'))
    except ValueError:
        return False, 'EOVSA_SMTP_PORT is not an integer'

    sender = os.environ.get('EOVSA_SMTP_FROM',
                            'eovsa-schedule@njit.edu')
    username = os.environ.get('EOVSA_SMTP_USER')
    password = os.environ.get('EOVSA_SMTP_PASSWORD')
    if bool(username) != bool(password):
        return False, ('EOVSA_SMTP_USER and EOVSA_SMTP_PASSWORD must be '
                       'configured together')

    message = MIMEText(body)
    message['From'] = sender
    message['To'] = recipient
    message['Subject'] = subject

    smtp = None
    try:
        smtp = smtplib.SMTP(host, port, timeout=10)
        if os.environ.get('EOVSA_SMTP_STARTTLS', '').lower() in (
                '1', 'true', 'yes', 'on'):
            smtp.starttls()
            smtp.ehlo()
        if username:
            smtp.login(username, password)
        refused = smtp.sendmail(sender, [recipient], message.as_string())
        if refused:
            return False, 'SMTP relay refused recipient: %s' % refused
        return True, 'email sent to %s' % recipient
    except Exception as err:
        return False, 'SMTP error: %s' % err
    finally:
        if smtp is not None:
            try:
                smtp.quit()
            except:
                pass
